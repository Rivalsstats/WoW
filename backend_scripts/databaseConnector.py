import mysql.connector
import time
from mysql.connector import errorcode
from mysql.connector import pooling
import random

# configuration
MAX_LOCK_WAIT_RETRIES = 5
LOCK_WAIT_BACKOFF_MIN = 0.2
LOCK_WAIT_BACKOFF_MAX = 1

# The shared pool, set by init_connection_pool(). Defined here so get_connection's
# ``CONNECTION_POOL is None`` guard raises the intended RuntimeError (rather than a
# NameError) in credential-free generators that never initialise a pool — e.g. the
# analyzer page, where build_global_trends() catches it and just hides the bar.
CONNECTION_POOL = None


def get_connection():
    """
    Return connection from the shared pool.
    """
    if CONNECTION_POOL is None:
        raise RuntimeError(
            "Connection pool not initialized; call init_connection_pool() first."
        )
    conn = CONNECTION_POOL.get_connection()
    return conn


def get_live_connection(ping_attempts=3, ping_delay=2):
    """Check out a pooled connection and guarantee it is actually alive.

    mysql.connector's pool hands connections back without validating them, so a
    connection that the server closed while it sat idle past ``wait_timeout``
    (e.g. across a multi-hour simc run that held no DB work) comes back dead and
    the next statement raises 'MySQL Connection not available'. ``ping`` with
    ``reconnect=True`` transparently re-opens it; if even that fails we force a
    reconnect before returning. Session settings (autocommit, isolation, lock
    timeouts) are reset by a reconnect, so callers must (re)apply
    ``configure_read_session`` after checkout, which they already do per phase."""
    conn = get_connection()
    try:
        conn.ping(reconnect=True, attempts=ping_attempts, delay=ping_delay)
    except Exception:
        try:
            conn.reconnect(attempts=ping_attempts, delay=ping_delay)
        except Exception:
            # Give up on this pooled slot and return a brand-new connection so the
            # caller never receives a dead handle.
            conn = get_connection()
            conn.ping(reconnect=True, attempts=ping_attempts, delay=ping_delay)
    return conn


def init_connection_pool(host, user, password, database, port, pool_size=30):
    global CONNECTION_POOL
    CONNECTION_POOL = pooling.MySQLConnectionPool(
        pool_name="region_pool",
        pool_size=pool_size,
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
        use_pure=True,
    )


def configure_read_session(conn, cursor):
    """Configure a read-only page-build session so it can never wedge the
    nightly aggregation pipeline: under the pool default autocommit=0 the
    first SELECT opens a transaction that holds a shared metadata lock on
    every table it reads until commit, which blocks the pipeline's RENAME
    swaps and queues all later queries behind them. autocommit releases the
    MDL after each statement; READ UNCOMMITTED matches the events' isolation;
    the lock timeouts bound how long a query waits instead of hanging."""
    conn.autocommit = True
    # Setting the ``autocommit`` attribute on a PooledMySQLConnection does NOT
    # reach the server (the wrapper swallows the setter), so issue it as SQL too —
    # otherwise the session stays autocommit=0 and the MDL never releases between
    # statements (and any writes on this session would roll back on pool return).
    cursor.execute("SET SESSION autocommit = 1")
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cursor.execute("SET SESSION lock_wait_timeout = 120")
    cursor.execute("SET SESSION innodb_lock_wait_timeout = 30")


def commit_with_retry(connection):
    """
    Commit; on lock-wait timeout, retry commit itself (rare) same as above.
    """
    attempt = 0
    while True:
        try:
            connection.commit()
            return
        except mysql.connector.DatabaseError as err:
            if (
                err.errno == errorcode.ER_LOCK_WAIT_TIMEOUT
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(
                    f"Commit lock wait timeout, retrying in {wait:.2f}s (attempt {attempt + 1}/{MAX_LOCK_WAIT_RETRIES})"
                )
                connection.rollback()
                time.sleep(wait)
                attempt += 1
                continue

            if (
                err.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST)
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(f"Lost connection, reconnecting in {wait:.2f}s")
                time.sleep(wait)
                # re‑acquire a fresh connection & cursor
                connection.reconnect(attempts=5, delay=5)
                attempt += 1
                continue
            raise


def fetch_with_retry(connection, cursor, sql, params=None):
    """
    Fetch data with retry logic on lock-wait timeout.
    """
    attempt = 0
    while True:
        try:
            cursor.execute(sql, params or ())

            return cursor.fetchall()
        except mysql.connector.DatabaseError as err:
            # err.errno is the integer error code
            if (
                err.errno == errorcode.ER_LOCK_WAIT_TIMEOUT
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(
                    f"Lock wait timeout, rolling back and retrying in {wait:.2f}s (attempt {attempt + 1}/{MAX_LOCK_WAIT_RETRIES})"
                )
                connection.rollback()  # undo any partial work
                time.sleep(wait)
                attempt += 1
                continue
            if (
                err.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST)
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(f"Lost connection, reconnecting in {wait:.2f}s")
                time.sleep(wait)
                # re‑acquire a fresh connection & cursor
                connection.reconnect(attempts=5, delay=5)
                cursor = connection.cursor()
                attempt += 1
                continue
            # if we hit max retried or a different error, re‑raise
            raise


def execute_with_retry(connection, cursor, sql, params=None):
    """
    Try cursor.execute(); on lock-wait timeout (1205) retry up to MAX_LOCK_WAIT_RETRIES.
    """
    attempt = 0
    while True:
        try:
            cursor.execute(sql, params or ())

            return
        except mysql.connector.DatabaseError as err:
            # err.errno is the integer error code
            if (
                err.errno == errorcode.ER_LOCK_WAIT_TIMEOUT
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(
                    f"Lock wait timeout, rolling back and retrying in {wait:.2f}s (attempt {attempt + 1}/{MAX_LOCK_WAIT_RETRIES})"
                )
                connection.rollback()  # undo any partial work
                time.sleep(wait)
                attempt += 1
                continue
            if (
                err.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST)
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(f"Lost connection, reconnecting in {wait:.2f}s")
                time.sleep(wait)
                # re‑acquire a fresh connection & cursor
                connection.reconnect(attempts=5, delay=5)
                cursor = connection.cursor()
                attempt += 1
                continue
            # if we hit max retried or a different error, re‑raise
            raise


def executemany_with_retry(connection, cursor, sql, param_list):
    """
    Bulk execute with retry logic.
    """
    attempt = 0
    while True:
        try:
            cursor.executemany(sql, param_list)
            return
        except mysql.connector.DatabaseError as err:
            if (
                err.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST)
                and attempt < MAX_LOCK_WAIT_RETRIES
            ):
                wait = random.uniform(LOCK_WAIT_BACKOFF_MIN, LOCK_WAIT_BACKOFF_MAX) * (
                    2**attempt
                )
                print(f"Lost connection, reconnecting in {wait:.2f}s")
                time.sleep(wait)
                connection.reconnect(attempts=5, delay=5)
                cursor = connection.cursor()
                attempt += 1
                continue
            raise


INSERT_RUN_SQL = "INSERT IGNORE INTO runs (`season`, `region`, `dungeon_id`, `keystone_level`, `duration`, `timestamp`, `faction`) VALUES (%s, %s, %s, %s, %s, %s, %s)"


def insert_run(
    connection,
    cursor,
    season: int,
    region: str,
    dungeon_id: str,
    keystone_level: int,
    duration: int,
    timestamp: int,
    faction: str,
):
    """Insert a run into the runs table."""
    val = (season, region, dungeon_id, keystone_level, duration, timestamp, faction)
    execute_with_retry(connection, cursor, INSERT_RUN_SQL, val)
    return cursor.lastrowid


INSERT_RUNS_SQL = "INSERT IGNORE INTO runs (`season`, `region`, `dungeon_id`, `keystone_level`, `duration`, `timestamp`, `faction`) VALUES (%s, %s, %s, %s, %s, %s, %s)"


def insert_runs_batch(connection, cursor, run_vals):
    """Bulk-insert runs, returns first inserted run_id."""
    executemany_with_retry(connection, cursor, INSERT_RUNS_SQL, run_vals)
    return cursor.lastrowid


SELECT_RUNS_SQL = "SELECT `run_id`, `dungeon_id`, `keystone_level`, `duration`, `timestamp`, `faction`, `region`, `season` FROM runs WHERE (`season`, `region`, `dungeon_id`) IN ((%s, %s, %s))"


def select_runs(connection, cursor, season, region, dungeon_id):
    param = (season, region, dungeon_id)
    execute_with_retry(connection, cursor, SELECT_RUNS_SQL, param)
    return cursor.fetchall()


INSERT_RUN_MEMBER_SQL = (
    "INSERT IGNORE INTO run_members (`run_id`, `member`) VALUES (%s, %s)"
)


def insert_run_member(connection, cursor, run_id: int, member: int):
    """Insert a member into the members table."""
    val = (run_id, member)
    return execute_with_retry(connection, cursor, INSERT_RUN_MEMBER_SQL, val)


def insert_run_members_batch(connection, cursor, rm_vals):
    """Bulk-insert run_members."""
    executemany_with_retry(connection, cursor, INSERT_RUN_MEMBER_SQL, rm_vals)


INSERT_MEMBER_SQL = "INSERT IGNORE INTO members (`spec_id`, `loadout`, `hero_talent_id`) VALUES (%s, %s, %s)"


def insert_member(connection, cursor, spec_id: int, loadout: str, hero_talent_id: int):
    """Insert a member into the members table."""
    val = (spec_id, loadout, hero_talent_id)
    execute_with_retry(connection, cursor, INSERT_MEMBER_SQL, val)
    return cursor.lastrowid


def insert_members_batch(connection, cursor, member_vals):
    """Bulk-insert members, returns first inserted member_id."""
    executemany_with_retry(connection, cursor, INSERT_MEMBER_SQL, member_vals)
    return cursor.lastrowid


INSERT_CLASS_TALENT_SQL = "INSERT IGNORE INTO class_talents (`member`, `talent_id`, `rank`) VALUES (%s, %s, %s)"


def insert_class_talents(connection, cursor, class_talents: list[tuple[int, int, int]]):
    """Bulk-insert class talents, each tuple being (member, talent_id, rank)."""
    return executemany_with_retry(
        connection, cursor, INSERT_CLASS_TALENT_SQL, class_talents
    )


INSERT_SPEC_TALENT_SQL = "INSERT IGNORE INTO spec_talents (`member`, `talent_id`, `rank`) VALUES (%s, %s, %s)"


def insert_spec_talents(connection, cursor, spec_talents: list[tuple[int, int, int]]):
    """Bulk-insert spec talents, each tuple being (member, talent_id, rank)."""
    return executemany_with_retry(
        connection, cursor, INSERT_SPEC_TALENT_SQL, spec_talents
    )


INSERT_HERO_TALENT_SQL = "INSERT IGNORE INTO hero_talents (`member`, `talent_id`, `rank`) VALUES (%s, %s, %s)"


def insert_hero_talents(connection, cursor, hero_talents: list[tuple[int, int, int]]):
    """Bulk-insert hero talents, each tuple being (member, talent_id, rank)."""
    return executemany_with_retry(
        connection, cursor, INSERT_HERO_TALENT_SQL, hero_talents
    )


INSERT_EQUIPMENT_SQL = "INSERT IGNORE INTO equipment (`member`, `slot`, `item_id`, `item_level`) VALUES (%s, %s, %s, %s)"


def insert_equipment(
    connection, cursor, member: int, slot: str, item_id: int, item_level: int
):
    """Insert a equipment item into the equipment table."""
    val = (member, slot, item_id, item_level)
    execute_with_retry(connection, cursor, INSERT_EQUIPMENT_SQL, val)
    return cursor.lastrowid


def insert_equipment_batch(connection, cursor, eq_vals):
    return executemany_with_retry(connection, cursor, INSERT_EQUIPMENT_SQL, eq_vals)


INSERT_ENCHANTMENT_SQL = (
    "INSERT IGNORE INTO enchantments (`equipment_id`, `enchantment_id`) VALUES (%s, %s)"
)


def insert_enchantments(connection, cursor, enchantments):
    """Insert a enchantment into the enchantments table."""
    executemany_with_retry(connection, cursor, INSERT_ENCHANTMENT_SQL, enchantments)
    return cursor.lastrowid


INSERT_SOCKET_SQL = "INSERT IGNORE INTO sockets (`equipment_id`, `socket_type`, `socket_item_id`) VALUES (%s, %s, %s)"


def insert_sockets(connection, cursor, sockets):
    """Insert a socket into the sockets table."""
    try:
        # try the fast path
        executemany_with_retry(connection, cursor, INSERT_SOCKET_SQL, sockets)
        return cursor.lastrowid
    except mysql.connector.errors.DatabaseError as err:
        # catch the 1467 “Failed to read auto-increment” bug and fall back
        if err.errno == 1467:
            lastid = None
            for sock in sockets:
                # single-row insert never trips the bug
                execute_with_retry(connection, cursor, INSERT_SOCKET_SQL, sock)
                lastid = cursor.lastrowid
            return lastid
        # anything else, re-raise
        raise


INSERT_BONUS_SQL = (
    "INSERT IGNORE INTO bonus_ids (`equipment_id`, `bonus_id`) VALUES (%s, %s)"
)


def insert_bonuses(connection, cursor, bonuses):
    """Insert a bonus_id into the bonus table."""
    executemany_with_retry(connection, cursor, INSERT_BONUS_SQL, bonuses)
    return cursor.lastrowid


INSERT_STATS_SQL = "INSERT INTO Mythistone.character_stats (`member`, stat, raw, percent) VALUES(%s, %s, %s, %s);"


def insert_stats(
    connection, cursor, member: int, stat: str, raw: float, percent: float
):
    """Insert a stat into the character_stats table."""
    val = (member, stat, raw, percent)
    execute_with_retry(connection, cursor, INSERT_STATS_SQL, val)
    return cursor.lastrowid


def insert_stats_batch(connection, cursor, eq_vals):
    return executemany_with_retry(connection, cursor, INSERT_STATS_SQL, eq_vals)


INSERT_DUNGEON_SQL = (
    "INSERT INTO dungeon_data "
    "(dungeon_id, slug, name_en_us, upgrade_1_duration, upgrade_2_duration, upgrade_3_duration) "
    "VALUES (%s, %s, %s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE "
    "slug = VALUES(slug), name_en_us = VALUES(name_en_us), "
    "upgrade_1_duration = VALUES(upgrade_1_duration), "
    "upgrade_2_duration = VALUES(upgrade_2_duration), "
    "upgrade_3_duration = VALUES(upgrade_3_duration)"
)


def insert_dungeon_data(
    connection,
    cursor,
    dungeon_id: str,
    slug: str,
    name_en_us: str,
    up1: int,
    up2: int,
    up3: int,
):
    params = (dungeon_id, slug, name_en_us, up1, up2, up3)
    execute_with_retry(connection, cursor, INSERT_DUNGEON_SQL, params)


def commit_changes(connection):
    """Commit changes to the database."""
    try:
        commit_with_retry(connection)
    except mysql.connector.Error as err:
        print(f"Error committing changes: {err}")


# fetching data

FETCH_SLOTS_SQL = "SELECT slot, slot_group FROM Mythistone.slot_group_map;"


def fetch_slots(connection, cursor):
    """Fetch slot information from the database."""
    return fetch_with_retry(connection, cursor, FETCH_SLOTS_SQL)


FETCH_TOP_ITEM_BY_SLOT_SQL = """
SELECT
  item_id,
  run_count AS equip_count
FROM Mythistone.global_aggregated_equipment
WHERE spec_id = %s
  AND season  = %s
  AND slot    = %s
ORDER BY equip_count DESC, item_id
LIMIT 10;
"""


def fetch_top_items_for_slot(connection, cursor, spec_id, season, slot):
    """Fetch the top items from the database."""
    params = (spec_id, season, slot)
    return fetch_with_retry(connection, cursor, FETCH_TOP_ITEM_BY_SLOT_SQL, params)


FETCH_TOP_ITEM_BY_SLOT_GROUP_SQL = """
SELECT
  item_id,
  SUM(run_count) AS equip_count
FROM Mythistone.global_aggregated_equipment
JOIN Mythistone.slot_group_map sgm ON sgm.slot = Mythistone.global_aggregated_equipment.slot
WHERE spec_id = %s
  AND season  = %s
  AND slot_group = %s
GROUP BY item_id
ORDER BY equip_count DESC, item_id
LIMIT 10;
"""


def fetch_top_items_for_slot_group(connection, cursor, spec_id, season, slot_group):
    """Fetch the top items from the database."""
    params = (spec_id, season, slot_group)
    return fetch_with_retry(
        connection, cursor, FETCH_TOP_ITEM_BY_SLOT_GROUP_SQL, params
    )


FETCH_TOP_ITEMS_BY_SLOT_WITH_BONUS_SQL = """
-- SQL: top items with top bonus per item (MySQL 8+)
WITH top_items AS (
  SELECT
    item_id,
    SUM(run_count) AS equip_count
  FROM Mythistone.global_aggregated_equipment
  WHERE spec_id = %s
    AND season  = %s
    AND slot    = %s
  GROUP BY item_id
  ORDER BY equip_count DESC, item_id
  LIMIT 10
),
bonus_sums AS (
  SELECT
    item_id,
    bonus_list,
    SUM(run_count) AS list_count
  FROM Mythistone.global_aggregated_bonus_lists
  WHERE spec_id = %s
    AND season  = %s
    AND item_id IN (SELECT item_id FROM top_items)
  GROUP BY item_id, bonus_list
),
ranked AS (
  SELECT
    item_id,
    bonus_list,
    list_count,
    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY list_count DESC, bonus_list) AS rn
  FROM bonus_sums
)
SELECT
  ti.item_id,
  ti.equip_count,
  r.bonus_list,
  r.list_count,
  gai.max_timed_key,
  gai.max_depleted_key
FROM top_items ti
LEFT JOIN Mythistone.global_aggregated_items gai 
  ON gai.item_id = ti.item_id AND gai.spec_id = %s AND gai.season = %s
LEFT JOIN ranked r ON ti.item_id = r.item_id AND r.rn = 1
ORDER BY ti.equip_count DESC;
"""


def fetch_top_items_for_slot_with_bonus(connection, cursor, spec_id, season, slot):
    """Fetch the top items with bonus for a specific slot from the database."""
    params = (spec_id, season, slot, spec_id, season, spec_id, season)
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_ITEMS_BY_SLOT_WITH_BONUS_SQL, params
    )

    data = []
    for row in rows:
        # row = (item_id, equip_count, bonus_list, list_count, max_timed_key, max_depleted_key)
        item_id = row[0]
        equip_count = row[1]
        bonus_list = row[2]  # may be None
        list_count = row[3]  # may be None
        max_timed_key = row[4]
        max_depleted_key = row[5]
        data.append(
            {
                "item": item_id,
                "count": int(equip_count),
                "max_timed_key": int(max_timed_key) if max_timed_key else 0,
                "max_depleted_key": int(max_depleted_key) if max_depleted_key else 0,
                "bonus": {"ids": bonus_list, "count": int(list_count)}
                if bonus_list is not None
                else None,
            }
        )
    return data


FETCH_TOP_ITEMS_BY_SLOT_GROUP_WITH_BONUS_SQL = """
-- SQL: top items (slot_group) with top bonus per item (MySQL 8+)
WITH top_items AS (
  SELECT
    ae.item_id,
    SUM(ae.run_count) AS equip_count
  FROM Mythistone.global_aggregated_equipment ae
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = ae.slot
  WHERE ae.spec_id = %s
    AND ae.season  = %s
    AND sgm.slot_group = %s
  GROUP BY ae.item_id
  ORDER BY equip_count DESC, ae.item_id
  LIMIT 10
),
bonus_sums AS (
  SELECT
    item_id,
    bonus_list,
    SUM(run_count) AS list_count
  FROM Mythistone.global_aggregated_bonus_lists
  WHERE spec_id = %s
    AND season  = %s
    AND item_id IN (SELECT item_id FROM top_items)
  GROUP BY item_id, bonus_list
),
ranked AS (
  SELECT
    item_id,
    bonus_list,
    list_count,
    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY list_count DESC, bonus_list) AS rn
  FROM bonus_sums
)
SELECT
  ti.item_id,
  ti.equip_count,
  r.bonus_list,
  r.list_count,
  gai.max_timed_key,
  gai.max_depleted_key
FROM top_items ti
LEFT JOIN Mythistone.global_aggregated_items gai 
  ON gai.item_id = ti.item_id AND gai.spec_id = %s AND gai.season = %s
LEFT JOIN ranked r ON ti.item_id = r.item_id AND r.rn = 1
ORDER BY ti.equip_count DESC;
"""


def fetch_top_items_for_slot_group_with_bonus(
    connection, cursor, spec_id, season, slot_group
):
    """Fetch top items for a slot_group along with each item's top bonus_list (MySQL 8+)."""
    # param order must match the SQL: spec, season, slot_group, spec, season, spec, season
    params = (spec_id, season, slot_group, spec_id, season, spec_id, season)
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_ITEMS_BY_SLOT_GROUP_WITH_BONUS_SQL, params
    )

    data = []
    for row in rows:
        # row = (item_id, equip_count, bonus_list, list_count, max_timed_key, max_depleted_key)
        item_id = row[0]
        equip_count = row[1]
        bonus_list = row[2]  # may be None
        list_count = row[3]  # may be None
        max_timed_key = row[4]
        max_depleted_key = row[5]
        data.append(
            {
                "item": item_id,
                "count": int(equip_count),
                "max_timed_key": int(max_timed_key) if max_timed_key else 0,
                "max_depleted_key": int(max_depleted_key) if max_depleted_key else 0,
                "bonus": {"ids": bonus_list, "count": int(list_count)}
                if bonus_list is not None
                else None,
            }
        )
    return data


# Per-slot totals (runs with any item in the slot) for one spec — the
# denominators for the spec page's gear-list noise filter
# (generateSpecPages.filter_gear_entries).
FETCH_SLOT_TOTALS_SQL = """
SELECT slot, SUM(run_count)
  FROM Mythistone.global_aggregated_equipment
 WHERE spec_id = %s
   AND season  = %s
 GROUP BY slot;
"""


def fetch_slot_totals(connection, cursor, spec_id, season):
    """Total runs per slot (any item equipped) for one spec, as {slot: runs}."""
    rows = fetch_with_retry(connection, cursor, FETCH_SLOT_TOTALS_SQL, (spec_id, season))
    return {r[0]: int(r[1]) for r in rows}


# Cheap "is this season ready for simc BiS" probe. gather_candidates ultimately
# reads global_aggregated_equipment; if the season has no rows there yet (the
# pre-season gap, before any new-season runs are collected and aggregated) then
# every spec would fail with "no candidate items". The simc collector uses this
# to skip the per-spec loop and emit a single "no data yet" alert instead of one
# failure alert per spec. LIMIT 1 keeps it O(1).
SIMC_SEASON_HAS_GEAR_DATA_SQL = """
SELECT 1
  FROM Mythistone.global_aggregated_equipment
 WHERE season = %s
 LIMIT 1;
"""


def simc_season_has_gear_data(connection, cursor, season):
    """True if any aggregated gear data exists for the season (i.e. the season is
    ready for BiS simulation). Reads under the same relaxed isolation the rest of
    the simc reads use (configure_read_session on the connection)."""
    rows = fetch_with_retry(connection, cursor, SIMC_SEASON_HAS_GEAR_DATA_SQL, (season,))
    return bool(rows)


# Every item id that appears in some spec page's top-10 gear list, computed in
# one sweep. Must stay in lockstep with the top_items CTEs of
# FETCH_TOP_ITEMS_BY_SLOT[_GROUP]_WITH_BONUS_SQL above: same table, same
# grouping (per slot and per slot_group), same top-10 cutoff and the same
# deterministic item_id tiebreak — generateItemPages uses this to guarantee a
# page exists for every item the spec pages link. Crucially it is restricted
# to the spec ids and slots/slot groups the spec pages actually render:
# without that, sparse partitions the pages never show (SHIRT/TABARD rows,
# unknown spec ids, per-slot FINGER_1/TRINKET_1 instead of the grouped lists)
# each "protect" up to 10 junk items apiece.
FETCH_SPEC_PAGE_LINKED_ITEMS_SQL = """
WITH slot_sums AS (
  SELECT spec_id, slot, item_id, SUM(run_count) AS equip_count
    FROM Mythistone.global_aggregated_equipment
   WHERE season = %s
     AND spec_id IN ({spec_ph})
     AND slot IN ({slot_ph})
   GROUP BY spec_id, slot, item_id
),
slot_ranked AS (
  SELECT item_id,
         equip_count,
         ROW_NUMBER() OVER (PARTITION BY spec_id, slot
                            ORDER BY equip_count DESC, item_id) AS rn,
         SUM(equip_count) OVER (PARTITION BY spec_id, slot) AS slot_total
    FROM slot_sums
),
group_sums AS (
  SELECT ae.spec_id, sgm.slot_group, ae.item_id, SUM(ae.run_count) AS equip_count
    FROM Mythistone.global_aggregated_equipment ae
    JOIN Mythistone.slot_group_map sgm ON sgm.slot = ae.slot
   WHERE ae.season = %s
     AND ae.spec_id IN ({spec_ph})
     AND sgm.slot_group IN ({group_ph})
   GROUP BY ae.spec_id, sgm.slot_group, ae.item_id
),
group_ranked AS (
  SELECT item_id,
         equip_count,
         ROW_NUMBER() OVER (PARTITION BY spec_id, slot_group
                            ORDER BY equip_count DESC, item_id) AS rn,
         SUM(equip_count) OVER (PARTITION BY spec_id, slot_group) AS slot_total
    FROM group_sums
),
weapon_sums AS (
  SELECT spec_id, slot, item_id, SUM(run_count) AS equip_count
    FROM Mythistone.global_aggregated_equipment
   WHERE season = %s
     AND spec_id IN ({spec_ph})
     AND slot IN ({weapon_ph})
   GROUP BY spec_id, slot, item_id
),
weapon_ranked AS (
  SELECT item_id,
         equip_count,
         ROW_NUMBER() OVER (PARTITION BY spec_id, slot
                            ORDER BY equip_count DESC, item_id) AS rn,
         ROW_NUMBER() OVER (PARTITION BY spec_id
                            ORDER BY equip_count DESC, item_id, slot) AS rn_comb,
         SUM(equip_count) OVER (PARTITION BY spec_id) AS weapon_total
    FROM weapon_sums
)
SELECT item_id FROM slot_ranked
 WHERE rn <= 10 AND (rn <= %s OR equip_count >= slot_total * %s)
UNION
SELECT item_id FROM group_ranked
 WHERE rn <= 10 AND (rn <= %s OR equip_count >= slot_total * %s)
UNION
SELECT item_id FROM weapon_ranked
 WHERE rn <= 10 AND (rn_comb <= %s OR equip_count >= weapon_total * %s);
"""


def fetch_spec_page_linked_items(connection, cursor, season, spec_ids, slots,
                                 slot_groups, weapon_slots, min_keep, min_share_pct):
    """Item ids linked from any spec page's per-slot top-10 gear lists, after
    the gear-list noise filter.

    ``spec_ids`` are the specs that actually get a page, ``slots`` the
    individually-rendered armor slot names, ``slot_groups`` the grouped ones
    (FINGER/TRINKET) and ``weapon_slots`` MAIN_HAND/OFF_HAND — weapons keep
    per-slot top-10 lists but share one denominator and one min-keep floor
    across both slots (generateSpecPages.filter_weapon_gear_entries), so a
    two-hander spec's stray off-hand loadouts never surface. ``min_keep`` /
    ``min_share_pct`` are generateSpecPages.GEAR_LIST_MIN_KEEP /
    GEAR_LIST_MIN_SLOT_SHARE: past the floor a list entry must hold
    ``min_share_pct``% of its denominator. Returns a set of str item ids."""
    if not (spec_ids and slots and slot_groups and weapon_slots):
        raise ValueError("spec_ids, slots, slot_groups and weapon_slots must all be non-empty")
    sql = FETCH_SPEC_PAGE_LINKED_ITEMS_SQL.format(
        spec_ph=", ".join(["%s"] * len(spec_ids)),
        slot_ph=", ".join(["%s"] * len(slots)),
        group_ph=", ".join(["%s"] * len(slot_groups)),
        weapon_ph=", ".join(["%s"] * len(weapon_slots)),
    )
    share = float(min_share_pct) / 100.0
    params = (
        (season,) + tuple(spec_ids) + tuple(slots)
        + (season,) + tuple(spec_ids) + tuple(slot_groups)
        + (season,) + tuple(spec_ids) + tuple(weapon_slots)
        + (min_keep, share, min_keep, share, min_keep, share)
    )
    rows = fetch_with_retry(connection, cursor, sql, params)
    return {str(r[0]) for r in rows}


FETCH_TOP_ENCHANT_FOR_SLOT_SQL = """
SELECT
    enchantment_id,
    run_count AS equip_count,
    max_timed_key,
    max_depleted_key
  FROM Mythistone.global_aggregated_enchantments_slot_group
  WHERE spec_id = %s
    AND season  = %s
    AND slot_group = %s
  ORDER BY equip_count DESC 
  LIMIT %s
"""


def fetch_top_enchant_for_slot(connection, cursor, spec_id, season, slot_group, amount):
    """Fetch the top enchant for a specific slot from the database."""
    params = (spec_id, season, slot_group, amount)
    return fetch_with_retry(connection, cursor, FETCH_TOP_ENCHANT_FOR_SLOT_SQL, params)


FETCH_TOP_SOCKET_FOR_ITEM_SQL = """
SELECT ais.socket_item_id, ais.run_count AS equip_count, ais.max_timed_key, ais.max_depleted_key
FROM Mythistone.global_aggregated_item_sockets AS ais
WHERE ais.spec_id = %s
  AND ais.season  = %s
  AND ais.item_id = %s
ORDER BY equip_count DESC
LIMIT 10;

"""


def fetch_top_sockets_for_item(connection, cursor, spec_id, season, item_id):
    """Fetch the top sockets for a specific item from the database."""
    params = (spec_id, season, item_id)
    return fetch_with_retry(connection, cursor, FETCH_TOP_SOCKET_FOR_ITEM_SQL, params)


FETCH_TOP_SOCKETS_FOR_ITEMS_SQL = """
SELECT ais.item_id, ais.socket_item_id, ais.run_count AS equip_count, ais.max_timed_key, ais.max_depleted_key
FROM Mythistone.global_aggregated_item_sockets AS ais
WHERE ais.spec_id = %s
  AND ais.season  = %s
  AND ais.item_id IN ({placeholders})
ORDER BY ais.item_id, equip_count DESC;
"""


def fetch_top_sockets_for_items(connection, cursor, spec_id, season, item_ids):
    """
    Return dict: { str(item_id): [ (socket_item_id, equip_count), ... ], ... }
    Runs one query for all item_ids.
    """
    if not item_ids:
        return {}
    # ensure items are strings/ints, and build placeholders
    item_ids_clean = [str(i) for i in item_ids]
    placeholders = ",".join(["%s"] * len(item_ids_clean))
    sql = FETCH_TOP_SOCKETS_FOR_ITEMS_SQL.format(placeholders=placeholders)
    params = [spec_id, season] + item_ids_clean
    rows = fetch_with_retry(connection, cursor, sql, params)
    out = {}
    for row in rows:
        item_id, socket_item_id, equip_count, max_timed_key, max_depleted_key = row
        key = str(item_id)
        out.setdefault(key, []).append((socket_item_id, int(equip_count), max_timed_key, max_depleted_key))
    return out


FETCH_TOP_BONUS_IDS_FOR_ITEM_SQL = """
SELECT
  bonus_list,
  run_count AS list_count
FROM Mythistone.global_aggregated_bonus_lists
WHERE spec_id = %s
  AND season  = %s
  AND item_id = %s
ORDER BY list_count DESC, bonus_list
LIMIT 1;

"""


def fetch_top_bonus_ids_for_item(connection, cursor, spec_id, season, item_id):
    """Fetch the top bonus IDs for a specific item from the database."""
    params = (spec_id, season, item_id)
    return fetch_with_retry(
        connection, cursor, FETCH_TOP_BONUS_IDS_FOR_ITEM_SQL, params
    )


FETCH_TOP_SOCKETS_SQL = """
SELECT ais.socket_item_id, SUM(ais.run_count) AS equip_count, MAX(ais.max_timed_key) AS max_timed_key, MAX(ais.max_depleted_key) AS max_depleted_key
FROM Mythistone.global_aggregated_item_sockets AS ais
WHERE ais.spec_id = %s
  AND ais.season  = %s
GROUP BY ais.socket_item_id
ORDER BY equip_count DESC
LIMIT 10;
"""


def fetch_top_sockets(connection, cursor, spec_id, season):
    """Fetch the top sockets for a specific item from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_TOP_SOCKETS_SQL, params)


# ---------------------------------------------------------------------------
# Item-page sweeps
#
# The dedicated item pages (generateItemPages.py) need data keyed by item_id
# rather than by spec/slot. Each helper sweeps one spec at a time: every one of
# these aggregation tables has a primary key that *starts* with spec_id, so
# filtering by spec_id (plus season) uses the index, whereas a season-only
# filter would force a full table scan (the bonus table in particular is huge
# and carries a large text column). The generator loops over the specs and
# buckets rows by item_id (keeping the per-spec split for the bySpec view).
# ---------------------------------------------------------------------------

FETCH_ITEM_SPEC_USAGE_SQL = """
SELECT item_id, run_count, max_timed_key, max_depleted_key
FROM Mythistone.global_aggregated_items
WHERE spec_id = %s
  AND season = %s
  AND run_count > 0;
"""


def fetch_item_spec_usage(connection, cursor, season, spec_id):
    """Per-item usage rollup for one spec/season.

    Returns rows: (item_id, run_count, max_timed_key, max_depleted_key).
    Across all specs, the distinct item_ids here define which items get a page.
    """
    return fetch_with_retry(
        connection, cursor, FETCH_ITEM_SPEC_USAGE_SQL, (spec_id, season)
    )


FETCH_ITEM_SOCKET_USAGE_SQL = """
SELECT item_id, socket_item_id, SUM(run_count) AS run_count
FROM Mythistone.global_aggregated_item_sockets
WHERE spec_id = %s
  AND season = %s
GROUP BY item_id, socket_item_id;
"""


def fetch_item_socket_usage(connection, cursor, season, spec_id):
    """Per-(item, gem) socket usage for one spec/season.

    Returns rows: (item_id, socket_item_id, run_count).
    """
    return fetch_with_retry(
        connection, cursor, FETCH_ITEM_SOCKET_USAGE_SQL, (spec_id, season)
    )


FETCH_ITEM_BONUS_USAGE_SQL = """
SELECT item_id, bonus_list, run_count
FROM Mythistone.global_aggregated_bonus_lists
WHERE spec_id = %s
  AND season = %s
  AND run_count > 0;
"""


def fetch_item_bonus_usage(connection, cursor, season, spec_id):
    """Per-(item, bonus_list) usage for one spec/season (ilvl / bonus variants).

    Returns rows: (item_id, bonus_list, run_count). The generator keeps only the
    top few combos per item.
    """
    return fetch_with_retry(
        connection, cursor, FETCH_ITEM_BONUS_USAGE_SQL, (spec_id, season)
    )


FETCH_ITEM_DUNGEON_USAGE_SQL = """
SELECT item_id, dungeon_id, keystone_level,
       SUM(CASE WHEN upgrade_tier = 'depleted' THEN 0 ELSE run_count END) AS timed_runs,
       SUM(CASE WHEN upgrade_tier = 'depleted' THEN run_count ELSE 0 END) AS depleted_runs
FROM Mythistone.aggregated_equipment
WHERE spec_id = %s
  AND season = %s
GROUP BY item_id, dungeon_id, keystone_level;
"""


def fetch_item_dungeon_usage(connection, cursor, season, spec_id):
    """Per-(item, dungeon, keystone level) usage for one spec, split timed/depleted.

    Collapses hero_talent / tier / slot out of aggregated_equipment. Returns rows:
    (item_id, dungeon_id, keystone_level, timed_runs, depleted_runs).
    """
    return fetch_with_retry(
        connection, cursor, FETCH_ITEM_DUNGEON_USAGE_SQL, (spec_id, season)
    )


# --- Denominators for adoption-rate (% of runs) metrics on the item page ----
# These are item-independent run totals: how many runs each spec / dungeon /
# key level had overall, so the item page can show "X% of this spec's runs use
# the item" instead of raw counts that just track how much each spec is played.

FETCH_SPEC_TOTAL_RUNS_SQL = """
SELECT spec_id, run_count
FROM Mythistone.aggregated_dungeon_global_specs
WHERE season = %s;
"""


def fetch_spec_total_runs(connection, cursor, season):
    """Total runs per spec for the season: {str(spec_id): run_count}."""
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_TOTAL_RUNS_SQL, (season,))
    return {str(sp): int(rc) for sp, rc in rows}


FETCH_DUNGEON_SPEC_TOTAL_RUNS_SQL = """
SELECT spec_id, dungeon_id, run_count
FROM Mythistone.aggregated_dungeon_specs
WHERE season = %s;
"""


def fetch_dungeon_spec_total_runs(connection, cursor, season):
    """Total runs per (spec, dungeon): {(str(spec_id), str(dungeon_id)): run_count}."""
    rows = fetch_with_retry(
        connection, cursor, FETCH_DUNGEON_SPEC_TOTAL_RUNS_SQL, (season,)
    )
    return {(str(sp), str(did)): int(rc) for sp, did, rc in rows}


FETCH_SPEC_KEYLEVEL_TOTAL_RUNS_SQL = """
SELECT spec_id, keystone_level, SUM(run_count) AS total_runs
FROM Mythistone.aggregated_spec
GROUP BY spec_id, keystone_level;
"""


def fetch_spec_keylevel_total_runs(connection, cursor):
    """Total runs per (spec, key level): {(str(spec_id), int(level)): run_count}.

    aggregated_spec has no season column (it holds the current season), so no
    season filter is applied.
    """
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_KEYLEVEL_TOTAL_RUNS_SQL)
    return {(str(sp), int(lvl)): int(rc) for sp, lvl, rc in rows}


# --- "best in slot" signals for the item page ------------------------------

FETCH_SIMC_BIS_RANK1_SQL = """
SELECT spec_id, item_id, dps_pct_gain
FROM Mythistone.simc_bis_items
WHERE season = %s AND `rank` = 1;
"""


def fetch_simc_bis_rank1(connection, cursor, season):
    """SimulationCraft rank-1 (BiS) pick per spec+slot for the season.

    Returns rows: (spec_id, item_id, dps_pct_gain). Used to mark which specs an
    item is the simulated best-in-slot for.
    """
    return fetch_with_retry(connection, cursor, FETCH_SIMC_BIS_RANK1_SQL, (season,))


FETCH_TOP50_ITEM_COUNTS_SQL = """
SELECT spec_id, item_id, COUNT(DISTINCT `rank`, map_challenge_mode_id) AS cnt
FROM Mythistone.top_player_loadout_items
WHERE season = %s
GROUP BY spec_id, item_id;
"""

FETCH_TOP50_LOADOUT_TOTALS_SQL = """
SELECT spec_id, COUNT(*) AS total
FROM Mythistone.top_player_loadouts
WHERE season = %s
GROUP BY spec_id;
"""


def fetch_top50_item_counts(connection, cursor, season):
    """How many top-player loadouts equip each (spec, item).

    Returns rows: (spec_id, item_id, cnt) where cnt is distinct loadouts.
    """
    return fetch_with_retry(connection, cursor, FETCH_TOP50_ITEM_COUNTS_SQL, (season,))


def fetch_top50_loadout_totals(connection, cursor, season):
    """Total top-player loadouts per spec: {str(spec_id): total}."""
    rows = fetch_with_retry(connection, cursor, FETCH_TOP50_LOADOUT_TOTALS_SQL, (season,))
    return {str(sp): int(t) for sp, t in rows}


FETCH_ENCHANT_SLOTGROUP_USAGE_SQL = """
SELECT spec_id, slot_group, enchantment_id, run_count
FROM Mythistone.global_aggregated_enchantments_slot_group
WHERE season = %s;
"""


def fetch_enchant_slotgroup_usage(connection, cursor, season):
    """Enchant usage per (spec, slot_group).

    Returns rows: (spec_id, slot_group, enchantment_id, run_count). The generator
    picks the most-used enchant per slot group (per spec and globally).
    """
    return fetch_with_retry(connection, cursor, FETCH_ENCHANT_SLOTGROUP_USAGE_SQL, (season,))


FETCH_TOP_LOADOUT_SQL = """
WITH summed AS (
  SELECT
    spec_id,
    season,
    hero_talent_id,
    loadout,
    run_count AS total_runs,
    max_timed_key,
    max_depleted_key
  FROM Mythistone.global_aggregated_loadout_data
  WHERE spec_id = %s   
    AND season  = %s   
)
, ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY hero_talent_id ORDER BY total_runs DESC, loadout) AS rn
  FROM summed
)
SELECT hero_talent_id, loadout, total_runs, max_timed_key, max_depleted_key
FROM ranked
WHERE rn = 1
ORDER BY hero_talent_id;


"""


def fetch_top_loadout(connection, cursor, spec_id, season):
    """Fetch the top loadout for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_TOP_LOADOUT_SQL, params)


# Same idea per dungeon: `global_aggregated_loadout_data` is season-wide, so the
# per-dungeon variant reads the dungeon-keyed aggregation and sums the run
# counts over keystone level / upgrade tier. A loadout string is one complete
# build, so counting whole strings stays exact even though the per-talent
# aggregations are sampled.
FETCH_TOP_LOADOUT_PER_DUNGEON_SQL = """
WITH summed AS (
  SELECT
    dungeon_id,
    hero_talent_id_key AS hero_talent_id,
    loadout,
    SUM(run_count) AS total_runs
  FROM Mythistone.aggregated_loadout_data
  WHERE spec_id = %s
    AND season  = %s
    AND loadout IS NOT NULL
  GROUP BY dungeon_id, hero_talent_id_key, loadout
)
, ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY dungeon_id, hero_talent_id
      ORDER BY total_runs DESC, loadout
    ) AS rn
  FROM summed
)
SELECT dungeon_id, hero_talent_id, loadout, total_runs
FROM ranked
WHERE rn = 1
ORDER BY dungeon_id, hero_talent_id;
"""


def fetch_top_loadout_per_dungeon(connection, cursor, spec_id, season):
    """Most-run talent loadout string per (dungeon, hero tree) for a spec."""
    params = (spec_id, season)
    return fetch_with_retry(
        connection, cursor, FETCH_TOP_LOADOUT_PER_DUNGEON_SQL, params
    )


FETCH_HERO_TREE_OVERVIEW_SQL = """
SELECT
  hero_talent_id,
  run_count AS total_runs,
  max_timed_key,
  max_depleted_key
FROM Mythistone.global_aggregated_hero_talent_overview
WHERE spec_id = %s
  AND hero_talent_id IS NOT NULL
  AND hero_talent_id <> 0
ORDER BY run_count DESC;
"""

def fetch_hero_tree_overview(connection, cursor, spec_id):
    """Fetch the top hero trees for a specific spec from the database."""
    params = (spec_id,)
    return fetch_with_retry(connection, cursor, FETCH_HERO_TREE_OVERVIEW_SQL, params)


FETCH_HERO_TREE_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, SUM(run_count), AVG(avg_rank) 
FROM Mythistone.aggregated_hero_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s  
GROUP BY aht.hero_talent_id, aht.dungeon_id 
"""


def fetch_hero_tree_differences(connection, cursor, spec_id, season):
    """Fetch the hero talents differences for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_HERO_TREE_DIFFERENCES_SQL, params)


FETCH_HERO_TALENTS_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count), AVG(avg_rank) 
FROM Mythistone.aggregated_hero_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s  
GROUP BY aht.talent_id, aht.hero_talent_id, aht.dungeon_id 
"""


def fetch_hero_talents_differences(connection, cursor, spec_id, season):
    """Fetch the hero talents differences for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(
        connection, cursor, FETCH_HERO_TALENTS_DIFFERENCES_SQL, params
    )


FETCH_SPEC_TALENTS_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count), AVG(avg_rank) 
FROM Mythistone.aggregated_spec_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s  
GROUP BY aht.talent_id, aht.hero_talent_id, aht.dungeon_id 
"""


def fetch_spec_talents_differences(connection, cursor, spec_id, season):
    """Fetch the spec talents differences for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(
        connection, cursor, FETCH_SPEC_TALENTS_DIFFERENCES_SQL, params
    )


FETCH_CLASS_TALENTS_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count), AVG(avg_rank) 
FROM Mythistone.aggregated_class_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s  
GROUP BY aht.talent_id, aht.hero_talent_id, aht.dungeon_id 
"""


def fetch_class_talents_differences(connection, cursor, spec_id, season):
    """Fetch the class talents differences for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(
        connection, cursor, FETCH_CLASS_TALENTS_DIFFERENCES_SQL, params
    )


FETCH_HERO_TALENTS_TOTAL_AMOUNT_SQL = """
SELECT COUNT(DISTINCT talent_id) AS distinct_talents
FROM Mythistone.aggregated_hero_talent
WHERE spec_id = %s
  AND season = %s;
"""


def fetch_hero_talent_total_amount(connection, cursor, spec_id, season):
    """Fetch the different amount of talents that we have data for"""
    params = (spec_id, season)
    return fetch_with_retry(
        connection, cursor, FETCH_HERO_TALENTS_TOTAL_AMOUNT_SQL, params
    )


FETCH_SPEC_DATA_COUNT_SQL = """
SELECT SUM(run_count) AS total_runs
FROM aggregated_spec
WHERE spec_id = %s
  AND hero_talent_id <> 0;

"""


def fetch_spec_data_count(connection, cursor, spec_id):
    """Fetch the spec data count for a specific spec from the database.
    Always returns an int (0 if no runs)."""
    params = (spec_id,)
    result = fetch_with_retry(connection, cursor, FETCH_SPEC_DATA_COUNT_SQL, params)

    if isinstance(result, dict):
        rows = next(iter(result.values()), [])
    else:
        rows = result or []

    if not rows:
        return 1

    first = rows[0]

    if isinstance(first, dict):
        val = first.get("total_runs")
    elif isinstance(first, (list, tuple)):
        val = first[0] if len(first) > 0 else None
    else:
        try:
            val = int(first)
        except Exception:
            return 1

    return int(val) if val is not None else 1


INSERT_EMBELLISHMENT_SQL = """
INSERT IGNORE INTO embellishments (`bonus_id`, `item_id`) VALUES (%s, %s)
"""

def insert_embellishment(connection, cursor, bonus_id, item_id):
    """Insert a new embellishment into the database."""
    params = (bonus_id, item_id)
    return execute_with_retry(connection, cursor, INSERT_EMBELLISHMENT_SQL, params)

INSERT_CRAFTED_ITEM_ID_SQL = """
  INSERT IGNORE INTO crafted_item_ids (`item_id`) VALUES (%s)
"""

def insert_crafted_item_id(connection, cursor, item_id):
    params = (item_id,)
    return execute_with_retry(connection, cursor, INSERT_CRAFTED_ITEM_ID_SQL, params)


INSERT_TIER_SET_ITEM_SQL = """
  INSERT IGNORE INTO tier_set_items (`item_id`, `item_set_id`) VALUES (%s, %s)
"""

def insert_tier_set_item(connection, cursor, item_id, item_set_id):
    """Insert a new tier set item into the database."""
    params = (item_id, item_set_id)
    return execute_with_retry(connection, cursor, INSERT_TIER_SET_ITEM_SQL, params)


INSERT_MISSIVE_SQL = """
INSERT IGNORE INTO missives (`bonus_id`, `item_id`) VALUES (%s, %s)
"""


def insert_missive(connection, cursor, bonus_id, item_id):
    """Insert a new missive into the database."""
    params = (bonus_id, item_id)
    return execute_with_retry(connection, cursor, INSERT_MISSIVE_SQL, params)


FETCH_MISSIVE_COUNT_SQL = """
SELECT item_id, run_count AS total_runs, max_timed_key, max_depleted_key
FROM Mythistone.global_aggregated_missives
WHERE spec_id = %s
  AND season = %s
ORDER BY total_runs DESC
"""


def fetch_missive_count(connection, cursor, spec_id, season):
    """Fetch the missive count for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_MISSIVE_COUNT_SQL, params)


FETCH_EMBELLISHMENT_COUNT_SQL = """
SELECT item_id, run_count AS total_runs, max_timed_key, max_depleted_key
FROM Mythistone.global_aggregated_embellishments
WHERE spec_id = %s
  AND season = %s
ORDER BY total_runs DESC
"""


def fetch_embellishment_count(connection, cursor, spec_id, season):
    """Fetch the embellishment count for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_EMBELLISHMENT_COUNT_SQL, params)


FETCH_CRAFTED_ITEMS_COUNT_SQL = """
SELECT item_id, run_count AS total_runs, max_timed_key, max_depleted_key
FROM Mythistone.global_aggregated_crafted_items
WHERE spec_id = %s
  AND season = %s
ORDER BY total_runs DESC
LIMIT 10
"""


def fetch_crafted_items_count(connection, cursor, spec_id, season):
    """Fetch the crafted items count for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_CRAFTED_ITEMS_COUNT_SQL, params)


FETCH_EMBELLISHMENT_COMPS_SQL = """
SELECT comp, SUM(run_count) AS total_runs, MAX(max_timed_key), MAX(max_depleted_key)
FROM Mythistone.aggregated_embellishment_comps
WHERE spec_id = %s
  AND season = %s
GROUP BY comp
ORDER BY total_runs DESC
LIMIT 15
"""


def fetch_embellishment_comps(connection, cursor, spec_id, season):
    """Fetch the embellishment comp counts for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_EMBELLISHMENT_COMPS_SQL, params)


FETCH_CRAFTED_COMPS_SQL = """
SELECT comp, SUM(run_count) AS total_runs, MAX(max_timed_key), MAX(max_depleted_key)
FROM Mythistone.aggregated_crafted_comps
WHERE spec_id = %s
  AND season = %s
GROUP BY comp
ORDER BY total_runs DESC
LIMIT 10
"""


def fetch_crafted_comps(connection, cursor, spec_id, season):
    """Fetch the crafted item comp counts for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_CRAFTED_COMPS_SQL, params)


FETCH_GEM_COMPS_SQL = """
SELECT comp, SUM(run_count) AS total_runs, MAX(max_timed_key), MAX(max_depleted_key)
FROM Mythistone.aggregated_gem_comps
WHERE spec_id = %s
  AND season = %s
GROUP BY comp
ORDER BY total_runs DESC
LIMIT 15
"""


def fetch_gem_comps(connection, cursor, spec_id, season):
    """Fetch the gem comp counts for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_GEM_COMPS_SQL, params)


FETCH_ENCHANT_COMPS_SQL = """
SELECT comp, SUM(run_count) AS total_runs, MAX(max_timed_key), MAX(max_depleted_key)
FROM Mythistone.aggregated_enchant_comps
WHERE spec_id = %s
  AND season = %s
GROUP BY comp
ORDER BY total_runs DESC
LIMIT 15
"""


def fetch_enchant_comps(connection, cursor, spec_id, season):
    """Fetch the enchantment comp counts for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_ENCHANT_COMPS_SQL, params)


FETCH_TIER_SET_COMPS_SQL = """
SELECT comp, SUM(run_count) AS total_runs, MAX(max_timed_key), MAX(max_depleted_key)
FROM Mythistone.aggregated_tier_set_comps
WHERE spec_id = %s
  AND season = %s
GROUP BY comp
ORDER BY total_runs DESC
LIMIT 10
"""


def fetch_tier_set_comps(connection, cursor, spec_id, season):
    """Fetch the tier set comp counts for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_TIER_SET_COMPS_SQL, params)


FETCH_TOTAL_SEASON_RUNS_SQL = """
SELECT COUNT(run_id) AS total_runs
FROM runs
WHERE season = %s
"""

PREAGG_TOTAL_SEASON_RUNS_SQL = """
SELECT SUM(total_runs) AS total_runs
FROM aggregated_runs_per_dungeon_per_level
WHERE season = %s
"""


def _fetch_runs_rollup_with_fallback(connection, cursor, preagg_sql, fallback_sql, params):
    """Read the nightly aggregated_runs_per_dungeon_per_level rollup; fall back
    to the live full-season runs scan when the rollup is missing (migration not
    applied yet) or holds nothing for the requested season (e.g. the season
    flipped before the nightly pipeline ran)."""
    try:
        rows = fetch_with_retry(connection, cursor, preagg_sql, params)
    except mysql.connector.DatabaseError as err:
        if err.errno != errorcode.ER_NO_SUCH_TABLE:
            raise
        rows = None
    # A SUM over zero rows yields a single all-NULL row, not an empty set
    if rows and any(
        v is not None for row in rows
        for v in (row.values() if isinstance(row, dict) else row)
    ):
        return rows
    print("aggregated_runs_per_dungeon_per_level unavailable or empty, falling back to live runs scan")
    return fetch_with_retry(connection, cursor, fallback_sql, params)


def fetch_total_season_runs(connection, cursor, season):
    """Fetch the total season runs for a specific season from the database."""
    rows = _fetch_runs_rollup_with_fallback(
        connection, cursor,
        PREAGG_TOTAL_SEASON_RUNS_SQL, FETCH_TOTAL_SEASON_RUNS_SQL, (season,),
    )
    amount_row = rows[0] if rows else None
    # amount_row might be tuple or dict depending on cursor type
    if not amount_row:
        return 0
    if isinstance(amount_row, dict):
        total_runs = amount_row.get("total_runs", 0)
    else:
        total_runs = amount_row[0] if amount_row[0] is not None else 0
    return total_runs


SEASON_HAS_RUNS_SQL = "SELECT 1 FROM runs WHERE season = %s LIMIT 1"


def season_has_runs(connection, cursor, season):
    """True if the season has any recorded runs — i.e. it is underway. False
    during the pre-season gap or just after a season wipe, when the runs table
    holds nothing for it yet. LIMIT 1 keeps it O(1). Used by the buildPages
    preflight (seasonHasData.py) and the Discord bot's season-not-started guard."""
    rows = fetch_with_retry(connection, cursor, SEASON_HAS_RUNS_SQL, (season,))
    return bool(rows)


FETCH_SEASON_RUNS_FOR_SPEC_SQL = """
SELECT SUM(run_count) AS total_runs
FROM aggregated_spec
WHERE spec_id = %s
"""


def fetch_runs_per_spec(connection, cursor, spec_id):
    """Fetch the total season runs for a specific season+spec and return an int."""
    params = (spec_id,)
    rows = fetch_with_retry(connection, cursor, FETCH_SEASON_RUNS_FOR_SPEC_SQL, params)
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        total_runs = row.get("total_runs", 0)
    else:
        total_runs = row[0] if row[0] is not None else 0
    return int(total_runs)


FETCH_SPEC_SAMPLE_SIZE_SQL = """
SELECT MAX(slot_total) AS sample_size FROM (
  SELECT SUM(run_count) AS slot_total
  FROM Mythistone.global_aggregated_equipment
  WHERE spec_id = %s AND season = %s
  GROUP BY slot
) t
"""


def fetch_spec_sample_size(connection, cursor, spec_id, season):
    """Fetch the number of sampled characters for a spec within the ~14-day
    gear-retention window: the busiest slot's total in
    global_aggregated_equipment (every character has e.g. a chest equipped).
    Unlike season-wide run counts, this matches the window the
    enchant/embellishment aggregations cover. Returns an int."""
    params = (spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_SAMPLE_SIZE_SQL, params)
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        sample_size = row.get("sample_size", 0)
    else:
        sample_size = row[0] if row[0] is not None else 0
    return int(sample_size)


FETCH_MAX_KEY_SPEC_SQL = """
WITH maxk AS (
  SELECT spec_id, MAX(keystone_level) AS max_keystone
  FROM aggregated_spec
  WHERE spec_id = %s
  GROUP BY spec_id
),
chosen_run AS (
  SELECT r.*
  FROM runs r
  JOIN maxk m ON r.keystone_level = m.max_keystone
  JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
  WHERE r.season = %s
    AND EXISTS (
    SELECT 1
    FROM run_members rm
    JOIN members mm ON mm.member = rm.member
    WHERE rm.run_id = r.run_id
      AND mm.spec_id = %s
  )
  -- Tie-break equal key levels by time left on the timer (par - duration),
  -- so a run that beat a longer timer by more ranks above a barely-timed
  -- shorter-timer clear even when the latter's raw duration is smaller.
  -- CAST to SIGNED: duration is unsigned, so over-time runs would otherwise
  -- underflow the subtraction and error (BIGINT UNSIGNED out of range).
  ORDER BY (CAST(dd.upgrade_1_duration AS SIGNED) - CAST(r.duration AS SIGNED)) DESC, r.timestamp ASC
  LIMIT 1
)
SELECT cr.*, mb.member AS member_id, mb.spec_id AS member_spec_id
FROM chosen_run cr
JOIN run_members rm ON rm.run_id = cr.run_id
JOIN members mb     ON mb.member = rm.member
ORDER BY mb.member;
"""


def fetch_max_key_run_per_spec(connection, cursor, spec_id, season):
    """Fetch the max key run for a specific spec and season from the database."""
    params = (spec_id, season, spec_id)
    raw = fetch_with_retry(connection, cursor, FETCH_MAX_KEY_SPEC_SQL, params)

    if not raw:
        print(f"No runs found for spec {spec_id} in season {season}")
        return None

    rows = list(raw)

    if not rows:
        print("No rows found")
        return None

    # first row contains run-level fields (same for all rows)
    first = rows[0]

    seen = set()
    members = []
    for r in rows:
        mid = r[8]
        mspec = r[9]
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        members.append(
            {
                "member_id": int(mid),
                "spec_id": int(mspec) if mspec is not None else None,
            }
        )
    top_run = {
        "run_id": int(first[5]) if len(first) > 5 and first[5] is not None else None,
        "dungeon_id": int(first[0])
        if len(first) > 0 and first[0] is not None
        else None,
        "keystone_level": int(first[1])
        if len(first) > 1 and first[1] is not None
        else None,
        "duration": int(first[2]) if len(first) > 2 and first[2] is not None else None,
        "timestamp": int(first[3]) if len(first) > 3 and first[3] is not None else None,
        "faction": first[4] if len(first) > 4 else None,
        "region": first[6] if len(first) > 5 else None,
        "season": int(first[7]) if len(first) > 6 and first[7] is not None else None,
        "members": members,
    }

    return top_run


FETCH_MAX_KEY_SQL = """
SELECT cr.*, mb.member AS member_id, mb.spec_id AS member_spec_id
FROM runs cr
LEFT JOIN run_members rm ON rm.run_id = cr.run_id
LEFT JOIN members mb     ON mb.member = rm.member
WHERE cr.run_id = (
    -- Highest key level first, then most time left on the timer
    -- (par - duration) so a bigger cushion against a longer timer outranks
    -- a barely-timed clear of a shorter-timer dungeon. CAST to SIGNED because
    -- duration is unsigned; over-time runs would otherwise underflow and error.
    SELECT r.run_id
    FROM runs r
    JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
    WHERE r.season = %s
    ORDER BY r.keystone_level DESC, (CAST(dd.upgrade_1_duration AS SIGNED) - CAST(r.duration AS SIGNED)) DESC, r.run_id ASC
    LIMIT 1
)
ORDER BY mb.member;
"""


def fetch_max_key_run(connection, cursor, season):
    """Fetch the max key run for a specific spec and season from the database."""
    params = (season,)
    raw = fetch_with_retry(connection, cursor, FETCH_MAX_KEY_SQL, params)

    if not raw:
        print(f"No runs found in season {season}")
        return None

    rows = list(raw)

    if not rows:
        print("No rows found")
        return None

    # first row contains run-level fields (same for all rows)
    first = rows[0]

    seen = set()
    members = []
    for r in rows:
        mid = r[8]
        mspec = r[9]
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        members.append(
            {
                "member_id": int(mid),
                "spec_id": int(mspec) if mspec is not None else None,
            }
        )
    top_run = {
        "run_id": int(first[5]) if len(first) > 5 and first[5] is not None else None,
        "dungeon_id": int(first[0])
        if len(first) > 0 and first[0] is not None
        else None,
        "keystone_level": int(first[1])
        if len(first) > 1 and first[1] is not None
        else None,
        "duration": int(first[2]) if len(first) > 2 and first[2] is not None else None,
        "timestamp": int(first[3]) if len(first) > 3 and first[3] is not None else None,
        "faction": first[4] if len(first) > 4 else None,
        "region": first[6] if len(first) > 5 else None,
        "season": int(first[7]) if len(first) > 6 and first[7] is not None else None,
        "members": members,
    }

    return top_run


FETCH_LONGEST_KEY_RUN_SQL = """
SELECT r.dungeon_id,
       r.keystone_level,
       r.duration,
       r.timestamp,
       r.faction,
       r.run_id,
       r.region,
       r.season,
       rm.member,
       m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT run_id
    FROM runs
    WHERE season = %s
    ORDER BY duration DESC, run_id ASC
    LIMIT 1
)
ORDER BY rm.member;
"""


def fetch_longest_run(connection, cursor, season):
    """
    Fetch the single longest run for a season (ties broken by smallest run_id),
    returning the same top_run dict structure as fetch_max_key_run.
    """
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_LONGEST_KEY_RUN_SQL, params)

    if not rows:
        print(f"No runs found in season {season}")
        return None

    rows = list(rows)
    if not rows:
        print("No rows found")
        return None

    first = rows[0]

    # collect unique members (preserve first-seen order)
    seen = set()
    members = []
    for r in rows:
        mid = r[8]
        mspec = r[9]
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        members.append(
            {
                "member_id": int(mid),
                "spec_id": int(mspec) if mspec is not None else None,
            }
        )

    top_run = {
        "run_id": int(first[5]) if len(first) > 5 and first[5] is not None else None,
        "dungeon_id": first[0] if len(first) > 0 else None,
        "keystone_level": int(first[1])
        if len(first) > 1 and first[1] is not None
        else None,
        "duration": int(first[2]) if len(first) > 2 and first[2] is not None else None,
        "timestamp": int(first[3]) if len(first) > 3 and first[3] is not None else None,
        "faction": first[4] if len(first) > 4 else None,
        "region": first[6] if len(first) > 6 else None,
        "season": int(first[7]) if len(first) > 7 and first[7] is not None else None,
        "members": members,
    }

    return top_run


FETCH_SHORTEST_KEY_RUN_SQL = """
SELECT r.dungeon_id,
       r.keystone_level,
       r.duration,
       r.timestamp,
       r.faction,
       r.run_id,
       r.region,
       r.season,
       rm.member,
       m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT run_id
    FROM runs
    WHERE season = %s
    ORDER BY duration ASC, run_id ASC
    LIMIT 1
)
AND duration > 0
ORDER BY rm.member;
"""


def fetch_shortest_run(connection, cursor, season):
    """
    Fetch the single shortest run for a season (ties broken by smallest run_id),
    returning the same top_run dict structure as fetch_max_key_run / fetch_longest_run.
    """
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_SHORTEST_KEY_RUN_SQL, params)

    if not rows:
        print(f"No runs found in season {season}")
        return None

    rows = list(rows)
    if not rows:
        print("No rows found")
        return None

    first = rows[0]

    # collect unique members (preserve first-seen order)
    seen = set()
    members = []
    for r in rows:
        mid = r[8]
        mspec = r[9]
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        members.append(
            {
                "member_id": int(mid),
                "spec_id": int(mspec) if mspec is not None else None,
            }
        )

    top_run = {
        "run_id": int(first[5]) if len(first) > 5 and first[5] is not None else None,
        "dungeon_id": first[0] if len(first) > 0 else None,
        "keystone_level": int(first[1])
        if len(first) > 1 and first[1] is not None
        else None,
        "duration": int(first[2]) if len(first) > 2 and first[2] is not None else None,
        "timestamp": int(first[3]) if len(first) > 3 and first[3] is not None else None,
        "faction": first[4] if len(first) > 4 else None,
        "region": first[6] if len(first) > 6 else None,
        "season": int(first[7]) if len(first) > 7 and first[7] is not None else None,
        "members": members,
    }

    return top_run


FETCH_SPEC_UPGRADE_SQL = """
SELECT upgrade_tier, sum(run_count) 
FROM Mythistone.aggregated_spec
WHERE spec_id = %s
GROUP BY upgrade_tier 

"""


def fetch_spec_upgrade(connection, cursor, spec_id):
    params = (spec_id,)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_UPGRADE_SQL, params)
    if not rows:
        return []
    upgrades = [{"upgrade_tier": row[0], "run_count": row[1]} for row in rows]
    upgrades.sort(
        key=lambda x: (
            x["upgrade_tier"] != "depleted",
            int(x["upgrade_tier"]) if x["upgrade_tier"] != "depleted" else -1,
        )
    )
    return upgrades


INSERT_PERIODS_SQL = """
INSERT IGNORE INTO Mythistone.season_periods (region, period_id, start_timestamp, end_timestamp, season) VALUES(%s, %s, %s, %s, %s);
"""


def insert_season_periods(
    connection, cursor, region, period_id, start_timestamp, end_timestamp, season
):
    """Insert the initial season periods into the database."""
    val = (region, period_id, start_timestamp, end_timestamp, season)
    execute_with_retry(connection, cursor, INSERT_PERIODS_SQL, val)
    return cursor.lastrowid


FETCH_SPEC_RUN_COUNTS = """
SELECT spec_id, SUM(run_count) AS count
FROM Mythistone.aggregated_spec
GROUP BY spec_id
ORDER BY count DESC
"""


def fetch_spec_run_counts(connection, cursor):
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_RUN_COUNTS, None)
    if not rows:
        return []
    return [{"id": int(row[0]), "count": int(row[1])} for row in rows]


FETCH_SPEC_RUN_COUNTS_PER_LEVEL = """
SELECT spec_id, keystone_level, SUM(run_count) AS count
FROM Mythistone.aggregated_spec
WHERE upgrade_tier <> 'depleted'
GROUP BY spec_id, keystone_level
ORDER BY spec_id, keystone_level;
"""


def fetch_spec_run_counts_per_level(connection, cursor):
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_RUN_COUNTS_PER_LEVEL, None)
    if not rows:
        return []
    return [
        {"spec_id": row[0], "keystone_level": row[1], "count": row[2]} for row in rows
    ]


FETCH_RUNS_PER_PERIOD = """
-- params: (season, season)
SELECT
  t.week,
  t.day_in_week,
  SUM(CASE WHEN t.upgrade_tier = '3' THEN 1 ELSE 0 END) AS tier_3,
  SUM(CASE WHEN t.upgrade_tier = '2' THEN 1 ELSE 0 END) AS tier_2,
  SUM(CASE WHEN t.upgrade_tier = '1' THEN 1 ELSE 0 END) AS tier_1,
  SUM(CASE WHEN t.upgrade_tier = 'depleted' THEN 1 ELSE 0 END) AS depleted,
  COUNT(*) AS total_runs
FROM (
  SELECT
    rp.week_number AS week,
    LEAST(GREATEST(FLOOR((r.timestamp - rp.start_timestamp) / 86400000) + 1, 1), 7) AS day_in_week,
    CASE
      WHEN r.duration IS NOT NULL AND dd.upgrade_3_duration IS NOT NULL AND r.duration <= dd.upgrade_3_duration THEN '3'
      WHEN r.duration IS NOT NULL AND dd.upgrade_2_duration IS NOT NULL AND r.duration <= dd.upgrade_2_duration THEN '2'
      WHEN r.duration IS NOT NULL AND dd.upgrade_1_duration IS NOT NULL AND r.duration <= dd.upgrade_1_duration THEN '1'
      ELSE 'depleted'
    END AS upgrade_tier
  FROM runs r
  LEFT JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
  JOIN (
    -- compute week_number per (season, region) without window functions
    SELECT
      sp.region,
      sp.season,
      sp.period_id,
      sp.start_timestamp,
      sp.end_timestamp,
      (
        SELECT COUNT(*)
        FROM season_periods sp2
        WHERE sp2.season = sp.season
          AND sp2.region = sp.region
          AND (
               sp2.start_timestamp < sp.start_timestamp
               OR (sp2.start_timestamp = sp.start_timestamp AND sp2.period_id <= sp.period_id)
          )
      ) AS week_number
    FROM season_periods sp
    WHERE sp.season = %s
  ) AS rp
    ON r.region = rp.region
   AND r.season = rp.season
   AND r.timestamp >= rp.start_timestamp
   AND r.timestamp < rp.end_timestamp
  WHERE r.season = %s
) AS t
GROUP BY t.week, t.day_in_week
ORDER BY t.week, t.day_in_week;
"""


def fetch_runs_per_period(connection, cursor, season):
    params = (season, season)
    rows = fetch_with_retry(connection, cursor, FETCH_RUNS_PER_PERIOD, params)
    if not rows:
        return []
    return [
        {
            "week": int(row[0]),
            "day": int(row[1]),
            "upgrade_3": int(row[2]),
            "upgrade_2": int(row[3]),
            "upgrade_1": int(row[4]),
            "depleted": int(row[5]),
            "total_runs": int(row[6]),
        }
        for row in rows
    ]


# Per-(region, period, day) run counts straight from the runs table. Used only
# for the season week-1 "Key Throughput" daily breakdown, where per-region daily
# lines are needed but aggregated_key_throughput has no per-day grain. Keyed by
# period_id (not a COUNT-derived week number) so callers can line each region up
# with the SAME period_id it carries in aggregated_key_throughput. The join is
# the exact runs<->season_periods timestamp join sp_agg_key_throughput uses to
# build aggregated_key_throughput, so a (region, period) present there resolves
# to the same day rows here. day_in_week is region-relative (days counted from
# that region's own period start).
FETCH_RUNS_PER_REGION_DAY = """
-- params: (season,)
SELECT
  sp.region,
  sp.period_id,
  LEAST(GREATEST(FLOOR((r.timestamp - sp.start_timestamp) / 86400000) + 1, 1), 7) AS day_in_week,
  COUNT(*) AS run_count
FROM runs r
JOIN season_periods sp
  ON sp.region = r.region
 AND sp.season = r.season
 AND r.timestamp >= sp.start_timestamp
 AND r.timestamp <  sp.end_timestamp
WHERE r.season = %s
GROUP BY sp.region, sp.period_id, day_in_week
ORDER BY sp.region, sp.period_id, day_in_week;
"""


def fetch_runs_per_region_day(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_RUNS_PER_REGION_DAY, params)
    if not rows:
        return []
    return [
        {
            "region": row[0],
            "period_id": int(row[1]),
            "day": int(row[2]),
            "run_count": int(row[3]),
        }
        for row in rows
    ]


# Latest run timestamp per region for a season. The dashboard generator's period
# self-heal compares this against the last season_period end it knows about to
# detect a region whose current period is missing from season_periods.
FETCH_REGION_RUN_EXTENT = """
SELECT region, MAX(timestamp) AS max_ts
FROM runs
WHERE season = %s
GROUP BY region;
"""


def fetch_region_run_extent(connection, cursor, season):
    rows = fetch_with_retry(connection, cursor, FETCH_REGION_RUN_EXTENT, (season,))
    if not rows:
        return {}
    return {
        (row[0].lower() if isinstance(row[0], str) else row[0]): (
            int(row[1]) if row[1] is not None else None
        )
        for row in rows
    }


# run_count + max_ts for one (region, period) window straight from runs, matching
# the runs<->season_periods timestamp join. Lets the self-heal inject a healed
# region's throughput row in memory without rebuilding aggregated_key_throughput.
FETCH_PERIOD_RUN_STATS = """
SELECT COUNT(*) AS run_count, MAX(timestamp) AS max_ts
FROM runs
WHERE season = %s AND region = %s AND timestamp >= %s AND timestamp < %s;
"""


def fetch_period_run_stats(connection, cursor, season, region, start_ts, end_ts):
    rows = fetch_with_retry(
        connection, cursor, FETCH_PERIOD_RUN_STATS, (season, region, start_ts, end_ts)
    )
    if not rows:
        return {"run_count": 0, "max_ts": None}
    run_count = int(rows[0][0] or 0)
    max_ts = int(rows[0][1]) if rows[0][1] is not None else None
    return {"run_count": run_count, "max_ts": max_ts}


FETCH_KEY_THROUGHPUT_SQL = """
SELECT region, period_id, run_count, max_ts
FROM aggregated_key_throughput
WHERE season = %s
ORDER BY period_id, region;
"""


def fetch_key_throughput(connection, cursor, season):
    """
    Per-region, per-period key throughput for the dashboard "Key Throughput"
    chart.

    Reads the pre-aggregated `aggregated_key_throughput` table (populated by the
    ev_update_key_throughput event) rather than scanning the full runs table at
    page-build time. Returns one row per (region, period): run count and the
    latest recorded run timestamp (max_ts). Period start/end bounds are static
    and supplied by the caller from data/static/periods.json; keys-per-minute is
    derived as count / period length, so a collection gap shrinks the count
    rather than inflating the rate.
    """
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_KEY_THROUGHPUT_SQL, params)
    if not rows:
        return []
    return [
        {
            "region": row[0],
            "period_id": int(row[1]),
            "run_count": int(row[2]) if row[2] is not None else 0,
            "max_ts": int(row[3]) if row[3] is not None else None,
        }
        for row in rows
    ]


FETCH_COMPLETION_HEATMAP_SQL = """
SELECT region, day_of_week, hour_of_day, run_count
FROM aggregated_completion_heatmap
WHERE season = %s
ORDER BY region, day_of_week, hour_of_day;
"""


def fetch_completion_heatmap(connection, cursor, season):
    """
    Per-region day-of-week x hour-of-day completion counts for the dashboard
    "When are keys completed?" heatmap.

    Reads the pre-aggregated `aggregated_completion_heatmap` table (rebuilt by
    the nightly pipeline's sp_agg_completion_heatmap step) rather than scanning
    the full runs table at page-build time. day_of_week is 0=Sunday..6=Saturday and
    hour_of_day is 0-23, both in UTC — matching JS Date.getUTCDay() so the
    client can rotate the grid into the viewer's local time.
    """
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_COMPLETION_HEATMAP_SQL, params)
    if not rows:
        return []
    return [
        {
            "region": row[0],
            "day": int(row[1]),
            "hour": int(row[2]),
            "count": int(row[3]) if row[3] is not None else 0,
        }
        for row in rows
    ]


DUNGEON_UPGRADES_SQL = """
SELECT
  r.dungeon_id,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_3_duration IS NOT NULL
               AND r.duration <= dd.upgrade_3_duration THEN 1 ELSE 0 END) AS tier_3,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_2_duration IS NOT NULL
               AND r.duration <= dd.upgrade_2_duration
               AND NOT (r.duration <= dd.upgrade_3_duration) THEN 1 ELSE 0 END) AS tier_2,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_1_duration IS NOT NULL
               AND r.duration <= dd.upgrade_1_duration
               AND NOT (r.duration <= dd.upgrade_2_duration) THEN 1 ELSE 0 END) AS tier_1,
  SUM(CASE WHEN r.duration IS NULL
               OR (dd.upgrade_1_duration IS NOT NULL AND r.duration > dd.upgrade_1_duration)
               THEN 1 ELSE 0 END) AS depleted,
  COUNT(*) AS total_runs
FROM runs r
JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
WHERE r.season = %s
GROUP BY r.dungeon_id
ORDER BY total_runs DESC;
"""

PREAGG_RUNS_PER_DUNGEON_SQL = """
SELECT
  dungeon_id,
  SUM(tier_3)     AS tier_3,
  SUM(tier_2)     AS tier_2,
  SUM(tier_1)     AS tier_1,
  SUM(depleted)   AS depleted,
  SUM(total_runs) AS total_runs
FROM aggregated_runs_per_dungeon_per_level
WHERE season = %s
GROUP BY dungeon_id
ORDER BY total_runs DESC;
"""


def fetch_runs_per_dungeon(connection, cursor, season):
    params = (season,)
    rows = _fetch_runs_rollup_with_fallback(
        connection, cursor, PREAGG_RUNS_PER_DUNGEON_SQL, DUNGEON_UPGRADES_SQL, params
    )
    if not rows:
        return []
        
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({
                "dungeon_id": row["dungeon_id"],
                "upgrade_3": int(row["tier_3"]),
                "upgrade_2": int(row["tier_2"]),
                "upgrade_1": int(row["tier_1"]),
                "depleted": int(row["depleted"]),
                "total_runs": int(row["total_runs"]),
            })
        else:
            out.append({
                "dungeon_id": row[0],
                "upgrade_3": int(row[1]),
                "upgrade_2": int(row[2]),
                "upgrade_1": int(row[3]),
                "depleted": int(row[4]),
                "total_runs": int(row[5]),
            })
    return out


FETCH_SPEC_UPGRADES_SQL = """
SELECT
    spec_id,
    keystone_level,
    SUM(CASE WHEN upgrade_tier = '3' THEN run_count ELSE 0 END) AS tier_3,
    SUM(CASE WHEN upgrade_tier = '2' THEN run_count ELSE 0 END) AS tier_2,
    SUM(CASE WHEN upgrade_tier = '1' THEN run_count ELSE 0 END) AS tier_1,
    SUM(CASE WHEN upgrade_tier = 'depleted' THEN run_count ELSE 0 END) AS depleted,
    SUM(run_count) AS total_runs
FROM aggregated_spec
GROUP BY spec_id, keystone_level
ORDER BY total_runs DESC;

"""


def fetch_spec_upgrades(connection, cursor):
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_UPGRADES_SQL, None)
    if not rows:
        return []
    return [
        {
            "spec_id": int(row[0]),
            "keystone_level": int(row[1]),
            "upgrade_3": int(row[2]),
            "upgrade_2": int(row[3]),
            "upgrade_1": int(row[4]),
            "depleted": int(row[5]),
            "total_runs": int(row[6]),
        }
        for row in rows
    ]


FETCH_SPEC_UPGRADES_ABOVE_LEVEL_SQL = """
SELECT
    spec_id,
    keystone_level,
    SUM(CASE WHEN upgrade_tier = '3' THEN run_count ELSE 0 END) AS tier_3,
    SUM(CASE WHEN upgrade_tier = '2' THEN run_count ELSE 0 END) AS tier_2,
    SUM(CASE WHEN upgrade_tier = '1' THEN run_count ELSE 0 END) AS tier_1,
    SUM(CASE WHEN upgrade_tier = 'depleted' THEN run_count ELSE 0 END) AS depleted,
    SUM(run_count) AS total_runs
FROM aggregated_spec
WHERE keystone_level > %s
GROUP BY spec_id, keystone_level
ORDER BY total_runs DESC;

"""


def fetch_spec_upgrades_above_level(connection, cursor, min_keylevel=15):
    params = (min_keylevel,)
    rows = fetch_with_retry(
        connection, cursor, FETCH_SPEC_UPGRADES_ABOVE_LEVEL_SQL, params
    )
    if not rows:
        return []
    return [
        {
            "spec_id": int(row[0]),
            "keystone_level": int(row[1]),
            "upgrade_3": int(row[2]),
            "upgrade_2": int(row[3]),
            "upgrade_1": int(row[4]),
            "depleted": int(row[5]),
            "total_runs": int(row[6]),
        }
        for row in rows
    ]


FETCH_UPGRADES_FOR_SPECS_SQL = """
SELECT
    keystone_level,
    SUM(CASE WHEN upgrade_tier = '3' THEN run_count ELSE 0 END) AS tier_3,
    SUM(CASE WHEN upgrade_tier = '2' THEN run_count ELSE 0 END) AS tier_2,
    SUM(CASE WHEN upgrade_tier = '1' THEN run_count ELSE 0 END) AS tier_1,
    SUM(CASE WHEN upgrade_tier = 'depleted' THEN run_count ELSE 0 END) AS depleted,
    SUM(run_count) AS total_runs
FROM aggregated_spec
WHERE spec_id IN ({placeholders}) and keystone_level > %s
GROUP BY keystone_level
ORDER BY total_runs DESC;

"""


def fetch_upgrade_for_specs(connection, cursor, specs, min_keylevel=15):
    spec_placeholder = ",".join(["%s"] * len(specs))
    specs_clean = [str(i) for i in specs]
    sql = FETCH_UPGRADES_FOR_SPECS_SQL.format(placeholders=spec_placeholder)
    params = specs_clean + [min_keylevel]
    rows = fetch_with_retry(connection, cursor, sql, params)
    if not rows:
        return []
    return [
        {
            "keystone_level": int(row[0]),
            "upgrade_3": int(row[1]),
            "upgrade_2": int(row[2]),
            "upgrade_1": int(row[3]),
            "depleted": int(row[4]),
            "total_runs": int(row[5]),
        }
        for row in rows
    ]


DUNGEON_UPGRADES_PER_KEYLEVEL_SQL = """
SELECT
  r.dungeon_id,
  r.keystone_level,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_3_duration IS NOT NULL
               AND r.duration <= dd.upgrade_3_duration THEN 1 ELSE 0 END) AS tier_3,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_2_duration IS NOT NULL
               AND r.duration <= dd.upgrade_2_duration
               AND NOT (r.duration <= dd.upgrade_3_duration) THEN 1 ELSE 0 END) AS tier_2,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_1_duration IS NOT NULL
               AND r.duration <= dd.upgrade_1_duration
               AND NOT (r.duration <= dd.upgrade_2_duration) THEN 1 ELSE 0 END) AS tier_1,
  SUM(CASE WHEN r.duration IS NULL
               OR (dd.upgrade_1_duration IS NOT NULL AND r.duration > dd.upgrade_1_duration)
               THEN 1 ELSE 0 END) AS depleted,
  COUNT(*) AS total_runs
FROM runs r
JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
WHERE r.season = %s
GROUP BY r.dungeon_id, r.keystone_level
"""

PREAGG_RUNS_PER_DUNGEON_PER_LEVEL_SQL = """
SELECT dungeon_id, keystone_level, tier_3, tier_2, tier_1, depleted, total_runs
FROM aggregated_runs_per_dungeon_per_level
WHERE season = %s
"""


def fetch_runs_per_dungeon_per_level(connection, cursor, season):
    params = (season,)
    rows = _fetch_runs_rollup_with_fallback(
        connection, cursor,
        PREAGG_RUNS_PER_DUNGEON_PER_LEVEL_SQL, DUNGEON_UPGRADES_PER_KEYLEVEL_SQL, params,
    )
    if not rows:
        return []
    
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({
                "dungeon_id": row["dungeon_id"],
                "keystone_level": int(row["keystone_level"]),
                "upgrade_3": int(row["tier_3"]),
                "upgrade_2": int(row["tier_2"]),
                "upgrade_1": int(row["tier_1"]),
                "depleted": int(row["depleted"]),
                "total_runs": int(row["total_runs"]),
            })
        else:
            out.append({
                "dungeon_id": row[0],
                "keystone_level": int(row[1]),
                "upgrade_3": int(row[2]),
                "upgrade_2": int(row[3]),
                "upgrade_1": int(row[4]),
                "depleted": int(row[5]),
                "total_runs": int(row[6]),
            })
    return out


DUNGEON_UPGRADES_PER_KEYLEVEL_ABOVE_LEVEL_SQL = """
SELECT
  r.dungeon_id,
  r.keystone_level,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_3_duration IS NOT NULL
               AND r.duration <= dd.upgrade_3_duration THEN 1 ELSE 0 END) AS tier_3,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_2_duration IS NOT NULL
               AND r.duration <= dd.upgrade_2_duration
               AND NOT (r.duration <= dd.upgrade_3_duration) THEN 1 ELSE 0 END) AS tier_2,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_1_duration IS NOT NULL
               AND r.duration <= dd.upgrade_1_duration
               AND NOT (r.duration <= dd.upgrade_2_duration) THEN 1 ELSE 0 END) AS tier_1,
  SUM(CASE WHEN r.duration IS NULL
               OR (dd.upgrade_1_duration IS NOT NULL AND r.duration > dd.upgrade_1_duration)
               THEN 1 ELSE 0 END) AS depleted,
  COUNT(*) AS total_runs
FROM runs r
JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
WHERE r.season = %s AND r.keystone_level > %s
GROUP BY r.dungeon_id, r.keystone_level
"""

PREAGG_RUNS_PER_DUNGEON_PER_LEVEL_ABOVE_LEVEL_SQL = """
SELECT dungeon_id, keystone_level, tier_3, tier_2, tier_1, depleted, total_runs
FROM aggregated_runs_per_dungeon_per_level
WHERE season = %s AND keystone_level > %s
"""


def fetch_runs_per_dungeon_per_level_above_level(
    connection, cursor, season, min_keylevel=15
):
    params = (season, min_keylevel)
    rows = _fetch_runs_rollup_with_fallback(
        connection, cursor,
        PREAGG_RUNS_PER_DUNGEON_PER_LEVEL_ABOVE_LEVEL_SQL,
        DUNGEON_UPGRADES_PER_KEYLEVEL_ABOVE_LEVEL_SQL, params,
    )
    if not rows:
        return []
    return [
        {
            "dungeon_id": int(row[0]),
            "keystone_level": int(row[1]),
            "upgrade_3": int(row[2]),
            "upgrade_2": int(row[3]),
            "upgrade_1": int(row[4]),
            "depleted": int(row[5]),
            "total_runs": int(row[6]),
        }
        for row in rows
    ]


DUNGEON_TIMED_RUNS_LAST_TWO_PERIODS_SQL = """
-- params: (season, season)
SELECT
  sp.period_id,
  r.dungeon_id,
  SUM(CASE WHEN r.duration IS NOT NULL
               AND dd.upgrade_1_duration IS NOT NULL
               AND r.duration <= dd.upgrade_1_duration THEN 1 ELSE 0 END) AS timed_runs
FROM (
  SELECT MAX(period_id) AS cur_period
  FROM season_periods
  WHERE season = %s
    AND start_timestamp <= CAST(UNIX_TIMESTAMP() * 1000 AS UNSIGNED)
) AS latest
JOIN season_periods sp
  ON sp.season = %s
 AND sp.period_id >= latest.cur_period - 1
JOIN runs r
  ON r.region = sp.region
 AND r.season = sp.season
 AND r.timestamp >= sp.start_timestamp
 AND r.timestamp < sp.end_timestamp
JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
GROUP BY sp.period_id, r.dungeon_id
"""


def fetch_dungeon_timed_runs_last_two_periods(connection, cursor, season):
    """Timed-run counts per dungeon for the two most recent weekly periods that
    have started (per-region reset windows from season_periods). Feeds the
    week-over-week trend panel on the dungeon popularity image."""
    params = (season, season)
    rows = fetch_with_retry(
        connection, cursor, DUNGEON_TIMED_RUNS_LAST_TWO_PERIODS_SQL, params
    )
    if not rows:
        return []
    return [
        {
            "period_id": int(row[0]),
            "dungeon_id": row[1],
            "timed_runs": int(row[2]),
        }
        for row in rows
    ]


FETCH_SPEC_TALENT_OVERVIEW_SQL = """
SELECT talent_id, SUM(run_count) AS count
FROM Mythistone.aggregated_spec_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s
GROUP BY aht.talent_id
ORDER BY count DESC
"""


def fetch_spec_talent_overview(connection, cursor, spec_id, season):
    params = (spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_TALENT_OVERVIEW_SQL, params)
    if not rows:
        return []
    return [{"talent_id": int(row[0]), "count": int(row[1])} for row in rows]


FETCH_GROUPBUFFS_SQL_TEMPLATE = """
SELECT
  COUNT(*) AS total_runs,
  {select_cols}
FROM (
  SELECT
    r.run_id,
    {has_cols}
  FROM runs r
  LEFT JOIN run_members rm ON rm.run_id = r.run_id
  LEFT JOIN members m ON m.member = rm.member
  WHERE r.season = %s
    AND r.keystone_level > %s
    AND r.timestamp >= CAST(UNIX_TIMESTAMP(NOW() - INTERVAL %s DAY) * 1000 AS UNSIGNED)
  GROUP BY r.run_id
) sub;
"""


def build_simple_groupbuffs_query(groupbuffs):
    """
    groupbuffs: list of dicts like {"name": "Arcane Intellect", "spec_ids": [62,63,64]}
    returns: sql string and number of buffs (for result mapping)
    """
    has_cols = []
    select_cols = []
    for i, buff in enumerate(groupbuffs):
        has_alias = f"has_{i}"
        runs_alias = f"runs_{i}"
        pct_alias = f"pct_{i}"

        spec_ids = buff.get("specIDs", [])
        if not spec_ids:
            # no specs -> always 0
            has_expr = "0"
        else:
            # safe because we convert to ints here
            ids = ",".join(str(int(x)) for x in spec_ids)
            has_expr = f"COALESCE(MAX(m.spec_id IN ({ids})), 0)"

        has_cols.append(f"{has_expr} AS {has_alias}")
        select_cols.append(f"SUM({has_alias}) AS {runs_alias}")
        select_cols.append(
            f"ROUND(100.0 * SUM({has_alias}) / NULLIF(COUNT(*), 0), 4) AS {pct_alias}"
        )
    return FETCH_GROUPBUFFS_SQL_TEMPLATE.format(
        has_cols=",\n    ".join(has_cols), select_cols=",\n  ".join(select_cols)
    ), len(groupbuffs)


def fetch_groupbuffs_stats(
    connection, cursor, groupbuffs, season, keystone_threshold=11, days_back=14
):
    """
    Executes the dynamically built SQL and returns:
      {"total_runs": int, "buffs": [ { "name":..., "spec_ids":..., "runs":int, "pct":float }, ... ] }
    - Uses fetch_with_retry(connection, cursor, sql, params) if available; otherwise uses cursor.execute.
    """
    sql, n = build_simple_groupbuffs_query(groupbuffs)
    params = (int(season), int(keystone_threshold), int(days_back))

    rows = fetch_with_retry(connection, cursor, sql, params)

    if not rows:
        return {"total_runs": 0, "buffs": []}

    row = rows[0]
    total_runs = int(row[0] or 0)
    buffs_out = []
    off = 1
    for i, buff in enumerate(groupbuffs):
        runs = int(row[off] or 0)
        pct = float(row[off + 1] or 0.0)
        buffs_out.append({"id": buff.get("id"), "runs": runs, "pct": pct})
        off += 2

    return {"total_runs": total_runs, "buffs": buffs_out}


FETCH_CLASS_TALENT_OVERVIEW_SQL = """
SELECT talent_id, SUM(run_count) AS count
FROM Mythistone.aggregated_class_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s
GROUP BY aht.talent_id
ORDER BY count DESC
"""


def fetch_class_talent_overview(connection, cursor, spec_id, season):
    params = (spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_CLASS_TALENT_OVERVIEW_SQL, params)
    if not rows:
        return []
    return [{"talent_id": int(row[0]), "count": int(row[1])} for row in rows]


FETCH_STATS_SQL = """
SELECT run_count, stat, avg_percent, avg_raw, min_raw, max_raw 
FROM Mythistone.aggregated_character_stats
WHERE spec_id = %s and season = %s
ORDER BY avg_raw DESC
"""


def fetch_stats(connection, cursor, spec_id, season):
    params = (spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_STATS_SQL, params)
    if not rows:
        return []
    data = {}
    for row in rows:
        data[row[1]] = {
            "run_count": int(row[0]),
            "avg_percent": float(row[2]) if row[2] else None,
            "avg_raw": float(row[3]),
            "min_raw": float(row[4]),
            "max_raw": float(row[5]),
        }
    return data


INSERT_PULL_ENEMIES_SQL = """
INSERT INTO Mythistone.pull_enemies (`route_key`, `pull_id`, `npc_id`, `count`) VALUES(%s, %s, %s, %s);
"""


def insert_pull_enemies(connection, cursor, route_key, pull_id, npc_id, count):
    """Insert a new enemy to a pull."""
    val = (route_key, pull_id, npc_id, count)
    execute_with_retry(connection, cursor, INSERT_PULL_ENEMIES_SQL, val)
    return cursor.rowcount


INSERT_PULL_SPELLS_SQL = """
INSERT INTO Mythistone.pull_spells (`route_key`, `pull_id`, `spell_id`) VALUES(%s, %s, %s);
"""


def insert_pull_spells(connection, cursor, route_key, pull_id, spell_id):
    """Insert a new spell to a pull."""
    val = (route_key, pull_id, spell_id)
    execute_with_retry(connection, cursor, INSERT_PULL_SPELLS_SQL, val)
    return cursor.rowcount


INSERT_ROUTE_DATA_SQL = """
INSERT IGNORE INTO Mythistone.route_data (`rio_run_id`, `mapping_version`, `enemy_forces`, `timestamp`, `keystone_level`, `duration`, `dungeon_id`, `route_key`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
"""


def insert_route_data(
    connection,
    cursor,
    rio_run_id,
    mapping_version,
    enemy_forces,
    timestamp,
    keystone_level,
    duration,
    dungeon_id,
    route_key,
):
    """Insert a new route into the database."""
    val = (
        rio_run_id,
        mapping_version,
        enemy_forces,
        timestamp,
        keystone_level,
        duration,
        dungeon_id,
        route_key,
    )
    execute_with_retry(connection, cursor, INSERT_ROUTE_DATA_SQL, val)
    return cursor.rowcount


INSERT_ROUTE_PULL_SQL = """
INSERT INTO Mythistone.route_pulls (`route_key`) VALUES(%s);
"""


def insert_route_pull(connection, cursor, route_key):
    """Add a new pull to a route"""
    val = (route_key,)
    execute_with_retry(connection, cursor, INSERT_ROUTE_PULL_SQL, val)
    return cursor.lastrowid


INSERT_ROUTE_SPEC_SQL = """
INSERT INTO Mythistone.route_specs (`route_key`, `spec_id`) VALUES(%s, %s);
"""


def insert_route_spec(connection, cursor, route_key, spec_id):
    """Insert a new spec to a route."""
    val = (route_key, spec_id)
    execute_with_retry(connection, cursor, INSERT_ROUTE_SPEC_SQL, val)
    return cursor.rowcount


def fetch_route_specs_map(connection, cursor):
    """
    Return dict: { route_key: [spec_id, ...], ... }
    """
    sql = "SELECT route_key, spec_id FROM Mythistone.route_specs;"
    rows = fetch_with_retry(connection, cursor, sql, None)
    out = {}
    for r in rows:
        rk = r[0]
        sid = int(r[1])
        out.setdefault(rk, []).append(sid)
    for rk in out:
        out[rk] = sorted(list(set(out[rk])))
    return out


def fetch_route_npcs_map(connection, cursor):
    """
    Return dict: { route_key: [npc_id, ...], ... }
    Aggregates NPCs across pulls for each route.
    """
    sql = """
    SELECT route_key, npc_id, SUM(count) as total_count
    FROM Mythistone.pull_enemies
    GROUP BY route_key, npc_id;
    """
    rows = fetch_with_retry(connection, cursor, sql, None)
    out = {}
    for r in rows:
        rk = r[0]
        npc = int(r[1])
        out.setdefault(rk, []).append(npc)
    # unique + sorted
    return {rk: sorted(list(set(v))) for rk, v in out.items()}


def fetch_route_spells_map(connection, cursor):
    """
    Return dict: { route_key: [spell_id, ...], ... }
    """
    sql = "SELECT route_key, spell_id FROM Mythistone.pull_spells;"
    rows = fetch_with_retry(connection, cursor, sql, None)
    out = {}
    for r in rows:
        rk = r[0]
        sid = int(r[1])
        out.setdefault(rk, []).append(sid)
    return {rk: sorted(list(set(v))) for rk, v in out.items()}


def fetch_comp_routes(
    connection, cursor, recent_only_days=None, min_level=0, limit=None
):
    """
    Build compRoutes-style dict directly from DB.
    Returns: { "specA,specB": { route_key, run_id, dungeon, level, duration, timestamp, specs, npcs, spells, enemy_forces }, ... }
    This function raises on DB errors (caller should catch).
    """
    # We need a large group_concat_max_len for the signature
    try:
        cursor.execute("SET SESSION group_concat_max_len = 1000000;")
    except Exception:
        pass

    # base SELECT with new duplicate aggregation logic
    sql = """
    WITH PullEnemies AS (
        SELECT 
            route_key, 
            pull_id, 
            GROUP_CONCAT(CONCAT(npc_id, ':', count) ORDER BY npc_id ASC SEPARATOR ',') AS enemies
        FROM Mythistone.pull_enemies
        GROUP BY route_key, pull_id
    ),
    PullSpells AS (
        SELECT 
            route_key, 
            pull_id, 
            GROUP_CONCAT(spell_id ORDER BY spell_id ASC SEPARATOR ',') AS spells
        FROM Mythistone.pull_spells
        GROUP BY route_key, pull_id
    ),
    RouteSignatures AS (
        SELECT 
            rp.route_key,
            GROUP_CONCAT(
                CONCAT('{E:', COALESCE(pe.enemies, ''), '}{S:', COALESCE(ps.spells, ''), '}') 
                ORDER BY rp.pull_id ASC 
                SEPARATOR ' | '
            ) AS route_signature
        FROM Mythistone.route_pulls rp
        LEFT JOIN PullEnemies pe ON rp.route_key = pe.route_key AND rp.pull_id = pe.pull_id
        LEFT JOIN PullSpells ps ON rp.route_key = ps.route_key AND rp.pull_id = ps.pull_id
        GROUP BY rp.route_key
    )
    """
    
    # We will inject the standard WHERE clauses into the CTE below to pre-filter
    where_clauses = []
    params = []

    if min_level and int(min_level) > 0:
        where_clauses.append("rd_base.keystone_level >= %s")
        params.append(int(min_level))

    if recent_only_days:
        where_clauses.append(
            "rd_base.timestamp >= CAST(UNIX_TIMESTAMP(NOW() - INTERVAL %s DAY) AS UNSIGNED)"
        )
        params.append(int(recent_only_days))
        
    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
        
    sql += f"""
    , RankedRoutes AS (
        SELECT 
            rs.route_signature,
            rs.route_key,
            rd_base.rio_run_id as run_id,
            rd_base.enemy_forces,
            rd_base.timestamp,
            rd_base.keystone_level,
            rd_base.duration,
            rd_base.dungeon_id,
            COUNT(rs.route_key) OVER (PARTITION BY rs.route_signature) as usage_count,
            ROW_NUMBER() OVER (PARTITION BY rs.route_signature ORDER BY rd_base.keystone_level DESC, rd_base.duration ASC) as rn
        FROM RouteSignatures rs
        JOIN Mythistone.route_data rd_base ON rs.route_key = rd_base.route_key
        {where_sql}
    )
    SELECT 
        route_key, 
        run_id, 
        enemy_forces, 
        timestamp, 
        keystone_level, 
        duration, 
        dungeon_id,
        usage_count
    FROM RankedRoutes
    WHERE rn = 1
    ORDER BY usage_count DESC
    """
    
    if limit:
        sql += f" LIMIT {int(limit)}"
        
    sql += ";"
    
    rows = fetch_with_retry(connection, cursor, sql, tuple(params) if params else None)

    route_specs_map = fetch_route_specs_map(connection, cursor)
    route_npcs_map = fetch_route_npcs_map(connection, cursor)
    route_spells_map = fetch_route_spells_map(connection, cursor)

    out = {}
    # We'll create a unique key per route based on sorted spec list (same pattern as compRoutes)
    for row in rows:
        route_key = row[0]
        rio_run_id = row[1]
        enemy_forces = int(row[2]) if row[2] is not None else None
        timestamp = int(row[3]) if row[3] is not None else None
        keystone_level = int(row[4]) if row[4] is not None else None
        duration = int(row[5]) if row[5] is not None else None
        dungeon_id = str(row[6]) if row[6] is not None else None
        usage_count = int(row[7]) if len(row) > 7 and row[7] is not None else 1

        specs = route_specs_map.get(route_key, [])
        spec_key = ",".join(str(s) for s in sorted(specs)) if specs else "unknown"

        # Instead of just spec_key, we want each route to stand on its own in the list.
        # But out is a dict. Let's use route_key as the unique dictionary key
        out[route_key] = {
            "route_key": route_key,
            "run_id": int(rio_run_id) if rio_run_id is not None else None,
            "dungeon": dungeon_id,
            "level": keystone_level,
            "duration": duration,
            "timestamp": timestamp,
            "specs": specs,
            "npcs": route_npcs_map.get(route_key, []),
            "spells": route_spells_map.get(route_key, []),
            "enemy_forces": enemy_forces,
            "usage_count": usage_count,
        }
    return out


FETCH_DISTINCT_SPELL_IDS_SQL = """
SELECT DISTINCT ps.spell_id from Mythistone.pull_spells ps
"""


def fetch_distinct_spell_ids(connection, cursor):
    """
    Fetch all distinct spell IDs recorded in pull_spells.
    Returns list of int spell IDs (may be empty).
    """
    rows = fetch_with_retry(connection, cursor, FETCH_DISTINCT_SPELL_IDS_SQL, None)
    if not rows:
        return []
    return [int(r[0]) for r in rows if r and r[0] is not None]


FETCH_DISTINCT_NPC_IDS_SQL = """
SELECT DISTINCT pe.npc_id from Mythistone.pull_enemies pe
"""


def fetch_distinct_npc_ids(connection, cursor):
    """
    Fetch all distinct NPC IDs recorded in pull_enemies.
    Returns list of int NPC IDs (may be empty).
    """
    rows = fetch_with_retry(connection, cursor, FETCH_DISTINCT_NPC_IDS_SQL, None)
    if not rows:
        return []
    return [int(r[0]) for r in rows if r and r[0] is not None]


FETCH_DISTINCT_NPC_IDS_FOR_DUNGEON_SQL = """
SELECT DISTINCT pe.npc_id from Mythistone.pull_enemies pe
join Mythistone.route_pulls rp on rp.pull_id = pe.pull_id 
join Mythistone.route_data rd on rd.route_key = rp.route_key 
WHERE rd.dungeon_id = %s
"""


def fetch_distinct_npc_ids_for_dungeon(connection, cursor, dungeon_id):
    """
    Fetch all distinct NPC IDs recorded in pull_enemies for a specific dungeon.
    Returns list of int NPC IDs (may be empty).
    """
    rows = fetch_with_retry(
        connection, cursor, FETCH_DISTINCT_NPC_IDS_FOR_DUNGEON_SQL, (dungeon_id,)
    )
    if not rows:
        return []
    return [int(r[0]) for r in rows if r and r[0] is not None]


FETCH_TOP_ROUTES_FOR_SPEC_SQL = """
WITH filtered AS (
  SELECT rd.*
  FROM route_data rd
  JOIN route_specs rs_filter
    ON rd.route_key = rs_filter.route_key
    AND rs_filter.spec_id = %s
  WHERE rd.timestamp >= (UNIX_TIMESTAMP() - 4*7*24*3600)
),
ranked AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY dungeon_id
      ORDER BY keystone_level DESC, duration ASC, timestamp DESC
    ) AS rn
  FROM filtered f
)
SELECT
  r.dungeon_id,
  ANY_VALUE(r.route_key)                   AS route_key,
  ANY_VALUE(r.rio_run_id)                  AS rio_run_id,
  ANY_VALUE(r.mapping_version)             AS mapping_version,
  ANY_VALUE(r.enemy_forces)                AS enemy_forces,
  ANY_VALUE(r.keystone_level)              AS highest_key,
  ANY_VALUE(r.duration)                    AS duration,
  ANY_VALUE(r.timestamp)                   AS timestamp,
  GROUP_CONCAT(rs.spec_id ORDER BY rs.id SEPARATOR ',') AS comps_csv
FROM ranked r
JOIN route_specs rs
  ON rs.route_key = r.route_key
WHERE r.rn = 1
GROUP BY r.dungeon_id;

"""


def fetch_top_routes_for_spec(connection, cursor, spec_id):
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_ROUTES_FOR_SPEC_SQL, (spec_id,)
    )
    routes = {}
    for row in rows:
        routes[row[0]] = {
            "route_key": row[1],
            "run_id": row[2],
            "mapping_version": row[3],
            "enemy_forces": row[4],
            "highest_key": row[5],
            "duration": row[6],
            "timestamp": row[7],
            "specs": row[8].split(","),
        }

    return routes

FETCH_DUNGEON_TOP_SPECS_SQL = """
SELECT spec_id, run_count as total_runs
FROM Mythistone.aggregated_dungeon_specs
WHERE dungeon_id = %s AND season = %s
ORDER BY run_count DESC
LIMIT 5;
"""

def fetch_dungeon_top_specs(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_DUNGEON_TOP_SPECS_SQL,
        (dungeon_id, season)
    )

FETCH_DUNGEON_SPECS_RATIO_SQL = """
SELECT 
    ds.spec_id,
    ds.run_count as local_runs,
    gs.run_count as global_runs,
    ds.max_keystone_level as highest_key,
    ds.timed_runs as timed_runs,
    ds.depleted_runs as depleted_runs
FROM Mythistone.aggregated_dungeon_specs ds
JOIN Mythistone.aggregated_dungeon_global_specs gs 
  ON ds.spec_id = gs.spec_id AND ds.season = gs.season
WHERE ds.dungeon_id = %s AND ds.season = %s
"""

def fetch_dungeon_specs_ratio(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_DUNGEON_SPECS_RATIO_SQL,
        (dungeon_id, season)
    )

FETCH_DUNGEON_TOTALS_SQL = """
SELECT SUM(run_count) as total
FROM Mythistone.aggregated_dungeon_specs
WHERE dungeon_id = %s AND season = %s
"""

def fetch_dungeon_totals(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_DUNGEON_TOTALS_SQL,
        (dungeon_id, season)
    )

FETCH_GLOBAL_TOTALS_SQL = """
SELECT SUM(run_count) as total
FROM Mythistone.aggregated_dungeon_global_specs
WHERE season = %s
"""

def fetch_global_totals(connection, cursor, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_GLOBAL_TOTALS_SQL,
        (season,)
    )

FETCH_GLOBAL_TOP_COMPS_SQL = """
SELECT comp, SUM(timed_runs + depleted_runs) as comp_count
FROM Mythistone.aggregated_dungeon_comps
WHERE season = %s
GROUP BY comp
ORDER BY comp_count DESC
LIMIT 5
"""

def fetch_global_top_comps(connection, cursor, season: int):
    cursor.execute(
        FETCH_GLOBAL_TOP_COMPS_SQL,
        (season,),
    )
    return cursor.fetchall()

FETCH_SPEC_TOP_COMPS_SQL = """
SELECT 
    comp, 
    SUM(timed_runs + depleted_runs) as comp_count,
    MAX(keystone_level) as highest_key,
    ROUND((SUM(timed_runs) / SUM(timed_runs + depleted_runs)) * 100) as win_rate
FROM Mythistone.aggregated_dungeon_comps
WHERE season = %s AND FIND_IN_SET(%s, comp) > 0
GROUP BY comp
ORDER BY comp_count DESC
LIMIT 5
"""

def fetch_spec_top_comps(connection, cursor, spec_id: str, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_SPEC_TOP_COMPS_SQL,
        (season, str(spec_id))
    )

FETCH_DUNGEON_TOP_COMPS_SQL = """
SELECT 
    comp, 
    SUM(timed_runs + depleted_runs) as comp_count,
    MAX(keystone_level) as highest_key,
    ROUND((SUM(timed_runs) / SUM(timed_runs + depleted_runs)) * 100) as win_rate
FROM Mythistone.aggregated_dungeon_comps
WHERE dungeon_id = %s AND season = %s
GROUP BY comp
ORDER BY comp_count DESC
LIMIT 5;
"""

def fetch_dungeon_top_comps(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_DUNGEON_TOP_COMPS_SQL,
        (dungeon_id, season)
    )

FETCH_ALL_COMPS_SQL = """
SELECT dungeon_id, keystone_level, comp, timed_runs, depleted_runs
FROM Mythistone.aggregated_dungeon_comps
WHERE season = %s
"""

def fetch_all_comps(connection, cursor, season: int):
    return fetch_with_retry(
        connection,
        cursor,
        FETCH_ALL_COMPS_SQL,
        (season,)
    )


def fetch_spec_top_comps_all(connection, cursor, season: int):
    """Season-wide replacement for calling fetch_spec_top_comps once per spec:
    the per-spec query's FIND_IN_SET filter can't use an index, so 40 calls
    mean 40 full scans of aggregated_dungeon_comps. One scan + Python
    aggregation yields every spec's top 5 at once.

    Returns {spec_id (str): [(comp, comp_count, highest_key, win_rate), ...]}
    with rows shaped exactly like FETCH_SPEC_TOP_COMPS_SQL's output.
    """
    rows = fetch_all_comps(connection, cursor, season)
    per_comp = {}  # comp -> [timed, total, highest_key]
    for row in rows:
        if isinstance(row, dict):
            comp = row["comp"]
            level = int(row["keystone_level"])
            timed = int(row["timed_runs"] or 0)
            depleted = int(row["depleted_runs"] or 0)
        else:
            comp = row[2]
            level = int(row[1])
            timed = int(row[3] or 0)
            depleted = int(row[4] or 0)
        agg = per_comp.get(comp)
        if agg is None:
            per_comp[comp] = [timed, timed + depleted, level]
        else:
            agg[0] += timed
            agg[1] += timed + depleted
            agg[2] = max(agg[2], level)

    by_spec = {}
    for comp, (timed, total, highest_key) in per_comp.items():
        # matches SQL ROUND() (half away from zero) rather than Python's
        # banker's rounding
        win_rate = int(timed / total * 100 + 0.5) if total else 0
        entry = (comp, total, highest_key, win_rate)
        for spec in comp.split(","):
            by_spec.setdefault(spec.strip(), []).append(entry)

    for spec, comps in by_spec.items():
        comps.sort(key=lambda c: c[1], reverse=True)
        del comps[5:]
    return by_spec

FETCH_DUNGEON_TOP_ROUTES_SQL = """
WITH PullEnemies AS (
    SELECT 
        route_key, 
        pull_id, 
        GROUP_CONCAT(CONCAT(npc_id, ':', count) ORDER BY npc_id ASC SEPARATOR ',') AS enemies
    FROM Mythistone.pull_enemies
    GROUP BY route_key, pull_id
),
PullSpells AS (
    SELECT 
        route_key, 
        pull_id, 
        GROUP_CONCAT(spell_id ORDER BY spell_id ASC SEPARATOR ',') AS spells
    FROM Mythistone.pull_spells
    GROUP BY route_key, pull_id
),
RouteSignatures AS (
    SELECT 
        rp.route_key,
        GROUP_CONCAT(
            CONCAT('{E:', COALESCE(pe.enemies, ''), '}{S:', COALESCE(ps.spells, ''), '}') 
            ORDER BY rp.pull_id ASC 
            SEPARATOR ' | '
        ) AS route_signature
    FROM Mythistone.route_pulls rp
    LEFT JOIN PullEnemies pe ON rp.route_key = pe.route_key AND rp.pull_id = pe.pull_id
    LEFT JOIN PullSpells ps ON rp.route_key = ps.route_key AND rp.pull_id = ps.pull_id
    GROUP BY rp.route_key
),
RankedRoutes AS (
    SELECT 
        rs.route_signature,
        rs.route_key,
        rd.rio_run_id as run_id,
        rd.enemy_forces,
        rd.timestamp,
        rd.keystone_level,
        rd.duration,
        rd.dungeon_id,
        COUNT(rs.route_key) OVER (PARTITION BY rs.route_signature) as usage_count,
        ROW_NUMBER() OVER (PARTITION BY rs.route_signature ORDER BY rd.keystone_level DESC, rd.duration ASC) as rn
    FROM RouteSignatures rs
    JOIN Mythistone.route_data rd ON rs.route_key = rd.route_key
    WHERE rd.dungeon_id = %s
)
SELECT 
    route_key, 
    enemy_forces, 
    keystone_level, 
    duration, 
    timestamp, 
    run_id,
    usage_count
FROM RankedRoutes
WHERE rn = 1
ORDER BY usage_count DESC, keystone_level DESC
LIMIT 5;
"""

FETCH_ROUTE_SPECS_SQL = """
SELECT spec_id FROM Mythistone.route_specs WHERE route_key = %s;
"""

def fetch_dungeon_top_routes(connection, cursor, dungeon_id: str):
    routes_rows = fetch_with_retry(
        connection,
        cursor,
        FETCH_DUNGEON_TOP_ROUTES_SQL,
        (dungeon_id,)
    )
    if not routes_rows:
        return []

    # one IN() round trip for all routes' specs instead of one query per route
    top_routes = [dict(r) for r in routes_rows]
    route_keys = [r['route_key'] for r in top_routes]
    placeholders = ", ".join(["%s"] * len(route_keys))
    specs_rows = fetch_with_retry(
        connection,
        cursor,
        f"SELECT route_key, spec_id FROM Mythistone.route_specs WHERE route_key IN ({placeholders});",
        tuple(route_keys),
    )
    specs_by_route = {}
    for s in specs_rows or []:
        key, spec = (s['route_key'], s['spec_id']) if isinstance(s, dict) else (s[0], s[1])
        specs_by_route.setdefault(key, []).append(spec)
    for r_dict in top_routes:
        r_dict['specs'] = specs_by_route.get(r_dict['route_key'], [])

    return top_routes

FETCH_DUNGEON_SHORTEST_KEY_RUN_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.timestamp, r.faction, r.run_id, r.region, r.season, rm.member, m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT run_id FROM runs WHERE dungeon_id = %s AND season = %s AND duration > 0 ORDER BY duration ASC, run_id ASC LIMIT 1
)
ORDER BY rm.member;
"""

def fetch_dungeon_shortest_run(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_SHORTEST_KEY_RUN_SQL, (dungeon_id, season))

FETCH_DUNGEON_LONGEST_KEY_RUN_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.timestamp, r.faction, r.run_id, r.region, r.season, rm.member, m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT run_id FROM runs WHERE dungeon_id = %s AND season = %s ORDER BY duration DESC, run_id ASC LIMIT 1
)
ORDER BY rm.member;
"""

def fetch_dungeon_longest_run(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_LONGEST_KEY_RUN_SQL, (dungeon_id, season))

FETCH_DUNGEON_MAX_KEY_RUN_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.timestamp, r.faction, r.run_id, r.region, r.season, rm.member, m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT run_id FROM runs WHERE dungeon_id = %s AND season = %s ORDER BY keystone_level DESC, duration ASC, run_id ASC LIMIT 1
)
ORDER BY rm.member;
"""

def fetch_dungeon_max_key_run(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_MAX_KEY_RUN_SQL, (dungeon_id, season))

FETCH_DUNGEON_CLOSEST_CALL_RUN_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.timestamp, r.faction, r.run_id, r.region, r.season, rm.member, m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT r2.run_id
    FROM runs r2
    JOIN dungeon_data dd ON dd.dungeon_id = r2.dungeon_id
    WHERE r2.dungeon_id = %s AND r2.season = %s AND r2.duration > 0
      AND dd.upgrade_1_duration IS NOT NULL
      AND r2.duration <= dd.upgrade_1_duration
    ORDER BY (dd.upgrade_1_duration - r2.duration) ASC, r2.run_id ASC
    LIMIT 1
)
ORDER BY rm.member;
"""

def fetch_dungeon_closest_call_run(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_CLOSEST_CALL_RUN_SQL, (dungeon_id, season))

FETCH_DUNGEON_FASTEST_TOP_LEVELS_RUN_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.timestamp, r.faction, r.run_id, r.region, r.season, rm.member, m.spec_id
FROM runs r
LEFT JOIN run_members rm ON rm.run_id = r.run_id
LEFT JOIN members m       ON m.member = rm.member
WHERE r.run_id = (
    SELECT r2.run_id
    FROM runs r2
    WHERE r2.dungeon_id = %s AND r2.season = %s AND r2.duration > 0
      AND r2.keystone_level >= (
          SELECT MIN(t.kl) FROM (
              SELECT DISTINCT keystone_level AS kl
              FROM runs
              WHERE dungeon_id = %s AND season = %s
              ORDER BY kl DESC
              LIMIT 3
          ) AS t
      )
    ORDER BY r2.duration ASC, r2.run_id ASC
    LIMIT 1
)
ORDER BY rm.member;
"""

def fetch_dungeon_fastest_top_levels_run(connection, cursor, dungeon_id: str, season: int):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_FASTEST_TOP_LEVELS_RUN_SQL, (dungeon_id, season, dungeon_id, season))

FETCH_DUNGEON_LUST_TIMELINE_SQL = """
WITH PullSigs AS (
    SELECT 
        rp.route_key,
        rp.pull_id,
        GROUP_CONCAT(CONCAT(pe.npc_id, ':', pe.count) ORDER BY pe.npc_id ASC SEPARATOR ',') as pull_sig,
        CASE WHEN MAX(ps.spell_id) IS NOT NULL THEN 1 ELSE 0 END as lusted,
        MAX(rd.keystone_level) as keystone_level
    FROM route_data rd
    JOIN route_pulls rp ON rd.route_key = rp.route_key
    JOIN pull_enemies pe ON rp.pull_id = pe.pull_id AND rp.route_key = pe.route_key
    LEFT JOIN pull_spells ps ON rp.pull_id = ps.pull_id AND rp.route_key = ps.route_key 
        AND ps.spell_id IN (SELECT spell_id FROM bloodlust_spells)
    WHERE rd.dungeon_id = %s
    AND EXISTS (
        SELECT 1 FROM pull_spells ps_lust 
        WHERE ps_lust.route_key = rd.route_key 
        AND ps_lust.spell_id IN (SELECT spell_id FROM bloodlust_spells)
    )
    GROUP BY rp.route_key, rp.pull_id
)
SELECT 
    pull_sig as top_npcs,
    COUNT(*) as total_pulls_at_index,
    SUM(lusted) as lust_count,
    (SUM(lusted) / COUNT(*)) * 100 AS lust_percentage,
    MAX(CASE WHEN lusted = 1 THEN keystone_level ELSE NULL END) AS max_key_lusted,
    MAX(CASE WHEN lusted = 0 THEN keystone_level ELSE NULL END) AS max_key_not_lusted
FROM PullSigs
GROUP BY pull_sig
HAVING SUM(lusted) > 0
ORDER BY lust_count DESC
LIMIT 20
"""

def fetch_dungeon_lust_timeline(connection, cursor, dungeon_id: str):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_LUST_TIMELINE_SQL, (dungeon_id,))

FETCH_BLOODLUST_SPELL_IDS_SQL = """
SELECT spell_id FROM bloodlust_spells ORDER BY spell_id;
"""

def fetch_bloodlust_spell_ids(connection, cursor):
    """The spell ids the lust queries above filter on, for the dungeon page's
    keystone.guru heatmap link (includePlayerSpellIds)."""
    rows = fetch_with_retry(connection, cursor, FETCH_BLOODLUST_SPELL_IDS_SQL)
    return [int(row["spell_id"] if isinstance(row, dict) else row[0]) for row in rows]

FETCH_DUNGEON_SKIP_RATES_SQL = """
SELECT 
    ansr.npc_id,
    ansr.total_encounters,
    ansr.total_routes,
    (ansr.total_encounters / ansr.total_routes) * 100 AS inclusion_percentage,
    (SELECT MAX(rd.keystone_level) FROM route_data rd JOIN pull_enemies pe ON rd.route_key = pe.route_key WHERE rd.dungeon_id = ansr.dungeon_id AND pe.npc_id = ansr.npc_id) as max_key_played,
    (SELECT MAX(rd.keystone_level) FROM route_data rd WHERE rd.dungeon_id = ansr.dungeon_id AND NOT EXISTS (SELECT 1 FROM pull_enemies pe WHERE pe.route_key = rd.route_key AND pe.npc_id = ansr.npc_id)) as max_key_skipped
FROM aggregated_npc_skip_rates ansr
WHERE ansr.dungeon_id = %s AND ansr.total_routes > 0 AND ansr.total_encounters < ansr.total_routes
ORDER BY inclusion_percentage ASC
LIMIT 50
"""

def fetch_dungeon_skip_rates(connection, cursor, dungeon_id: str, season: int = None):
    return fetch_with_retry(connection, cursor, FETCH_DUNGEON_SKIP_RATES_SQL, (dungeon_id,))

FETCH_EXAMPLE_SKIP_ROUTE_SQL = """
SELECT rd.rio_run_id, rd.route_key, rd.keystone_level
FROM route_data rd
WHERE rd.dungeon_id = %s
  AND rd.route_key NOT IN (
      SELECT route_key FROM pull_enemies WHERE npc_id = %s
  )
ORDER BY rd.keystone_level DESC, rd.timestamp DESC
LIMIT 1
"""

def fetch_example_skip_route(connection, cursor, dungeon_id: str, npc_id: int):
    return fetch_with_retry(connection, cursor, FETCH_EXAMPLE_SKIP_ROUTE_SQL, (dungeon_id, npc_id))


# same statement as FETCH_EXAMPLE_SKIP_ROUTE_SQL plus an npc marker column, so
# one UNION ALL round trip can answer for every skipped NPC at once
FETCH_EXAMPLE_SKIP_ROUTE_ARM_SQL = """(
SELECT %s AS skip_npc_id, rd.rio_run_id, rd.route_key, rd.keystone_level
FROM route_data rd
WHERE rd.dungeon_id = %s
  AND rd.route_key NOT IN (
      SELECT route_key FROM pull_enemies WHERE npc_id = %s
  )
ORDER BY rd.keystone_level DESC, rd.timestamp DESC
LIMIT 1
)"""


def fetch_example_skip_routes(connection, cursor, dungeon_id: str, npc_ids):
    """Batched fetch_example_skip_route: one round trip for all NPCs.
    Returns {npc_id: row} with rows shaped like the single-NPC query's."""
    npc_ids = list(npc_ids)
    if not npc_ids:
        return {}
    sql = "\nUNION ALL\n".join([FETCH_EXAMPLE_SKIP_ROUTE_ARM_SQL] * len(npc_ids))
    params = []
    for npc_id in npc_ids:
        params.extend((npc_id, dungeon_id, npc_id))
    rows = fetch_with_retry(connection, cursor, sql, tuple(params))
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            out[row["skip_npc_id"]] = {
                k: v for k, v in row.items() if k != "skip_npc_id"
            }
        else:
            out[row[0]] = row[1:]
    return out


FETCH_EXAMPLE_LUST_ROUTE_SQL = """
WITH target_pull AS (
    SELECT 
        rp.route_key,
        rp.pull_id,
        rd.keystone_level
    FROM route_data rd
    JOIN route_pulls rp ON rd.route_key = rp.route_key
    JOIN pull_enemies pe ON rp.pull_id = pe.pull_id AND rp.route_key = pe.route_key
    JOIN pull_spells ps ON rp.pull_id = ps.pull_id AND rp.route_key = ps.route_key 
        AND ps.spell_id IN (SELECT spell_id FROM bloodlust_spells)
    WHERE rd.dungeon_id = %s
    GROUP BY rp.route_key, rp.pull_id, rd.keystone_level
    HAVING GROUP_CONCAT(CONCAT(pe.npc_id, ':', pe.count) ORDER BY pe.npc_id ASC SEPARATOR ',') = %s
    ORDER BY rd.keystone_level DESC
    LIMIT 1
)
SELECT
    rd.rio_run_id, 
    rd.route_key, 
    rd.keystone_level,
    (SELECT COUNT(*) FROM route_pulls rp2 WHERE rp2.route_key = tp.route_key AND rp2.pull_id <= tp.pull_id) as pull_number
FROM target_pull tp
JOIN route_data rd ON rd.route_key = tp.route_key;
"""

def fetch_example_lust_route(connection, cursor, dungeon_id: str, pull_sig: str):
    return fetch_with_retry(connection, cursor, FETCH_EXAMPLE_LUST_ROUTE_SQL, (dungeon_id, pull_sig))


# FETCH_EXAMPLE_LUST_ROUTE_SQL with the CTE inlined as a derived table (WITH
# isn't allowed inside a parenthesized UNION ALL arm) plus a signature marker
# column, so one round trip can answer for every lust pull at once
FETCH_EXAMPLE_LUST_ROUTE_ARM_SQL = """(
SELECT
    %s AS lust_sig,
    rd.rio_run_id,
    rd.route_key,
    rd.keystone_level,
    (SELECT COUNT(*) FROM route_pulls rp2 WHERE rp2.route_key = tp.route_key AND rp2.pull_id <= tp.pull_id) as pull_number
FROM (
    SELECT
        rp.route_key,
        rp.pull_id,
        rd.keystone_level
    FROM route_data rd
    JOIN route_pulls rp ON rd.route_key = rp.route_key
    JOIN pull_enemies pe ON rp.pull_id = pe.pull_id AND rp.route_key = pe.route_key
    JOIN pull_spells ps ON rp.pull_id = ps.pull_id AND rp.route_key = ps.route_key
        AND ps.spell_id IN (SELECT spell_id FROM bloodlust_spells)
    WHERE rd.dungeon_id = %s
    GROUP BY rp.route_key, rp.pull_id, rd.keystone_level
    HAVING GROUP_CONCAT(CONCAT(pe.npc_id, ':', pe.count) ORDER BY pe.npc_id ASC SEPARATOR ',') = %s
    ORDER BY rd.keystone_level DESC
    LIMIT 1
) tp
JOIN route_data rd ON rd.route_key = tp.route_key
)"""


def fetch_example_lust_routes(connection, cursor, dungeon_id: str, pull_sigs):
    """Batched fetch_example_lust_route: one round trip for all pull
    signatures. Returns {pull_sig: row} with rows shaped like the
    single-signature query's."""
    pull_sigs = list(pull_sigs)
    if not pull_sigs:
        return {}
    sql = "\nUNION ALL\n".join([FETCH_EXAMPLE_LUST_ROUTE_ARM_SQL] * len(pull_sigs))
    params = []
    for sig in pull_sigs:
        params.extend((sig, dungeon_id, sig))
    rows = fetch_with_retry(connection, cursor, sql, tuple(params))
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            out[row["lust_sig"]] = {k: v for k, v in row.items() if k != "lust_sig"}
        else:
            out[row[0]] = row[1:]
    return out



# -- Top player verified loadouts: SQL + helpers ---------------------------------

DELETE_TOP_PLAYER_META_SQL = """
DELETE FROM `Mythistone`.`top_player_loadouts`
WHERE `spec_id` = %s AND `rank` = %s AND `map_challenge_mode_id` = %s
"""

INSERT_TOP_PLAYER_META_SQL = """
INSERT INTO `Mythistone`.`top_player_loadouts`
(`spec_id`, `season`, `rank`, `map_challenge_mode_id`, `region`, `character_id`, `character_name`, `realm`, `loadout_key`, `loadout_updated_at`, `keystone_level`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_TOP_PLAYER_ITEMS_SQL = """
INSERT INTO `Mythistone`.`top_player_loadout_items`
(`spec_id`, `season`, `rank`, `map_challenge_mode_id`, `slot`, `item_id`, `item_level`, `bonus_ids`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_TOP_PLAYER_GEMS_SQL = """
INSERT INTO `Mythistone`.`top_player_loadout_gems`
(`spec_id`, `season`, `rank`, `map_challenge_mode_id`, `gem_item_id`, `usage_count`)
VALUES (%s, %s, %s, %s, %s, %s)
"""

INSERT_TOP_PLAYER_ENCHANTS_SQL = """
INSERT INTO `Mythistone`.`top_player_loadout_enchants`
(`spec_id`, `season`, `rank`, `map_challenge_mode_id`, `slot_group`, `enchantment_id`)
VALUES (%s, %s, %s, %s, %s, %s)
"""

INSERT_TOP_PLAYER_TALENTS_SQL = """
INSERT INTO `Mythistone`.`top_player_loadout_talents`
(`spec_id`, `season`, `rank`, `map_challenge_mode_id`, `node_id`, `node_rank`, `entry_id`, `spell_id`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

FETCH_TOP_PLAYER_META_SQL = """
SELECT `spec_id`, `season`, `rank`, `map_challenge_mode_id`, `region`, `character_id`, `character_name`, `realm`, `loadout_key`, `loadout_updated_at`, `keystone_level`
FROM `Mythistone`.`top_player_loadouts`
WHERE `spec_id` = %s AND `rank` = %s AND `map_challenge_mode_id` = %s
ORDER BY `season` DESC
LIMIT 1
"""


def delete_top_player_meta(connection, cursor, spec_id, rank, map_challenge_mode_id):
    """Delete the top-player meta row (cascades to child tables).

    Note: `season` was removed from the primary key; this function deletes by
    the new unique key (spec_id, rank, map_challenge_mode_id)."""
    params = (spec_id, rank, map_challenge_mode_id)
    # Debugging: print SQL + params to help diagnose syntax errors
    try:
        print(f"DEBUG delete_top_player_meta executing SQL: {DELETE_TOP_PLAYER_META_SQL.strip()} params={params!r}")
        execute_with_retry(connection, cursor, DELETE_TOP_PLAYER_META_SQL, params)
        return cursor.rowcount
    except Exception as err:
        # Print detailed debug info and re-raise
        try:
            stmt = getattr(cursor, "statement", None)
        except Exception:
            stmt = None
        print("ERROR in delete_top_player_meta:")
        print("SQL:", DELETE_TOP_PLAYER_META_SQL)
        print("params:", params)
        if stmt:
            print("cursor.statement:", stmt)
        raise


def insert_top_player_meta(
    connection,
    cursor,
    spec_id,
    season,
    rank,
    map_challenge_mode_id,
    region=None,
    character_id=None,
    character_name=None,
    realm=None,
    loadout_key=None,
    loadout_updated_at=None,
    keystone_level=None,
):
    """Insert a top-player meta row."""
    val = (
        spec_id,
        season,
        rank,
        map_challenge_mode_id,
        region,
        character_id,
        character_name,
        realm,
        loadout_key,
        loadout_updated_at,
        keystone_level,
    )
    execute_with_retry(connection, cursor, INSERT_TOP_PLAYER_META_SQL, val)
    return cursor.lastrowid


def insert_top_player_items_batch(connection, cursor, rows):
    """
    Bulk insert item rows for a top-player loadout.
    Each row should be a tuple matching the INSERT_TOP_PLAYER_ITEMS_SQL params.
    """
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_TOP_PLAYER_ITEMS_SQL, rows)
    return cursor.lastrowid


def insert_top_player_gems_batch(connection, cursor, rows):
    """Bulk insert gem/socket rows for a top-player loadout."""
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_TOP_PLAYER_GEMS_SQL, rows)
    return cursor.lastrowid


def insert_top_player_enchants_batch(connection, cursor, rows):
    """Bulk insert enchantment rows for a top-player loadout."""
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_TOP_PLAYER_ENCHANTS_SQL, rows)
    return cursor.lastrowid


def insert_top_player_talents_batch(connection, cursor, rows):
    """Bulk insert talent node rows for a top-player loadout."""
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_TOP_PLAYER_TALENTS_SQL, rows)
    return cursor.lastrowid


def fetch_top_player_meta(connection, cursor, spec_id, rank, map_challenge_mode_id):
    """Fetch a single top-player meta row as a dict, or None if not found.

    Since `season` is no longer part of the unique key, this returns the
    most-recent (`season` DESC) row for the given (spec_id, rank, map_challenge_mode_id).
    """
    params = (spec_id, rank, map_challenge_mode_id)
    rows = fetch_with_retry(connection, cursor, FETCH_TOP_PLAYER_META_SQL, params)
    if not rows:
        return None
    row = rows[0]
    # row may be tuple or dict depending on cursor type
    if isinstance(row, dict):
        return {
            "spec_id": int(row.get("spec_id")),
            "season": int(row.get("season")),
            "rank": int(row.get("rank")),
            "map_challenge_mode_id": int(row.get("map_challenge_mode_id")) if row.get("map_challenge_mode_id") else None,
            "region": row.get("region"),
            "character_id": int(row.get("character_id")) if row.get("character_id") else None,
            "character_name": row.get("character_name"),
            "realm": row.get("realm"),
            "loadout_key": row.get("loadout_key"),
            "loadout_updated_at": row.get("loadout_updated_at"),
            "keystone_level": int(row.get("keystone_level")) if row.get("keystone_level") else None,
        }
    else:
        return {
            "spec_id": int(row[0]),
            "season": int(row[1]),
            "rank": int(row[2]),
            "map_challenge_mode_id": int(row[3]) if row[3] is not None else None,
            "region": row[4],
            "character_id": int(row[5]) if row[5] is not None else None,
            "character_name": row[6],
            "realm": row[7],
            "loadout_key": row[8],
            "loadout_updated_at": row[9],
            "keystone_level": int(row[10]) if row[10] is not None else None,
        }


# Fetch the top-player loadouts of the top N ranked players (meta + child rows)
def fetch_top50_loadouts(connection, cursor, spec_id, season, limit=50):
    """Return the verified loadouts of the top `limit` players for spec/season.

    `limit` bounds ranked PLAYERS, not rows: the collector stores one verified
    loadout per dungeon per ranked player, so the result holds up to
    `limit` * (dungeons in the season) entries. Bounding by rows instead would
    only reach the first handful of players and leave every per-dungeon
    statistic derived from this data on a sample of a few loadouts.

    Each returned entry is a dict with keys:
      - meta: dict (spec_id, season, rank, map_challenge_mode_id, region, character_id, character_name, realm, loadout_key, loadout_updated_at, keystone_level)
      - items: list of { slot, item_id, item_level, bonus_ids }
      - gems: list of { gem_item_id, usage_count }
      - enchants: list of { slot_group, enchantment_id }
      - talents: list of { node_id, node_rank, entry_id, spell_id }

    This helper performs a small number of queries (1 meta + 4 child queries).
    """
    FETCH_TOP50_META_SQL = """
    SELECT `spec_id`, `season`, `rank`, `map_challenge_mode_id`, `region`, `character_id`, `character_name`, `realm`, `loadout_key`, `loadout_updated_at`, `keystone_level`
    FROM `Mythistone`.`top_player_loadouts`
    WHERE `spec_id` = %s AND `season` = %s AND `rank` <= %s
    ORDER BY `rank` ASC, `map_challenge_mode_id` ASC
    """

    params = (spec_id, season, limit)
    rows = fetch_with_retry(connection, cursor, FETCH_TOP50_META_SQL, params)
    if not rows:
        return []

    metas = []
    for row in rows:
        if isinstance(row, dict):
            rank = int(row.get("rank"))
            map_id = int(row.get("map_challenge_mode_id")) if row.get("map_challenge_mode_id") is not None else None
            meta = {
                "spec_id": int(row.get("spec_id")),
                "season": int(row.get("season")),
                "rank": rank,
                "map_challenge_mode_id": map_id,
                "region": row.get("region"),
                "character_id": int(row.get("character_id")) if row.get("character_id") else None,
                "character_name": row.get("character_name"),
                "realm": row.get("realm"),
                "loadout_key": row.get("loadout_key"),
                "loadout_updated_at": row.get("loadout_updated_at"),
                "keystone_level": int(row.get("keystone_level")) if row.get("keystone_level") else None,
            }
        else:
            rank = int(row[2])
            map_id = int(row[3]) if row[3] is not None else None
            meta = {
                "spec_id": int(row[0]),
                "season": int(row[1]),
                "rank": rank,
                "map_challenge_mode_id": map_id,
                "region": row[4],
                "character_id": int(row[5]) if row[5] is not None else None,
                "character_name": row[6],
                "realm": row[7],
                "loadout_key": row[8],
                "loadout_updated_at": row[9],
                "keystone_level": int(row[10]) if row[10] is not None else None,
            }
        metas.append(meta)

    # build a mapping key -> meta dict
    meta_map = {f"{m['rank']}|{m['map_challenge_mode_id']}": {**m, "items": [], "gems": [], "enchants": [], "talents": []} for m in metas}

    # Child rows: one query per table over the same rank window as the meta
    # query. `map_challenge_mode_id` is NOT NULL in all four child tables and
    # they are FK-bound to the meta table, so the rank window selects exactly
    # the rows belonging to the metas fetched above.
    def _child_rows(table, columns):
        """Yield (meta_key, {column: value}) for one child table."""
        col_sql = ", ".join(f"`{c}`" for c in columns)
        sql = f"""
        SELECT `rank`, `map_challenge_mode_id`, {col_sql}
        FROM `Mythistone`.`{table}`
        WHERE `spec_id` = %s AND `season` = %s AND `rank` <= %s
        ORDER BY `rank`
        """
        for row in fetch_with_retry(connection, cursor, sql, (spec_id, season, limit)) or []:
            if isinstance(row, dict):
                key = f"{int(row['rank'])}|{int(row['map_challenge_mode_id'])}"
                values = {c: row.get(c) for c in columns}
            else:
                key = f"{int(row[0])}|{int(row[1])}"
                values = dict(zip(columns, row[2:]))
            # rows whose meta was filtered out (e.g. stale season) are dropped
            if key in meta_map:
                yield key, values

    def _int_or_none(value):
        return int(value) if value is not None else None

    # ITEMS
    for key, v in _child_rows(
        "top_player_loadout_items", ("slot", "item_id", "item_level", "bonus_ids")
    ):
        meta_map[key]["items"].append({
            "slot": v["slot"],
            "item_id": int(v["item_id"]),
            "item_level": _int_or_none(v["item_level"]),
            "bonus_ids": v["bonus_ids"],
        })

    # GEMS
    for key, v in _child_rows("top_player_loadout_gems", ("gem_item_id", "usage_count")):
        meta_map[key]["gems"].append({
            "gem_item_id": int(v["gem_item_id"]),
            "usage_count": int(v["usage_count"]),
        })

    # ENCHANTS
    for key, v in _child_rows(
        "top_player_loadout_enchants", ("slot_group", "enchantment_id")
    ):
        meta_map[key]["enchants"].append({
            "slot_group": v["slot_group"],
            "enchantment_id": int(v["enchantment_id"]),
        })

    # TALENTS
    for key, v in _child_rows(
        "top_player_loadout_talents", ("node_id", "node_rank", "entry_id", "spell_id")
    ):
        meta_map[key]["talents"].append({
            "node_id": int(v["node_id"]),
            "node_rank": int(v["node_rank"]),
            "entry_id": _int_or_none(v["entry_id"]),
            "spell_id": _int_or_none(v["spell_id"]),
        })

    # Return ordered list corresponding to metas
    out = []
    for m in metas:
        key = f"{m['rank']}|{m['map_challenge_mode_id']}"
        out.append(meta_map.get(key, m))
    return out

# ---------------------------------------------------------------------------
# SimulationCraft "best item per slot" (simc BiS) helpers
# ---------------------------------------------------------------------------

DELETE_SIMC_BIS_META_SQL = """
DELETE FROM `Mythistone`.`simc_bis_meta`
WHERE `spec_id` = %s AND `season` = %s
"""

INSERT_SIMC_BIS_META_SQL = """
INSERT INTO `Mythistone`.`simc_bis_meta`
(`spec_id`, `season`, `simc_version`, `baseline_dps`, `iterations`, `target_error`, `tier_config`, `updated_at`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_SIMC_BIS_ITEMS_SQL = """
INSERT INTO `Mythistone`.`simc_bis_items`
(`spec_id`, `season`, `slot`, `rank`, `item_id`, `bonus_list`, `ilevel`, `dps`, `dps_pct_gain`, `is_set_piece`, `item_set_id`, `enchant_id`, `gem_ids`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

FETCH_SIMC_BIS_ITEMS_SQL = """
SELECT `slot`, `rank`, `item_id`, `bonus_list`, `ilevel`, `dps`, `dps_pct_gain`, `is_set_piece`, `item_set_id`, `enchant_id`, `gem_ids`
FROM `Mythistone`.`simc_bis_items`
WHERE `spec_id` = %s AND `season` = %s
ORDER BY `slot`, `rank`
"""

FETCH_SIMC_BIS_UPDATED_AT_SQL = """
SELECT `updated_at`
FROM `Mythistone`.`simc_bis_meta`
WHERE `spec_id` = %s AND `season` = %s
"""


def delete_simc_bis(connection, cursor, spec_id, season):
    """Delete the simc BiS meta row for a spec/season (cascades to simc_bis_items)."""
    params = (spec_id, season)
    execute_with_retry(connection, cursor, DELETE_SIMC_BIS_META_SQL, params)
    return cursor.rowcount


def insert_simc_bis_meta(
    connection,
    cursor,
    spec_id,
    season,
    simc_version=None,
    baseline_dps=None,
    iterations=None,
    target_error=None,
    tier_config=None,
    updated_at=None,
):
    """Insert a simc BiS meta row."""
    val = (
        spec_id,
        season,
        simc_version,
        baseline_dps,
        iterations,
        target_error,
        tier_config,
        updated_at,
    )
    execute_with_retry(connection, cursor, INSERT_SIMC_BIS_META_SQL, val)
    return cursor.lastrowid


def insert_simc_bis_items_batch(connection, cursor, rows):
    """Bulk insert simc BiS per-slot ranked item rows.

    Each row must match INSERT_SIMC_BIS_ITEMS_SQL:
    (spec_id, season, slot, rank, item_id, bonus_list, ilevel, dps, dps_pct_gain,
     is_set_piece, item_set_id, enchant_id, gem_ids)
    """
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_SIMC_BIS_ITEMS_SQL, rows)
    return cursor.lastrowid


def fetch_simc_bis_updated_at(connection, cursor, spec_id, season):
    """Return the `updated_at` datetime for a spec/season simc BiS run, or None."""
    rows = fetch_with_retry(
        connection, cursor, FETCH_SIMC_BIS_UPDATED_AT_SQL, (spec_id, season)
    )
    if not rows:
        return None
    row = rows[0]
    return row.get("updated_at") if isinstance(row, dict) else row[0]


def fetch_simc_bis(connection, cursor, spec_id, season):
    """Return simc BiS results as {slot: [ {item_id, bonus_list, ilevel, dps,
    dps_pct_gain, rank, is_set_piece, item_set_id}, ... ]} ordered by rank (1 = BiS)."""
    rows = fetch_with_retry(connection, cursor, FETCH_SIMC_BIS_ITEMS_SQL, (spec_id, season))
    out = {}
    for row in rows:
        if isinstance(row, dict):
            slot = row.get("slot")
            entry = {
                "rank": int(row.get("rank")),
                "item_id": int(row.get("item_id")),
                "bonus_list": row.get("bonus_list"),
                "ilevel": int(row.get("ilevel")) if row.get("ilevel") is not None else None,
                "dps": float(row.get("dps")) if row.get("dps") is not None else None,
                "dps_pct_gain": float(row.get("dps_pct_gain")) if row.get("dps_pct_gain") is not None else None,
                "is_set_piece": bool(row.get("is_set_piece")),
                "item_set_id": int(row.get("item_set_id")) if row.get("item_set_id") is not None else None,
                "enchant_id": int(row.get("enchant_id")) if row.get("enchant_id") is not None else None,
                "gem_ids": row.get("gem_ids"),
            }
        else:
            slot = row[0]
            entry = {
                "rank": int(row[1]),
                "item_id": int(row[2]),
                "bonus_list": row[3],
                "ilevel": int(row[4]) if row[4] is not None else None,
                "dps": float(row[5]) if row[5] is not None else None,
                "dps_pct_gain": float(row[6]) if row[6] is not None else None,
                "is_set_piece": bool(row[7]),
                "item_set_id": int(row[8]) if row[8] is not None else None,
                "enchant_id": int(row[9]) if row[9] is not None else None,
                "gem_ids": row[10],
            }
        out.setdefault(slot, []).append(entry)
    return out


# --------------------------------------------------------------------------
# simc BiS checkpoint / resume (simc_bis_progress[_meta])
#
# A heavy spec's full profileset run outlives one collector lifetime (the
# container restarts ~daily), so simcBis computes it in chunks and resumes from
# these tables. See the table comments in database.sql.
# --------------------------------------------------------------------------

FETCH_SIMC_PROGRESS_META_SQL = """
SELECT `run_signature`, `total_profilesets`, `baseline_dps`, `simc_version`,
       `started_at`, `last_attempt_at`, `failed`, `prep_snapshot`
FROM `Mythistone`.`simc_bis_progress_meta`
WHERE `spec_id` = %s AND `season` = %s
"""

FETCH_SIMC_PROGRESS_ACTIVITY_SQL = """
SELECT `spec_id`, `started_at`, `last_attempt_at`, `failed`
FROM `Mythistone`.`simc_bis_progress_meta`
WHERE `season` = %s
"""

FETCH_SIMC_PROGRESS_MEANS_SQL = """
SELECT `profileset_name`, `mean_dps`
FROM `Mythistone`.`simc_bis_progress`
WHERE `spec_id` = %s AND `season` = %s
"""

UPSERT_SIMC_PROGRESS_META_SQL = """
INSERT INTO `Mythistone`.`simc_bis_progress_meta`
(`spec_id`, `season`, `run_signature`, `total_profilesets`, `baseline_dps`,
 `simc_version`, `started_at`, `last_attempt_at`, `failed`, `prep_snapshot`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  `run_signature` = VALUES(`run_signature`),
  `total_profilesets` = VALUES(`total_profilesets`),
  `baseline_dps` = COALESCE(VALUES(`baseline_dps`), `baseline_dps`),
  `simc_version` = COALESCE(VALUES(`simc_version`), `simc_version`),
  `last_attempt_at` = VALUES(`last_attempt_at`),
  `failed` = VALUES(`failed`),
  `prep_snapshot` = COALESCE(VALUES(`prep_snapshot`), `prep_snapshot`)
"""

INSERT_SIMC_PROGRESS_ROW_SQL = """
INSERT INTO `Mythistone`.`simc_bis_progress`
(`spec_id`, `season`, `profileset_name`, `mean_dps`, `updated_at`)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  `mean_dps` = VALUES(`mean_dps`),
  `updated_at` = VALUES(`updated_at`)
"""

DELETE_SIMC_PROGRESS_META_SQL = """
DELETE FROM `Mythistone`.`simc_bis_progress_meta`
WHERE `spec_id` = %s AND `season` = %s
"""


def fetch_simc_progress_meta(connection, cursor, spec_id, season):
    """Return the in-progress run header for a spec/season, or None if idle.

    Keys: run_signature, total_profilesets, baseline_dps, simc_version,
    started_at, last_attempt_at, failed, prep_snapshot."""
    rows = fetch_with_retry(
        connection, cursor, FETCH_SIMC_PROGRESS_META_SQL, (spec_id, season)
    )
    if not rows:
        return None
    row = rows[0]
    if isinstance(row, dict):
        return dict(row)
    return {
        "run_signature": row[0],
        "total_profilesets": row[1],
        "baseline_dps": row[2],
        "simc_version": row[3],
        "started_at": row[4],
        "last_attempt_at": row[5],
        "failed": row[6],
        "prep_snapshot": row[7],
    }


def fetch_simc_progress_activity(connection, cursor, season):
    """Return {spec_id: {started_at, last_attempt_at, failed}} for every
    in-progress run this season.

    Drives the collector's spec selection: an unfailed in-progress run keeps its
    (old) started_at as its queue position so it is resumed promptly after a
    restart; a failed one is ordered by last_attempt_at so it rotates to the
    back of the queue instead of monopolising the loop."""
    rows = fetch_with_retry(connection, cursor, FETCH_SIMC_PROGRESS_ACTIVITY_SQL, (season,))
    out = {}
    for row in rows:
        if isinstance(row, dict):
            out[int(row.get("spec_id"))] = {
                "started_at": row.get("started_at"),
                "last_attempt_at": row.get("last_attempt_at"),
                "failed": bool(row.get("failed")),
            }
        else:
            out[int(row[0])] = {
                "started_at": row[1],
                "last_attempt_at": row[2],
                "failed": bool(row[3]),
            }
    return out


def fetch_simc_progress_means(connection, cursor, spec_id, season):
    """Return {profileset_name: mean_dps} for all chunks already computed."""
    rows = fetch_with_retry(
        connection, cursor, FETCH_SIMC_PROGRESS_MEANS_SQL, (spec_id, season)
    )
    out = {}
    for row in rows:
        if isinstance(row, dict):
            out[row.get("profileset_name")] = float(row.get("mean_dps"))
        else:
            out[row[0]] = float(row[1])
    return out


def upsert_simc_progress_meta(connection, cursor, spec_id, season, run_signature,
                              total_profilesets, baseline_dps, simc_version,
                              started_at, last_attempt_at, failed=False,
                              prep_snapshot=None):
    """Insert/refresh the in-progress run header. baseline_dps / simc_version /
    prep_snapshot are only overwritten when a non-NULL value is supplied
    (COALESCE), and started_at is never updated on duplicate (it anchors the
    run's queue position), so a failed chunk attempt can flip
    last_attempt_at/failed without clobbering the rest."""
    val = (spec_id, season, run_signature, total_profilesets, baseline_dps,
           simc_version, started_at, last_attempt_at, 1 if failed else 0,
           prep_snapshot)
    execute_with_retry(connection, cursor, UPSERT_SIMC_PROGRESS_META_SQL, val)


def insert_simc_progress_rows(connection, cursor, rows):
    """Upsert computed profileset rows. Each row:
    (spec_id, season, profileset_name, mean_dps, updated_at)."""
    if not rows:
        return 0
    executemany_with_retry(connection, cursor, INSERT_SIMC_PROGRESS_ROW_SQL, rows)
    return cursor.rowcount


def delete_simc_progress(connection, cursor, spec_id, season):
    """Drop all checkpoint state for a spec/season (progress rows cascade from
    the meta delete)."""
    execute_with_retry(connection, cursor, DELETE_SIMC_PROGRESS_META_SQL, (spec_id, season))


FETCH_TOP50_ENCHANT_RANKING_SQL = """
SELECT `slot_group`, `enchantment_id`, COUNT(*) AS cnt
FROM `Mythistone`.`top_player_loadout_enchants`
WHERE `spec_id` = %s AND `season` = %s
GROUP BY `slot_group`, `enchantment_id`
ORDER BY `slot_group`, cnt DESC, `enchantment_id`
"""


def fetch_top50_enchant_ranking(connection, cursor, spec_id, season):
    """Enchant popularity among the top-50 player loadouts.

    Returns {slot_group: [(enchantment_id, count), ...]} most-popular-first.
    """
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP50_ENCHANT_RANKING_SQL, (spec_id, season)
    )
    out = {}
    for row in rows:
        if isinstance(row, dict):
            sg, eid, cnt = row.get("slot_group"), row.get("enchantment_id"), row.get("cnt")
        else:
            sg, eid, cnt = row[0], row[1], row[2]
        out.setdefault(sg, []).append((int(eid), int(cnt)))
    return out


FETCH_TOP50_GEM_RANKING_SQL = """
SELECT `gem_item_id`, SUM(`usage_count`) AS cnt
FROM `Mythistone`.`top_player_loadout_gems`
WHERE `spec_id` = %s AND `season` = %s
GROUP BY `gem_item_id`
ORDER BY cnt DESC, `gem_item_id`
"""


def fetch_top50_gem_ranking(connection, cursor, spec_id, season):
    """Gem popularity among the top-50 player loadouts (spec-wide, not per item).

    Returns [(gem_item_id, count), ...] most-popular-first.
    """
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP50_GEM_RANKING_SQL, (spec_id, season)
    )
    out = []
    for row in rows:
        if isinstance(row, dict):
            gid, cnt = row.get("gem_item_id"), row.get("cnt")
        else:
            gid, cnt = row[0], row[1]
        out.append((int(gid), int(cnt)))
    return out


FETCH_TOP_GEMS_SPEC_WIDE_SQL = """
SELECT `socket_item_id`, SUM(`run_count`) AS cnt
FROM `Mythistone`.`global_aggregated_item_sockets`
WHERE `spec_id` = %s AND `season` = %s
GROUP BY `socket_item_id`
ORDER BY cnt DESC, `socket_item_id`
"""


def fetch_top_gems_spec_wide(connection, cursor, spec_id, season):
    """Spec-wide gem popularity from the global socket aggregation (fallback for
    fetch_top50_gem_ranking when a spec has no top-50 loadout data yet).

    Returns [(gem_item_id, count), ...] most-popular-first. socket_item_id is
    stored as a varchar; non-numeric values are skipped.
    """
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_GEMS_SPEC_WIDE_SQL, (spec_id, season)
    )
    out = []
    for row in rows:
        if isinstance(row, dict):
            gid, cnt = row.get("socket_item_id"), row.get("cnt")
        else:
            gid, cnt = row[0], row[1]
        try:
            out.append((int(gid), int(cnt)))
        except (TypeError, ValueError):
            continue
    return out


FETCH_SIMC_BIS_OVERVIEW_SQL = """
SELECT `spec_id`, `baseline_dps`, `simc_version`, `tier_config`, `iterations`, `target_error`, `updated_at`
FROM `Mythistone`.`simc_bis_meta`
WHERE `season` = %s AND `baseline_dps` IS NOT NULL
"""


def fetch_simc_bis_overview(connection, cursor, season):
    """All specs' converged BiS-set DPS for a season (tierlist page input).

    Failed runs write meta rows without baseline_dps; those are filtered out.
    Returns [{spec_id, baseline_dps, simc_version, tier_config, iterations,
    target_error, updated_at}, ...].
    """
    rows = fetch_with_retry(connection, cursor, FETCH_SIMC_BIS_OVERVIEW_SQL, (season,))
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(
                {
                    "spec_id": int(row.get("spec_id")),
                    "baseline_dps": float(row.get("baseline_dps")),
                    "simc_version": row.get("simc_version"),
                    "tier_config": row.get("tier_config"),
                    "iterations": int(row.get("iterations")) if row.get("iterations") is not None else None,
                    "target_error": float(row.get("target_error")) if row.get("target_error") is not None else None,
                    "updated_at": row.get("updated_at"),
                }
            )
        else:
            out.append(
                {
                    "spec_id": int(row[0]),
                    "baseline_dps": float(row[1]),
                    "simc_version": row[2],
                    "tier_config": row[3],
                    "iterations": int(row[4]) if row[4] is not None else None,
                    "target_error": float(row[5]) if row[5] is not None else None,
                    "updated_at": row[6],
                }
            )
    return out


# ---------------------------------------------------------------------------
# Season-rollover wipe handshake (see database.sql `wipe_control` / ev_season_wipe)
# ---------------------------------------------------------------------------
def read_wipe_control():
    """Return the single-row wipe_control state as a dict, or None if the table
    does not exist yet (older DBs without the rollover-wipe feature applied)."""
    conn = get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT request_season, done_season, collector_paused, "
                "collector_beat, requested_at FROM wipe_control WHERE id = 1"
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
        conn.commit()  # release the read's MDL under the pool's autocommit=0 default
        if not row:
            return None
        return {
            "request_season": int(row["request_season"]),
            "done_season": int(row["done_season"]),
            "collector_paused": int(row["collector_paused"]),
            "collector_beat": int(row["collector_beat"]),
            "requested_at": int(row["requested_at"]),
        }
    except mysql.connector.errors.ProgrammingError as err:
        # ER_NO_SUCH_TABLE (1146): feature not deployed to this DB — treat as "no wipe".
        if err.errno == 1146:
            return None
        raise
    finally:
        conn.close()


def set_collector_wipe_state(paused, beat_ms):
    """Collector ack: record whether its writers are currently quiesced plus a
    heartbeat (unix ms). Best-effort; no-op if the table is absent."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE wipe_control SET collector_paused = %s, collector_beat = %s "
                "WHERE id = 1",
                (1 if paused else 0, int(beat_ms)),
            )
        finally:
            cursor.close()
        conn.commit()
    except mysql.connector.errors.ProgrammingError as err:
        if err.errno == 1146:
            return
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Trend bar snapshots (see database.sql `trend_snapshot`) — a weekly per-entity
# freeze used to show week-over-week movement in the "Top Trends" bar. The
# snapshot writer (snapshotTrends.py) reads the current aggregates and upserts
# one row per (week_id, feed, group_key, entity_key); the page generators read
# the latest two weeks back via fetch_trend_snapshots + pageGeneration.build_trends.
# ---------------------------------------------------------------------------

FETCH_CURRENT_PERIOD_SQL = """
SELECT MAX(period_id) AS cur_period
FROM season_periods
WHERE season = %s
  AND start_timestamp <= CAST(UNIX_TIMESTAMP() * 1000 AS UNSIGNED)
"""


def fetch_current_period(connection, cursor, season):
    """The current reset week: the highest period whose window has started for
    this season (season_periods timestamps are unix milliseconds — same idiom as
    fetch_dungeon_timed_runs_last_two_periods). Returns an int week_id or None
    when no period has started yet."""
    rows = fetch_with_retry(connection, cursor, FETCH_CURRENT_PERIOD_SQL, (season,))
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


FETCH_RECENT_TREND_WEEKS_SQL = """
SELECT DISTINCT week_id
FROM Mythistone.trend_snapshot
ORDER BY week_id DESC
LIMIT %s
"""


def fetch_recent_trend_weeks(connection, cursor, limit=2):
    """The most recent distinct week_ids that have snapshots (newest first).
    build_trends compares the latest two."""
    rows = fetch_with_retry(connection, cursor, FETCH_RECENT_TREND_WEEKS_SQL, (int(limit),))
    return [int(r[0]) for r in rows]


FETCH_TREND_WEEK_EXISTS_SQL = """
SELECT 1 FROM Mythistone.trend_snapshot WHERE week_id = %s LIMIT 1
"""


def fetch_trend_week_exists(connection, cursor, week_id):
    """True if this reset week already has a snapshot. The writer is write-once
    per period: it freezes each week at its first build so week-over-week deltas
    span a full reset period rather than collapsing to a day right after a reset."""
    rows = fetch_with_retry(connection, cursor, FETCH_TREND_WEEK_EXISTS_SQL, (int(week_id),))
    return bool(rows)


FETCH_TREND_FEED_WEEK_EXISTS_SQL = """
SELECT 1 FROM Mythistone.trend_snapshot WHERE feed = %s AND week_id = %s LIMIT 1
"""


def fetch_trend_feed_week_exists(connection, cursor, feed, week_id):
    """True if this reset week already has a snapshot for one specific feed. Lets a
    producer that owns a single feed the main snapshot step doesn't write (the sim
    tierlist) stay write-once per period without re-freezing the whole week."""
    rows = fetch_with_retry(
        connection, cursor, FETCH_TREND_FEED_WEEK_EXISTS_SQL, (feed, int(week_id))
    )
    return bool(rows)


FETCH_PREV_TREND_WEEK_SQL = """
SELECT MAX(week_id) FROM Mythistone.trend_snapshot WHERE week_id < %s
"""


def fetch_prev_trend_week(connection, cursor, current_week_id):
    """The most recent stored baseline strictly older than the current reset week
    — i.e. "last week's snapshotted value" that build_trends diffs the live current
    against. None when no prior week exists yet."""
    rows = fetch_with_retry(connection, cursor, FETCH_PREV_TREND_WEEK_SQL, (int(current_week_id),))
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


FETCH_TREND_SNAPSHOTS_SQL = """
SELECT week_id, entity_key, label, tier, rank_pos, score, popularity, run_count
FROM Mythistone.trend_snapshot
WHERE feed = %s AND group_key = %s AND week_id IN ({placeholders})
"""


def fetch_trend_snapshots(connection, cursor, feed, group_key, week_ids):
    """Rows for one feed/group across the given weeks. Returns a list of dicts;
    empty when the feed/group has no snapshots yet."""
    if not week_ids:
        return []
    placeholders = ",".join(["%s"] * len(week_ids))
    sql = FETCH_TREND_SNAPSHOTS_SQL.format(placeholders=placeholders)
    params = [feed, str(group_key)] + [int(w) for w in week_ids]
    rows = fetch_with_retry(connection, cursor, sql, params)
    return [
        {
            "week_id": int(r[0]),
            "entity_key": r[1],
            "label": r[2],
            "tier": None if r[3] is None else int(r[3]),
            "rank_pos": None if r[4] is None else int(r[4]),
            "score": None if r[5] is None else float(r[5]),
            "popularity": float(r[6]),
            "run_count": int(r[7]),
        }
        for r in rows
    ]


UPSERT_TREND_ROW_SQL = """
INSERT INTO Mythistone.trend_snapshot
  (week_id, feed, group_key, entity_key, label, tier, rank_pos, score, popularity, run_count)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  label = VALUES(label),
  tier = VALUES(tier),
  rank_pos = VALUES(rank_pos),
  score = VALUES(score),
  popularity = VALUES(popularity),
  run_count = VALUES(run_count)
"""


def upsert_trend_rows(connection, cursor, rows):
    """Idempotent bulk write of snapshot rows. Each row is a 10-tuple matching
    UPSERT_TREND_ROW_SQL's column order. The ON DUPLICATE clause makes --force
    re-snapshots and --debug reruns safe; the normal writer is write-once per
    period (snapshotTrends guards on fetch_trend_week_exists)."""
    if not rows:
        return
    executemany_with_retry(connection, cursor, UPSERT_TREND_ROW_SQL, rows)


DELETE_OLD_TREND_WEEKS_SQL = """
DELETE FROM Mythistone.trend_snapshot WHERE week_id < %s
"""


def prune_trend_snapshots(connection, cursor, keep_from_week):
    """Drop snapshots older than keep_from_week (retention bound). Cheap; the
    table is only a few thousand rows per week."""
    execute_with_retry(connection, cursor, DELETE_OLD_TREND_WEEKS_SQL, (int(keep_from_week),))


# --- snapshot source fetchers (per-spec / per-dungeon, small result sets) -----

FETCH_TALENT_USAGE_SQL = """
SELECT talent_id, tree, SUM(run_count) AS run_count
FROM (
  SELECT talent_id, 'spec'  AS tree, run_count FROM Mythistone.aggregated_spec_talent  WHERE spec_id = %s AND season = %s
  UNION ALL
  SELECT talent_id, 'class' AS tree, run_count FROM Mythistone.aggregated_class_talent WHERE spec_id = %s AND season = %s
  UNION ALL
  SELECT talent_id, 'hero'  AS tree, run_count FROM Mythistone.aggregated_hero_talent  WHERE spec_id = %s AND season = %s
) u
GROUP BY talent_id, tree
ORDER BY run_count DESC
"""


def fetch_talent_usage(connection, cursor, spec_id, season):
    """Per-talent pickrate for a spec, rolled up across dungeon/hero-tree splits
    for all three talent tables. Returns [{talent_id, tree, run_count}]."""
    params = (spec_id, season, spec_id, season, spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_TALENT_USAGE_SQL, params)
    return [
        {"talent_id": int(r[0]), "tree": r[1], "run_count": int(r[2])} for r in rows
    ]


FETCH_EQUIPMENT_USAGE_SQL = """
SELECT slot, item_id, run_count
FROM Mythistone.global_aggregated_equipment
WHERE spec_id = %s AND season = %s
"""


def fetch_equipment_usage(connection, cursor, spec_id, season):
    """All per-slot item usage for a spec (small — a few hundred rows). The
    writer keeps top-N per slot and computes each slot's share denominator."""
    params = (spec_id, season)
    rows = fetch_with_retry(connection, cursor, FETCH_EQUIPMENT_USAGE_SQL, params)
    return [
        {"slot": r[0], "item_id": str(r[1]), "run_count": int(r[2])} for r in rows
    ]


FETCH_EMBELLISHMENT_USAGE_SQL = """
SELECT item_id, run_count
FROM Mythistone.global_aggregated_embellishments
WHERE spec_id = %s AND season = %s
ORDER BY run_count DESC
"""

FETCH_CRAFTED_USAGE_SQL = """
SELECT item_id, run_count
FROM Mythistone.global_aggregated_crafted_items
WHERE spec_id = %s AND season = %s
ORDER BY run_count DESC
"""

FETCH_GEM_USAGE_SQL = """
SELECT socket_item_id, SUM(run_count) AS run_count
FROM Mythistone.global_aggregated_item_sockets
WHERE spec_id = %s AND season = %s
GROUP BY socket_item_id
ORDER BY run_count DESC
"""


def _fetch_id_run_pairs(connection, cursor, sql, spec_id, season):
    rows = fetch_with_retry(connection, cursor, sql, (spec_id, season))
    return [{"item_id": str(r[0]), "run_count": int(r[1])} for r in rows]


def fetch_embellishment_usage(connection, cursor, spec_id, season):
    return _fetch_id_run_pairs(connection, cursor, FETCH_EMBELLISHMENT_USAGE_SQL, spec_id, season)


def fetch_crafted_usage(connection, cursor, spec_id, season):
    return _fetch_id_run_pairs(connection, cursor, FETCH_CRAFTED_USAGE_SQL, spec_id, season)


def fetch_gem_usage(connection, cursor, spec_id, season):
    """Gem popularity for a spec: socket_item_id usage summed across the items it
    was socketed into. Returns [{item_id (=gem), run_count}]."""
    return _fetch_id_run_pairs(connection, cursor, FETCH_GEM_USAGE_SQL, spec_id, season)


FETCH_MISSIVE_USAGE_SQL = """
SELECT item_id, run_count
FROM Mythistone.global_aggregated_missives
WHERE spec_id = %s AND season = %s
ORDER BY run_count DESC
"""


def fetch_missive_usage(connection, cursor, spec_id, season):
    """Missive popularity for a spec, shaped like the other snapshot misc feeds:
    [{item_id, run_count}]. (fetch_missive_count returns positional tuples with
    the crafting max-key columns, the wrong shape for the trend snapshot.)"""
    return _fetch_id_run_pairs(connection, cursor, FETCH_MISSIVE_USAGE_SQL, spec_id, season)


