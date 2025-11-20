import mysql.connector
import time
from mysql.connector import errorcode
from mysql.connector import pooling
import random

# configuration
MAX_LOCK_WAIT_RETRIES = 5
LOCK_WAIT_BACKOFF_MIN = 0.2
LOCK_WAIT_BACKOFF_MAX = 1


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


INSERT_HUNTER_PETS_SQL = "INSERT IGNORE INTO Mythistone.hunter_pets (`member`, `creature_id`) VALUES (%s, %s)"


def insert_hunter_pets_batch(connection, cursor, run_vals):
    """Bulk-insert hunter pets, returns first inserted run_id."""
    executemany_with_retry(connection, cursor, INSERT_HUNTER_PETS_SQL, run_vals)
    return cursor.lastrowid


def insert_hunter_pets(connection, cursor, member: int, creature_id: int):
    """Insert a member into the members table."""
    val = (member, creature_id)
    return execute_with_retry(connection, cursor, INSERT_HUNTER_PETS_SQL, val)


SELECT_RUNS_SQL = "SELECT id, `dungeon_id`, `keystone_level`, `duration`, `timestamp`, `faction`, `run_id`, `region`, season FROM runs WHERE (`season`, `region`, `dungeon_id`) IN (%s, %s, %s)"


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
  SUM(run_count) AS equip_count
FROM Mythistone.aggregated_equipment
WHERE spec_id = %s
  AND season  = %s
  AND slot    = %s
GROUP BY item_id
ORDER BY equip_count DESC
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
FROM Mythistone.aggregated_equipment
JOIN Mythistone.slot_group_map sgm ON sgm.slot = Mythistone.aggregated_equipment.slot
WHERE spec_id = %s
  AND season  = %s
  AND slot_group = %s
GROUP BY item_id
ORDER BY equip_count DESC
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
  FROM Mythistone.aggregated_equipment
  WHERE spec_id = %s
    AND season  = %s
    AND slot    = %s
  GROUP BY item_id
  ORDER BY equip_count DESC
  LIMIT 10
),
bonus_sums AS (
  SELECT
    item_id,
    bonus_list,
    SUM(run_count) AS list_count
  FROM Mythistone.aggregated_bonus_lists
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
  r.list_count
FROM top_items ti
LEFT JOIN ranked r ON ti.item_id = r.item_id AND r.rn = 1
ORDER BY ti.equip_count DESC;
"""


def fetch_top_items_for_slot_with_bonus(connection, cursor, spec_id, season, slot):
    """Fetch the top items with bonus for a specific slot from the database."""
    params = (spec_id, season, slot, spec_id, season)
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_ITEMS_BY_SLOT_WITH_BONUS_SQL, params
    )

    data = []
    for row in rows:
        # row = (item_id, equip_count, bonus_list, list_count)
        item_id = row[0]
        equip_count = row[1]
        bonus_list = row[2]  # may be None
        list_count = row[3]  # may be None
        data.append(
            {
                "item": item_id,
                "count": int(equip_count),
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
  FROM Mythistone.aggregated_equipment ae
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = ae.slot
  WHERE ae.spec_id = %s
    AND ae.season  = %s
    AND sgm.slot_group = %s
  GROUP BY ae.item_id
  ORDER BY equip_count DESC
  LIMIT 10
),
bonus_sums AS (
  SELECT
    item_id,
    bonus_list,
    SUM(run_count) AS list_count
  FROM Mythistone.aggregated_bonus_lists
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
  r.list_count
FROM top_items ti
LEFT JOIN ranked r ON ti.item_id = r.item_id AND r.rn = 1
ORDER BY ti.equip_count DESC;
"""


def fetch_top_items_for_slot_group_with_bonus(
    connection, cursor, spec_id, season, slot_group
):
    """Fetch top items for a slot_group along with each item's top bonus_list (MySQL 8+)."""
    # param order must match the SQL: spec, season, slot_group, spec, season
    params = (spec_id, season, slot_group, spec_id, season)
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_ITEMS_BY_SLOT_GROUP_WITH_BONUS_SQL, params
    )

    data = []
    for row in rows:
        # row = (item_id, equip_count, bonus_list, list_count)
        item_id = row[0]
        equip_count = row[1]
        bonus_list = row[2]  # may be None
        list_count = row[3]  # may be None
        data.append(
            {
                "item": item_id,
                "count": int(equip_count),
                "bonus": {"ids": bonus_list, "count": int(list_count)}
                if bonus_list is not None
                else None,
            }
        )
    return data


FETCH_TOP_ENCHANT_FOR_SLOT_SQL = """
SELECT
    enchantment_id,
    SUM(run_count) AS equip_count
  FROM Mythistone.aggregated_enchantments_slot_group
  WHERE spec_id = %s
    AND season  = %s
    AND slot_group = %s
  GROUP BY enchantment_id
  ORDER BY equip_count DESC 
  LIMIT %s
"""


def fetch_top_enchant_for_slot(connection, cursor, spec_id, season, slot_group, amount):
    """Fetch the top enchant for a specific slot from the database."""
    params = (spec_id, season, slot_group, amount)
    return fetch_with_retry(connection, cursor, FETCH_TOP_ENCHANT_FOR_SLOT_SQL, params)


FETCH_TOP_SOCKET_FOR_ITEM_SQL = """
SELECT ais.socket_item_id, SUM(ais.run_count) AS equip_count
FROM Mythistone.aggregated_item_sockets AS ais
WHERE ais.spec_id LIKE %s
  AND ais.season  LIKE %s
  AND ais.item_id LIKE %s
GROUP BY ais.socket_item_id
ORDER BY equip_count DESC
LIMIT 10;

"""


def fetch_top_sockets_for_item(connection, cursor, spec_id, season, item_id):
    """Fetch the top sockets for a specific item from the database."""
    params = (spec_id, season, item_id)
    return fetch_with_retry(connection, cursor, FETCH_TOP_SOCKET_FOR_ITEM_SQL, params)


FETCH_TOP_SOCKETS_FOR_ITEMS_SQL = """
SELECT ais.item_id, ais.socket_item_id, SUM(ais.run_count) AS equip_count
FROM Mythistone.aggregated_item_sockets AS ais
WHERE ais.spec_id = %s
  AND ais.season  = %s
  AND ais.item_id IN ({placeholders})
GROUP BY ais.item_id, ais.socket_item_id
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
    for item_id, socket_item_id, equip_count in rows:
        key = str(item_id)
        out.setdefault(key, []).append((socket_item_id, int(equip_count)))
    return out


FETCH_TOP_BONUS_IDS_FOR_ITEM_SQL = """
SELECT
  bonus_list,
  SUM(run_count) AS list_count
FROM Mythistone.aggregated_bonus_lists
WHERE spec_id = %s
  AND season  = %s
  AND item_id = %s
GROUP BY bonus_list
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
SELECT ais.socket_item_id, SUM(ais.run_count) AS equip_count
FROM Mythistone.aggregated_item_sockets AS ais
WHERE ais.spec_id LIKE %s
  AND ais.season  LIKE %s
GROUP BY ais.socket_item_id
ORDER BY equip_count DESC
LIMIT 10;
"""


def fetch_top_sockets(connection, cursor, spec_id, season):
    """Fetch the top sockets for a specific item from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_TOP_SOCKETS_SQL, params)


FETCH_TOP_LOADOUT_SQL = """
WITH summed AS (
  SELECT
    spec_id,
    season,
    hero_talent_id,
    hero_talent_id_key,
    loadout_key,
    MAX(loadout) AS loadout,         -- recover actual loadout text (if any)
    SUM(run_count) AS total_runs
  FROM aggregated_loadout_data
  WHERE spec_id = %s   
    AND season  = %s   
    AND loadout  IS NOT NULL
  GROUP BY spec_id, season, hero_talent_id, hero_talent_id_key, loadout_key
)
, ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY hero_talent_id_key ORDER BY total_runs DESC, loadout_key) AS rn
  FROM summed
)
SELECT hero_talent_id, loadout, total_runs
FROM ranked
WHERE rn = 1
ORDER BY hero_talent_id_key;


"""


def fetch_top_loadout(connection, cursor, spec_id, season):
    """Fetch the top loadout for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_TOP_LOADOUT_SQL, params)


FETCH_HERO_TREE_OVERVIEW_SQL = """
SELECT
  hero_talent_id,
  SUM(run_count) AS total_runs
FROM aggregated_loadout_data
WHERE spec_id = %s
  AND season  = %s
  AND hero_talent_id IS NOT NULL 
  AND hero_talent_id <> 0
GROUP BY hero_talent_id
ORDER BY total_runs DESC;
"""


def fetch_hero_tree_overview(connection, cursor, spec_id, season):
    """Fetch the top hero trees for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_HERO_TREE_OVERVIEW_SQL, params)


FETCH_HERO_TREE_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, SUM(run_count) 
FROM Mythistone.aggregated_hero_talent aht 
WHERE aht.spec_id = %s AND aht.season = %s  
GROUP BY aht.hero_talent_id, aht.dungeon_id 
"""


def fetch_hero_tree_differences(connection, cursor, spec_id, season):
    """Fetch the hero talents differences for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_HERO_TREE_DIFFERENCES_SQL, params)


FETCH_HERO_TALENTS_DIFFERENCES_SQL = """
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count) 
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
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count) 
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
SELECT hero_talent_id, dungeon_id, talent_id, SUM(run_count) 
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
WHERE spec_id = %s;
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
  AND season = %s
  AND hero_talent_id <> 0;

"""


def fetch_spec_data_count(connection, cursor, spec_id, season):
    """Fetch the spec data count for a specific spec and season from the database.
    Always returns an int (0 if no runs)."""
    params = (spec_id, season)
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


INSERT_MISSIVE_SQL = """
INSERT IGNORE INTO missives (`bonus_id`, `item_id`) VALUES (%s, %s)
"""


def insert_missive(connection, cursor, bonus_id, item_id):
    """Insert a new missive into the database."""
    params = (bonus_id, item_id)
    return execute_with_retry(connection, cursor, INSERT_MISSIVE_SQL, params)


FETCH_MISSIVE_COUNT_SQL = """
SELECT item_id, SUM(run_count) AS total_runs
FROM aggregated_missives
WHERE spec_id = %s
  AND season = %s
GROUP BY item_id
ORDER BY total_runs DESC
"""


def fetch_missive_count(connection, cursor, spec_id, season):
    """Fetch the missive count for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_MISSIVE_COUNT_SQL, params)


FETCH_EMBELLISHMENT_COUNT_SQL = """
SELECT item_id, SUM(run_count) AS total_runs
FROM aggregated_embellishments
WHERE spec_id = %s
  AND season = %s
GROUP BY item_id
ORDER BY total_runs DESC
"""


def fetch_embellishment_count(connection, cursor, spec_id, season):
    """Fetch the embellishment count for a specific spec and season from the database."""
    params = (spec_id, season)
    return fetch_with_retry(connection, cursor, FETCH_EMBELLISHMENT_COUNT_SQL, params)


FETCH_TOTAL_SEASON_RUNS_SQL = """
SELECT COUNT(run_id) AS total_runs
FROM runs
WHERE season = %s
"""


def fetch_total_season_runs(connection, cursor, season):
    """Fetch the total season runs for a specific season from the database."""
    rows = fetch_with_retry(connection, cursor, FETCH_TOTAL_SEASON_RUNS_SQL, (season,))
    amount_row = rows[0] if rows else None
    # amount_row might be tuple or dict depending on cursor type
    if not amount_row:
        return 0
    if isinstance(amount_row, dict):
        total_runs = amount_row.get("total_runs") or 0
    else:
        total_runs = amount_row[0] or 0
    return total_runs


FETCH_SEASON_RUNS_FOR_SPEC_SQL = """
SELECT SUM(run_count) AS total_runs
FROM aggregated_spec
WHERE season = %s
AND spec_id = %s
"""


def fetch_runs_per_spec(connection, cursor, season, spec_id):
    """Fetch the total season runs for a specific season+spec and return an int."""
    params = (season, spec_id)
    rows = fetch_with_retry(connection, cursor, FETCH_SEASON_RUNS_FOR_SPEC_SQL, params)
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        total_runs = row.get("total_runs") or 0
    else:
        total_runs = row[0] or 0
    return int(total_runs)


FETCH_MAX_KEY_SPEC_SQL = """
WITH maxk AS (
  SELECT spec_id, season, MAX(keystone_level) AS max_keystone
  FROM aggregated_spec
  WHERE spec_id = %s
    AND season = %s
  GROUP BY spec_id, season
),
chosen_run AS (
  SELECT r.*
  FROM runs r
  JOIN maxk m ON r.season = m.season
              AND r.keystone_level = m.max_keystone
  WHERE EXISTS (
    SELECT 1
    FROM run_members rm
    JOIN members mm ON mm.member = rm.member
    WHERE rm.run_id = r.run_id
      AND mm.spec_id = %s
  )
  ORDER BY r.duration ASC, r.timestamp ASC
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
WITH maxk AS (
  SELECT season, MAX(keystone_level) AS max_keystone
  FROM aggregated_spec
  WHERE season = %s
  GROUP BY season
),
chosen_run AS (
  SELECT r.*
  FROM runs r
  JOIN maxk m ON r.season = m.season
              AND r.keystone_level = m.max_keystone
  WHERE EXISTS (
    SELECT 1
    FROM run_members rm
    JOIN members mm ON mm.member = rm.member
    WHERE rm.run_id = r.run_id
  )
  ORDER BY r.duration ASC, r.timestamp ASC
  LIMIT 1
)
SELECT cr.*, mb.member AS member_id, mb.spec_id AS member_spec_id
FROM chosen_run cr
JOIN run_members rm ON rm.run_id = cr.run_id
JOIN members mb     ON mb.member = rm.member
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
AND season = %s
GROUP BY upgrade_tier 

"""


def fetch_spec_upgrade(connection, cursor, spec_id, season):
    params = (spec_id, season)
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
WHERE season = %s
GROUP BY spec_id
ORDER BY count DESC
"""


def fetch_spec_run_counts(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_RUN_COUNTS, params)
    if not rows:
        return []
    return [{"id": int(row[0]), "count": int(row[1])} for row in rows]


FETCH_SPEC_RUN_COUNTS_PER_LEVEL = """
SELECT spec_id, keystone_level, SUM(run_count) AS count
FROM Mythistone.aggregated_spec
WHERE season = %s
GROUP BY spec_id, keystone_level
ORDER BY spec_id, keystone_level;
"""


def fetch_spec_run_counts_per_level(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_RUN_COUNTS_PER_LEVEL, params)
    if not rows:
        return []
    return [
        {"spec_id": row[0], "keystone_level": row[1], "count": row[2]} for row in rows
    ]


FETCH_RUNS_PER_PERIOD = """
-- params: (season, season)
SELECT
  t.week,
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
GROUP BY t.week
ORDER BY t.week;
"""


def fetch_runs_per_period(connection, cursor, season):
    params = (season, season)
    rows = fetch_with_retry(connection, cursor, FETCH_RUNS_PER_PERIOD, params)
    if not rows:
        return []
    return [
        {
            "week": int(row[0]),
            "upgrade_3": int(row[1]),
            "upgrade_2": int(row[2]),
            "upgrade_1": int(row[3]),
            "depleted": int(row[4]),
            "total_runs": int(row[5]),
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


def fetch_runs_per_dungeon(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(connection, cursor, DUNGEON_UPGRADES_SQL, params)
    if not rows:
        return []
    return [
        {
            "dungeon_id": int(row[0]),
            "upgrade_3": int(row[1]),
            "upgrade_2": int(row[2]),
            "upgrade_1": int(row[3]),
            "depleted": int(row[4]),
            "total_runs": int(row[5]),
        }
        for row in rows
    ]


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
WHERE season = %s
GROUP BY spec_id, keystone_level
ORDER BY total_runs DESC;

"""


def fetch_spec_upgrades(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(connection, cursor, FETCH_SPEC_UPGRADES_SQL, params)
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
WHERE season = %s AND keystone_level > %s
GROUP BY spec_id, keystone_level
ORDER BY total_runs DESC;

"""


def fetch_spec_upgrades_above_level(connection, cursor, season, min_keylevel=15):
    params = (season, min_keylevel)
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
WHERE season = %s AND spec_id IN ({placeholders}) and keystone_level > %s
GROUP BY keystone_level
ORDER BY total_runs DESC;

"""


def fetch_upgrade_for_specs(connection, cursor, season, specs, min_keylevel=15):
    spec_placeholder = ",".join(["%s"] * len(specs))
    specs_clean = [str(i) for i in specs]
    sql = FETCH_UPGRADES_FOR_SPECS_SQL.format(placeholders=spec_placeholder)
    params = [season] + specs_clean + [min_keylevel]
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


def fetch_runs_per_dungeon_per_level(connection, cursor, season):
    params = (season,)
    rows = fetch_with_retry(
        connection, cursor, DUNGEON_UPGRADES_PER_KEYLEVEL_SQL, params
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


def fetch_runs_per_dungeon_per_level_above_level(
    connection, cursor, season, min_keylevel=15
):
    params = (season, min_keylevel)
    rows = fetch_with_retry(
        connection, cursor, DUNGEON_UPGRADES_PER_KEYLEVEL_ABOVE_LEVEL_SQL, params
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


FETCH_SPEC_TALENT_OVERVIEW_SQL = """
SELECT talent_id, SUM(run_count) as count
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
SELECT talent_id, SUM(run_count) as count
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


FETCH_TOP_HUNTER_PETS_BY_SPEC_SQL = """
SELECT COUNT(hp.`member` ) as run_count, hp.creature_id  
from Mythistone.hunter_pets hp 
JOIN Mythistone.members m on hp.`member` = m.`member` 
WHERE m.spec_id = %s
GROUP BY hp.creature_id  ORDER BY run_count DESC LIMIT 100
"""


def fetch_top_hunter_pets_by_spec(connection, cursor, spec_id):
    params = (spec_id,)
    rows = fetch_with_retry(
        connection, cursor, FETCH_TOP_HUNTER_PETS_BY_SPEC_SQL, params
    )
    if not rows:
        return []
    return [{"creature_id": int(row[1]), "run_count": int(row[0])} for row in rows]


FETCH_TOP_HUNTER_PETS_SQL = """
SELECT COUNT(hp.`member` ) as run_count, hp.creature_id  
from Mythistone.hunter_pets hp GROUP BY hp.creature_id  ORDER BY run_count DESC LIMIT 500
"""


def fetch_top_hunter_pets(connection, cursor):
    rows = fetch_with_retry(connection, cursor, FETCH_TOP_HUNTER_PETS_SQL)
    if not rows:
        return []
    return [{"creature_id": int(row[1]), "run_count": int(row[0])} for row in rows]


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
    # base SELECT
    sql = "SELECT route_key, rio_run_id, enemy_forces, timestamp, keystone_level, duration, dungeon_id FROM Mythistone.route_data"
    clauses = []
    params = []

    if min_level and int(min_level) > 0:
        clauses.append("keystone_level >= %s")
        params.append(int(min_level))

    # handle recent_only_days: convert to unix timestamp seconds
    if recent_only_days:
        # We conservatively assume 'timestamp' is unix seconds OR milliseconds; we'll filter by seconds threshold,
        # and callers can interpret timestamps accordingly. Use UNIX_TIMESTAMP() to compute seconds.
        clauses.append(
            "timestamp >= CAST(UNIX_TIMESTAMP(NOW() - INTERVAL %s DAY) AS UNSIGNED)"
        )
        params.append(int(recent_only_days))

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

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

        specs = route_specs_map.get(route_key, [])
        spec_key = ",".join(str(s) for s in sorted(specs)) if specs else "unknown"

        out[spec_key] = {
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
