"""Bounded live->test copy of ONE spec's real runs, so the local simc smoke test
builds its BiS profile from real gear/talents instead of synthetic rows.

Why: the synthetic seeder invents item ids, bonus ids and loadout strings. simc
rejects those at profile init, so every simc chunk fails and a real bug in
simcBis profile generation would be masked. Copying a small slice of REAL runs
for the target spec (Arcane Mage, spec_id 62, by default) makes its aggregated
gear/talents real, so the collector can build a valid profile and actually sim.

Contract:
  * READ-ONLY against the live DB (LIVE_DATABASE_* env, opened as its own
    connection with the shared read-only session config). It NEVER writes to live.
  * Writes only to the test DB (the pool seed_test_db.py already initialized,
    reached via databaseConnector.get_connection()).
  * Copies a self-contained slice: the most recent runs (current season, inside
    the 14-day aggregation window) whose roster includes the target spec, plus
    every run_member, member, equipment row, enchantment, socket, and the
    talent_sets / bonus_sets dictionary rows those reference. The content-hash
    set_id contract (see the dedup-dictionary-hash-contract skill) is respected
    for free: dictionary rows are copied verbatim by set_id.
  * Live auto-increment ids (run_id / member / equipment_id) are PRESERVED
    verbatim. Live ids are in the millions; the seeder's synthetic ids start at 1,
    so they never collide, and preserving them keeps every FK intact without an
    id-remap pass.
  * BEFORE inserting, PURGE the synthetic rows for the target spec so its
    aggregates end up pure real. Other specs' synthetic data is left untouched.

The caller (seed_test_db.py) rebuilds the aggregates afterwards so the target
spec's popularity/gear/talent aggregates reflect the real slice.

This module follows the localDev seeding precedent (seeders.py): parameterized
inline SQL run through databaseConnector's retry wrappers, rather than adding
dev-only query functions to the production databaseConnector module.
"""

import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, BACKEND_DIR)

import mysql.connector  # noqa: E402
import databaseConnector as db  # noqa: E402

_MS_PER_DAY = 86400 * 1000

LIVE_ENV_KEYS = ["LIVE_DATABASE_HOST", "LIVE_DATABASE_USER", "LIVE_DATABASE_PASSWORD",
                 "LIVE_DATABASE_NAME", "LIVE_DATABASE_PORT"]


def live_env():
    """Return the LIVE_DATABASE_* config dict, or None if any key is missing."""
    vals = {k: os.environ.get(k) for k in LIVE_ENV_KEYS}
    if any(not v for v in vals.values()):
        return None
    return {
        "host": vals["LIVE_DATABASE_HOST"],
        "user": vals["LIVE_DATABASE_USER"],
        "password": vals["LIVE_DATABASE_PASSWORD"],
        "name": vals["LIVE_DATABASE_NAME"],
        "port": vals["LIVE_DATABASE_PORT"],
    }


def _live_connect(cfg):
    """Open a dedicated READ-ONLY connection to the live DB (never pooled, never
    written to). Uses the same relaxed read session the page builders use so it
    can never hold locks against the live aggregation pipeline."""
    conn = mysql.connector.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"],
        database=cfg["name"], port=int(cfg["port"]),
    )
    cur = conn.cursor()
    db.configure_read_session(conn, cur)
    return conn, cur


def _fetch_in(conn, cur, sql_tmpl, ids, prefix_params=(), chunk=1000):
    """Run a SELECT whose WHERE has an IN list, chunked to bound the placeholder
    count. sql_tmpl must contain a single ``{ph}`` where the IN placeholders go."""
    out = []
    ids = list(ids)
    for i in range(0, len(ids), chunk):
        part = ids[i:i + chunk]
        ph = ",".join(["%s"] * len(part))
        sql = sql_tmpl.format(ph=ph)
        out.extend(db.fetch_with_retry(conn, cur, sql, tuple(prefix_params) + tuple(part)))
    return out


# --- live read (READ-ONLY) -------------------------------------------------
SELECT_RUNS_SQL = """
SELECT r.dungeon_id, r.keystone_level, r.duration, r.`timestamp`, r.faction,
       r.run_id, r.region, r.season
FROM runs r
WHERE r.season = %s
  AND r.`timestamp` > %s
  AND EXISTS (
    SELECT 1 FROM run_members rm
    JOIN members m ON m.member = rm.member
    WHERE rm.run_id = r.run_id AND m.spec_id = %s
  )
ORDER BY r.`timestamp` DESC
LIMIT %s
"""


def fetch_slice(live_conn, live_cur, spec_id, season, max_runs, window_days, keep_dungeons):
    """Read a self-contained slice of real runs for spec_id from the live DB.

    keep_dungeons: the set of dungeon_ids present in the TEST dungeon_data, so
    runs whose dungeon the test DB does not know are dropped (their FK to
    dungeon_data would otherwise fail).
    """
    window_ms = int(time.time() * 1000) - window_days * _MS_PER_DAY
    run_rows = db.fetch_with_retry(
        live_conn, live_cur, SELECT_RUNS_SQL, (season, window_ms, spec_id, max_runs))
    # Drop runs whose dungeon the test DB does not carry (keeps runs.dungeon_id FK valid).
    run_rows = [r for r in run_rows if r[0] in keep_dungeons]
    run_ids = [r[5] for r in run_rows]
    if not run_ids:
        return {"runs": [], "run_members": [], "members": [], "equipment": [],
                "enchantments": [], "sockets": [], "talent_sets": [], "bonus_sets": []}

    rm_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT member, run_id FROM run_members WHERE run_id IN ({ph})", run_ids)
    member_ids = sorted({r[0] for r in rm_rows})

    member_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT member, spec_id, loadout, hero_talent_id, talent_set_id "
        "FROM members WHERE member IN ({ph})", member_ids)

    equip_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT slot, item_id, item_level, member, equipment_id, bonus_set_id "
        "FROM equipment WHERE member IN ({ph})", member_ids)
    equip_ids = [r[4] for r in equip_rows]

    ench_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT enchantment_id, equipment_id FROM enchantments WHERE equipment_id IN ({ph})",
        equip_ids) if equip_ids else []
    socket_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT socket_type, socket_item_id, equipment_id FROM sockets WHERE equipment_id IN ({ph})",
        equip_ids) if equip_ids else []

    talent_ids = sorted({r[4] for r in member_rows if r[4] is not None})
    talent_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT set_id, tree, talent_id, `rank` FROM talent_sets WHERE set_id IN ({ph})",
        talent_ids) if talent_ids else []

    bonus_ids = sorted({r[5] for r in equip_rows if r[5] is not None})
    bonus_rows = _fetch_in(
        live_conn, live_cur,
        "SELECT set_id, bonus_id FROM bonus_sets WHERE set_id IN ({ph})",
        bonus_ids) if bonus_ids else []

    return {
        "runs": run_rows,
        "run_members": rm_rows,
        "members": member_rows,
        "equipment": equip_rows,
        "enchantments": ench_rows,
        "sockets": socket_rows,
        "talent_sets": talent_rows,
        "bonus_sets": bonus_rows,
    }


# --- test-DB purge + insert ------------------------------------------------
def purge_spec(test_conn, test_cur, spec_id):
    """Remove the synthetic rows for spec_id from the test DB so its aggregates
    end up pure real. Deleting the members cascades to their equipment (and its
    enchantments/sockets) and run_members via ON DELETE CASCADE; then drop any
    run left with no members."""
    db.execute_with_retry(test_conn, test_cur,
                          "DELETE FROM members WHERE spec_id = %s", (spec_id,))
    db.execute_with_retry(test_conn, test_cur,
                          "DELETE r FROM runs r "
                          "LEFT JOIN run_members rm ON rm.run_id = r.run_id "
                          "WHERE rm.run_id IS NULL")
    db.commit_with_retry(test_conn)


def _test_dungeon_ids(test_conn, test_cur):
    rows = db.fetch_with_retry(test_conn, test_cur, "SELECT dungeon_id FROM dungeon_data")
    return {r[0] for r in rows}


def insert_slice(test_conn, test_cur, slice_):
    """Insert the real slice into the test DB, FK-safe order, live ids preserved.

    Dictionary tables use INSERT IGNORE (identical content hashes to identical
    set_id rows, so a set already present is a no-op). runs/members/equipment use
    plain INSERT so any unexpected id clash fails loudly."""
    # dictionaries first (no FK; referenced by members / equipment)
    if slice_["talent_sets"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT IGNORE INTO talent_sets (set_id, tree, talent_id, `rank`) VALUES (%s,%s,%s,%s)",
            slice_["talent_sets"])
    if slice_["bonus_sets"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT IGNORE INTO bonus_sets (set_id, bonus_id) VALUES (%s,%s)",
            slice_["bonus_sets"])
    # runs -> members -> run_members -> equipment -> enchantments/sockets
    if slice_["runs"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT INTO runs (dungeon_id, keystone_level, duration, `timestamp`, faction, "
            "run_id, region, season) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            slice_["runs"])
    if slice_["members"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT INTO members (member, spec_id, loadout, hero_talent_id, talent_set_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            slice_["members"])
    if slice_["run_members"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT IGNORE INTO run_members (member, run_id) VALUES (%s,%s)",
            slice_["run_members"])
    if slice_["equipment"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT INTO equipment (slot, item_id, item_level, member, equipment_id, bonus_set_id) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            slice_["equipment"])
    if slice_["enchantments"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT INTO enchantments (enchantment_id, equipment_id) VALUES (%s,%s)",
            slice_["enchantments"])
    if slice_["sockets"]:
        db.executemany_with_retry(test_conn, test_cur,
            "INSERT INTO sockets (socket_type, socket_item_id, equipment_id) VALUES (%s,%s,%s)",
            slice_["sockets"])
    db.commit_with_retry(test_conn)


def pull_and_seed(test_conn, test_cur, cfg, spec_id, season, max_runs=50, window_days=14):
    """Purge synthetic spec_id rows, copy a real slice from live, insert it.

    Returns the number of real runs inserted (0 means nothing was seeded, e.g.
    the live season has no recent runs for this spec)."""
    keep_dungeons = _test_dungeon_ids(test_conn, test_cur)
    live_conn, live_cur = _live_connect(cfg)
    try:
        slice_ = fetch_slice(live_conn, live_cur, spec_id, season, max_runs, window_days,
                             keep_dungeons)
    finally:
        try:
            live_cur.close()
            live_conn.close()  # read-only; never committed
        except Exception:
            pass

    n_runs = len(slice_["runs"])
    print(f"  live slice for spec {spec_id}: {n_runs} runs, {len(slice_['members'])} members, "
          f"{len(slice_['equipment'])} equipment, {len(slice_['talent_sets'])} talent-set rows, "
          f"{len(slice_['bonus_sets'])} bonus-set rows")
    if n_runs == 0:
        print(f"  no recent live runs for spec {spec_id}; leaving synthetic data in place.")
        return 0

    purge_spec(test_conn, test_cur, spec_id)
    insert_slice(test_conn, test_cur, slice_)
    print(f"  purged synthetic spec {spec_id} and inserted {n_runs} real runs.")
    return n_runs
