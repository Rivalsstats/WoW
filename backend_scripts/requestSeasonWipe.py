#!/usr/bin/env python3
"""CI side of the season-rollover wipe handshake.

Two modes:

  --detect   Decide whether a wipe should be requested. Reads the current
             blizzard_season_id from data/static/seasonInfo.json, checks the DB
             for lingering old-season data (runs.season < current) that has not
             already been cleared (wipe_control.done_season < current), and emits
             `current`, `oldmax`, `should_wipe` (to $GITHUB_OUTPUT if set, else
             stdout). It does NOT verify the archive branch — the workflow does
             that separately with the deploy key.

  --commit   Raise the intent flag: upsert wipe_control.request_season = <season>
             (idempotent; a no-op once done_season has caught up). The DB event
             ev_season_wipe + the paused collector then perform the actual clear.
             Pass --expect-season to assert the boundary hasn't moved since it was
             detected: the manual approval in between can sit for days, and
             committing a stale, lower season id would mark the wrong boundary
             cleared. Mismatch exits non-zero instead of guessing.

The wipe keys off seasonInfo.json (the archive's source of truth), never off live
runs.season — see database.sql's wipe section for the timing hazard this avoids.
"""
import argparse
import json
import os
import sys
import time

import mysql.connector

import databaseConnector

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEASON_INFO_JSON = os.path.join(REPO_ROOT, "data", "static", "seasonInfo.json")

ER_NO_SUCH_TABLE = 1146


def current_season_id():
    with open(SEASON_INFO_JSON, "r", encoding="utf-8") as f:
        info = json.load(f)
    return int(info["blizzard_season_id"])


def _init_pool():
    databaseConnector.init_connection_pool(
        os.environ["DATABASE_HOST"],
        os.environ["DATABASE_USER"],
        os.environ["DATABASE_PASSWORD"],
        os.environ["DATABASE_NAME"],
        os.environ["DATABASE_PORT"],
        pool_size=2,
    )


def _emit(**kv):
    """Write key=value pairs to $GITHUB_OUTPUT when running in Actions, and always
    echo them to stdout for logs / local runs."""
    line = " ".join(f"{k}={v}" for k, v in kv.items())
    print(line)
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={v}\n")


def _summary(text):
    """Append a line to the Actions job summary so a skipped run says why. A green
    workflow that silently did nothing is indistinguishable from one that silently
    failed to do something."""
    print(text)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")


def detect(current):
    conn = databaseConnector.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(season), 0) FROM runs WHERE season < %s", (current,)
        )
        oldmax = int(cur.fetchone()[0])

        done = 0
        table_ok = True
        try:
            cur.execute("SELECT done_season FROM wipe_control WHERE id = 1")
            row = cur.fetchone()
            done = int(row[0]) if row else 0
        except mysql.connector.errors.ProgrammingError as err:
            if err.errno == ER_NO_SUCH_TABLE:
                table_ok = False  # feature SQL not deployed to this DB
            else:
                raise
        cur.close()
        conn.commit()
    finally:
        conn.close()

    if not table_ok:
        print(
            "wipe_control table missing — season-wipe SQL not deployed to this DB; "
            "refusing to request a wipe.",
            file=sys.stderr,
        )
        _emit(current=current, oldmax=oldmax, should_wipe="false")
        _summary(
            "**Season wipe: not requested.** `wipe_control` is missing — the "
            "season-wipe SQL has not been applied to this database."
        )
        return

    should_wipe = oldmax > 0 and done < current
    _emit(current=current, oldmax=oldmax, should_wipe="true" if should_wipe else "false")
    if should_wipe:
        _summary(
            f"**Season rollover detected.** Current season `{current}`, lingering "
            f"old-season data up to season `{oldmax}` (last cleared boundary: "
            f"`{done}`). Pending the archive check and manual approval."
        )
    elif oldmax <= 0:
        _summary(
            f"Season wipe: nothing to do — no `runs` rows older than season "
            f"`{current}`."
        )
    else:
        _summary(
            f"Season wipe: nothing to do — boundary into season `{current}` was "
            f"already cleared (`done_season` = `{done}`)."
        )


def commit(season):
    conn = databaseConnector.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("INSERT IGNORE INTO wipe_control (id) VALUES (1)")
        # Only raise the request for a boundary we have not already cleared.
        cur.execute(
            "UPDATE wipe_control SET request_season = %s, requested_at = %s "
            "WHERE id = 1 AND %s > done_season",
            (season, int(time.time() * 1000), season),
        )
        affected = cur.rowcount
        cur.close()
        conn.commit()
    finally:
        conn.close()
    if affected:
        _summary(f"**Season wipe requested:** `request_season = {season}`.")
    else:
        _summary(
            f"Season wipe: no-op — season `{season}` already cleared "
            f"(`done_season` >= `{season}`)."
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--detect", action="store_true", help="decide if a wipe is needed")
    group.add_argument("--commit", action="store_true", help="raise the wipe request flag")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="season id to request (default: blizzard_season_id from seasonInfo.json)",
    )
    parser.add_argument(
        "--expect-season",
        type=int,
        default=None,
        help="fail unless the resolved season equals this (guards against the "
             "boundary moving while the manual approval was pending)",
    )
    args = parser.parse_args()

    season = args.season if args.season is not None else current_season_id()

    if args.expect_season is not None and season != args.expect_season:
        # Fail loudly: the approval gate can hold for days, and silently
        # committing the stale value would mark the wrong boundary cleared and
        # permanently block the real one.
        print(
            f"ERROR: season drifted since detection — expected {args.expect_season}, "
            f"seasonInfo.json now says {season}. Re-run detection instead of "
            f"committing a stale boundary.",
            file=sys.stderr,
        )
        _summary(
            f"**Season wipe aborted.** Boundary drifted while the approval was "
            f"pending (detected `{args.expect_season}`, now `{season}`)."
        )
        sys.exit(1)

    _init_pool()

    if args.detect:
        detect(season)
    else:
        commit(season)


if __name__ == "__main__":
    main()
