#!/usr/bin/env python3
"""Preflight for buildPages: does the target season have any runs yet?

During the pre-season gap (and immediately after a season wipe) the current
season has no `runs`, so the page generators crash on empty aggregates (e.g.
create_spec_scatter dereferencing a None highest_key). Rather than deploy a
broken/empty site, buildPages gates the whole build on this check and simply
skips when there is nothing to build — the live site + archive keep showing the
last good season until real new-season data arrives.

Emits `has_data=true|false` to $GITHUB_OUTPUT (and stdout). Fails loudly on DB
errors rather than silently reporting "no data".
"""
import argparse
import os
import sys

import databaseConnector


def _emit(**kv):
    line = " ".join(f"{k}={v}" for k, v in kv.items())
    print(line)
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={v}\n")


def _summary(text):
    print(text)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")


def has_runs(season):
    conn = databaseConnector.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM runs WHERE season = %s LIMIT 1", (season,))
        found = cur.fetchone() is not None
        cur.close()
        conn.commit()
    finally:
        conn.close()
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True,
                        help="Blizzard season id to check for runs")
    args = parser.parse_args()

    databaseConnector.init_connection_pool(
        os.environ["DATABASE_HOST"],
        os.environ["DATABASE_USER"],
        os.environ["DATABASE_PASSWORD"],
        os.environ["DATABASE_NAME"],
        os.environ["DATABASE_PORT"],
        pool_size=2,
    )

    found = has_runs(args.season)
    _emit(has_data="true" if found else "false")
    if found:
        _summary(f"Season `{args.season}` has runs — building normally.")
    else:
        _summary(
            f"Season `{args.season}` has no runs yet (pre-season gap or post-wipe). "
            f"Skipping the build; the live site + archive keep the last good season."
        )


if __name__ == "__main__":
    main()
