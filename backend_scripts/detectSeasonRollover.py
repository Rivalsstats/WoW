#!/usr/bin/env python3
"""Detect whether a WoW season rollover is imminent, for the pre-flip archive job.

A rollover is "imminent" when Blizzard's current-season id has already moved past
the season our site still describes — i.e. the next `getStaticData` run will flip
`data/static/seasonInfo.json`. Because that flip also overwrites the static
reference files (equippable-items, dungeon/spell metadata), the outgoing season
must be archived BEFORE it runs; this detector is what gates that archive build.

Keyed off the US region only (by design): the US reset is the earliest each
week, so Blizzard's US `current_season` flips exactly when the outgoing season
ends in the US — which is also the "season ended" marker the wipe gate uses (the
archive stamps season_ends_utc = ends.us). This keeps the detector, the archive
timestamp and the gate all on the same clock, so a Wednesday-morning archive is
always stamped after the US season end. Trailing data from later-resetting
regions (EU/KR/TW) is intentionally not waited for — it is a non-issue for a cold
archive.

The tiny Blizzard OAuth helpers are duplicated here rather than imported from
fetchSeasonAndPeriodInfo because importing that module initialises a DB
connection pool at import time — and this detector must run with only Blizzard
credentials, no DB access.

Emits `is_rollover`, `season_id` (the outgoing season, from seasonInfo.json) and
`blizzard_season_id` (the incoming US season, from Blizzard) to $GITHUB_OUTPUT
when running in Actions, and always to stdout. Fails loudly on any API/file error
rather than silently reporting "no rollover".
"""
import json
import os
import sys

import requests

# US only, by design (see module docstring): the earliest reset, and the region
# the wipe gate's "season ended" check is keyed to.
REGION = "us"
SEASON_INDEX_URL = (
    "https://{region}.api.blizzard.com/data/wow/mythic-keystone/season/index"
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEASON_INFO_JSON = os.path.join(REPO_ROOT, "data", "static", "seasonInfo.json")

CLIENT_ID = os.getenv("BLIZ_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLIZ_CLIENT_SECRET")


def get_access_token():
    resp = requests.post(
        "https://oauth.battle.net/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def blizzard_get(url, params=None, token=None):
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
    resp.raise_for_status()
    return resp.json()


def stored_season_id():
    with open(SEASON_INFO_JSON, "r", encoding="utf-8") as f:
        info = json.load(f)
    return int(info["blizzard_season_id"])


def us_blizzard_season_id(token):
    data = blizzard_get(
        SEASON_INDEX_URL.format(region=REGION),
        params={"namespace": f"dynamic-{REGION}", "locale": "en_US"},
        token=token,
    )
    season_id = int(data["current_season"]["id"])
    if season_id <= 0:
        raise RuntimeError("Blizzard returned no US current-season id.")
    return season_id


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


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: BLIZ_CLIENT_ID / BLIZ_CLIENT_SECRET are not set.", file=sys.stderr)
        sys.exit(1)

    stored = stored_season_id()
    token = get_access_token()
    blizzard = us_blizzard_season_id(token)

    is_rollover = blizzard > stored
    _emit(
        is_rollover="true" if is_rollover else "false",
        season_id=stored,
        blizzard_season_id=blizzard,
    )
    if is_rollover:
        _summary(
            f"**Season rollover imminent.** Blizzard US current season `{blizzard}` is "
            f"ahead of our stored season `{stored}` — archiving season `{stored}` "
            f"before `getStaticData` overwrites its static files."
        )
    else:
        _summary(
            f"No rollover: stored season `{stored}` still matches Blizzard US current "
            f"season `{blizzard}`. Proceeding with the normal static refresh."
        )


if __name__ == "__main__":
    main()
