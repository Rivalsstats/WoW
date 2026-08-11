"""Build data/static/raids.json: the current expansion's raids and their bosses,
so item loot can be filtered by raid / boss the same way it is by dungeon.

Raids are discovered straight from the Raidbots loot dump, NOT from Raider.IO.
Raider.IO's raiding static-data exists, but (unlike its dungeon data) its raid
`id` is a Raider.IO id that does NOT equal the Blizzard journal instance id, so it
never joins to the loot. The Raidbots ``sources[].instanceId`` on each item IS the
Blizzard journal instance id (e.g. Sporefall = 1305), and ``sources[].encounterId``
is the journal encounter id, so everything we need is in equippable-items.json:

  1. expansion_id = current client major version - 1 (same derivation as
     fetchDungeonData.py); items carry that value in their ``expansion`` field.
  2. Candidate raid instances = positive ``instanceId``s from current-expansion
     items that are not current M+ dungeons.
  3. Each candidate is confirmed + labelled via Blizzard's journal-instance API
     (``category.type == "RAID"``, plus its encounters = bosses and tile media).

Output shape mirrors dungeons.json so generateItemPages.py can build a raids_map
the same way it builds dungeons_map:

    {
      "1305": {
        "name": {"en_US": "Sporefall"},
        "slug": "sporefall",
        "icon": "raid_tile.jpg",
        "order": 0,
        "bosses": { "2711": {"name": {"en_US": "Rotmire"}, "slug": "rotmire"} }
      }
    }
"""
import os
import re
import json
import time
import requests
from collections import defaultdict
from aggregateData import get_access_token

# config
CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]
API_BASE = "https://us.api.blizzard.com"
NAMESPACE_STATIC = "static-us"
LOCALE = "en_US"
ICON_DIR = "data/icons"
LOOKUP_DIR = "data/static"


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "unknown"


def en(val):
    """Blizzard static endpoints return localized strings as plain strings when a
    locale is requested, but a few return the full {locale: value} dict. Normalise
    to a plain string either way."""
    if isinstance(val, dict):
        return val.get("en_US") or next(iter(val.values()), None)
    return val


def bliz_get(url, token, allow_missing=False):
    params = {"namespace": NAMESPACE_STATIC, "locale": LOCALE}
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 429:
        retry = int(resp.headers.get("Retry-After", 1))
        time.sleep(retry)
        resp = requests.get(url, headers=headers, params=params)
    if allow_missing and resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def fetch_icon(iid, token):
    """Download the raid's tile art into ICON_DIR (same convention as dungeons).
    Returns the stored filename, or None if the raid has no tile / the fetch failed
    (missing art is not fatal — the frontend falls back to a generic icon)."""
    media = bliz_get(f"{API_BASE}/data/wow/media/journal-instance/{iid}", token, allow_missing=True)
    if not media:
        print(f"    Warning: no media for journal instance {iid}")
        return None
    tile = next((a for a in media.get("assets", []) if a.get("key") == "tile"), None)
    if not tile or not tile.get("value"):
        return None
    icon_url = tile["value"]
    icon_filename = icon_url.rsplit("/", 1)[-1]
    try:
        os.makedirs(ICON_DIR, exist_ok=True)
        img = requests.get(icon_url)
        img.raise_for_status()
        with open(os.path.join(ICON_DIR, icon_filename), "wb") as fh:
            fh.write(img.content)
    except requests.RequestException as e:
        print(f"    Warning: failed to fetch raid icon for {iid}: {e}")
        return None
    return icon_filename


def main():
    # Current expansion id, derived from the live client build the same way
    # fetchDungeonData.py does (major client version - 1). Items in
    # equippable-items.json carry this value in their "expansion" field.
    meta = requests.get("https://www.raidbots.com/static/data/live/metadata.json")
    meta.raise_for_status()
    wow_build = meta.json().get("wowBuild")            # e.g. "12.1.0.68914"
    expansion_id = int(wow_build.split(".", 1)[0]) - 1
    print(f"Derived expansion_id = {expansion_id} (wowBuild {wow_build})")

    with open(os.path.join(LOOKUP_DIR, "dungeons.json"), encoding="utf-8") as f:
        dungeons = json.load(f)
    dungeon_jiis = {
        int(d["journal_instance_id"])
        for d in dungeons.values()
        if d.get("journal_instance_id") is not None
    }

    with open(os.path.join(LOOKUP_DIR, "equippable-items.json"), encoding="utf-8") as f:
        items = json.load(f)

    # Candidate raid instances = positive source instanceIds from current-expansion
    # items that are not current dungeons. Track the boss encounter ids the loot
    # references per instance so we can reconcile them with the Journal list.
    loot_encounters = defaultdict(set)   # instanceId -> {encounterId}
    for it in items:
        if it.get("expansion") != expansion_id:
            continue
        for src in it.get("sources") or []:
            inst = src.get("instanceId")
            if isinstance(inst, int) and inst > 0 and inst not in dungeon_jiis:
                loot_encounters[inst]  # ensure the instance is registered
                enc = src.get("encounterId")
                if isinstance(enc, int) and enc > 0:
                    loot_encounters[inst].add(enc)
    print(f"{len(loot_encounters)} candidate raid instance id(s): {sorted(loot_encounters)}")

    token = get_access_token(CLIENT_ID, CLIENT_SECRET)

    out = {}
    order = 0
    for iid in sorted(loot_encounters):
        inst = bliz_get(f"{API_BASE}/data/wow/journal-instance/{iid}", token, allow_missing=True)
        if inst is None:
            print(f"  instance {iid}: no Journal entry (404), skipping")
            continue
        category = (inst.get("category") or {}).get("type")
        if category != "RAID":
            print(f"  instance {iid}: category {category!r}, not a raid, skipping")
            continue
        name = en(inst.get("name")) or f"Raid {iid}"
        print(f"  raid {iid}: {name}")

        # Bosses from the Journal encounter list (journal encounter ids match the
        # Raidbots sources[].encounterId used for the loot join). Reconcile with the
        # loot: add any loot-observed encounter id the Journal did not list so the
        # boss is still filterable (fail loudly, never silently drop it).
        journal_encs = {
            int(e["id"]): en(e.get("name"))
            for e in inst.get("encounters", []) or []
            if e.get("id") is not None
        }
        bosses = {}
        for enc_id, enc_name in journal_encs.items():
            ename = enc_name or f"Boss {enc_id}"
            bosses[str(enc_id)] = {"name": {"en_US": ename}, "slug": slugify(ename)}
        for enc_id in sorted(loot_encounters[iid]):
            if enc_id not in journal_encs:
                print(f"    WARNING: loot boss encounterId {enc_id} not in Journal "
                      f"encounters for raid {iid} ('{name}'); using fallback label")
                bosses[str(enc_id)] = {"name": {"en_US": f"Boss {enc_id}"}, "slug": f"boss-{enc_id}"}

        out[str(iid)] = {
            "name": {"en_US": name},
            "slug": slugify(name),
            "icon": fetch_icon(iid, token),
            "order": order,
            "bosses": bosses,
        }
        order += 1

    os.makedirs(LOOKUP_DIR, exist_ok=True)
    print(f"Output is {len(out)} raid(s) long")
    if out:
        with open(os.path.join(LOOKUP_DIR, "raids.json"), "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print("raids.json written")
    else:
        print("No raids discovered; raids.json left unchanged")


if __name__ == "__main__":
    main()
