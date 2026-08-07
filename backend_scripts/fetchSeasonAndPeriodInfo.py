from contextlib import closing
import requests
import json
import os
import re
import csv
import io
from collections import Counter
from datetime import datetime, timezone
import databaseConnector

# List of Blizzard API regions to process
regions = ["us", "eu", "kr", "tw"]

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)

# Base template URLs
season_index_url = (
    "https://{region}.api.blizzard.com/data/wow/mythic-keystone/season/index"
)
season_details_url = (
    "https://{region}.api.blizzard.com/data/wow/mythic-keystone/season/{season_id}"
)
period_details_url = (
    "https://{region}.api.blizzard.com/data/wow/mythic-keystone/period/{period_id}"
)

CLIENT_ID = os.getenv("BLIZ_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLIZ_CLIENT_SECRET")
RAIDERIO_API_KEY = os.getenv("RAIDERIO_API_KEY")
CURRENT_EXPANSION_ID = 11  # MIDNIGHT

SEASON_INFO_JSON = os.path.join("data", "static", "seasonInfo.json")
# The season this file described before the most recent flip; see the write below.
SEASON_INFO_PREV_JSON = os.path.join("data", "static", "seasonInfo.prev.json")


# Obtain an access token
def get_access_token():
    auth_url = "https://oauth.battle.net/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(auth_url, data=data, auth=(CLIENT_ID, CLIENT_SECRET))
    response.raise_for_status()
    return response.json()["access_token"]


# Common function to perform GET requests with token
def blizzard_get(url, params=None, token=None):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def fetch_rio_season():
    rio_season_url = f"https://raider.io/api/v1/mythic-plus/static-data?expansion_id={CURRENT_EXPANSION_ID}"
    resp = requests.get(rio_season_url, {"access_key": RAIDERIO_API_KEY})
    resp.raise_for_status()
    return resp.json().get("seasons", [])


# wago.tools exposes Blizzard DB2 tables. ContentTuning carries the player level
# bracket (MaxLevelSquish) per piece of content; the current max level is the
# highest bracket value that appears as a real content bracket (ignoring the
# handful of >100 scaling outliers). This avoids hardcoding the cap each expansion.
CONTENT_TUNING_CSV = "https://wago.tools/db2/ContentTuning/csv"

# wago.tools also tracks every client build per product with its CDN push time.
# The earliest retail ("wow") build per X.Y.Z version lands a few days before
# the patch goes live, so it pins down the release week of each content patch.
WAGO_BUILDS_URL = "https://wago.tools/api/builds"

PATCHES_JSON = os.path.join("data", "static", "patches.json")


def fetch_patch_list():
    """Return one entry per retail X.Y.Z patch version with the timestamp of its
    earliest retail build (epoch ms, UTC), sorted by that timestamp."""
    resp = requests.get(
        WAGO_BUILDS_URL,
        headers={"User-Agent": "Mythistone-static-collector"},
        timeout=60,
    )
    resp.raise_for_status()
    builds = resp.json().get("wow", [])
    earliest = {}
    for build in builds:
        parts = build.get("version", "").split(".")
        if len(parts) < 4:
            continue
        version = ".".join(parts[:3])
        created = build.get("created_at")
        if not created:
            continue
        ts = int(
            datetime.strptime(created, "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        if version not in earliest or ts < earliest[version]["first_seen_ts"]:
            earliest[version] = {
                "version": version,
                "first_build": build["version"],
                "first_seen_ts": ts,
            }
    return sorted(earliest.values(), key=lambda p: p["first_seen_ts"])


def fetch_max_character_level(min_occurrences=10, ceiling=100):
    """Return the current player max level derived from wago.tools ContentTuning,
    or None if it can't be determined."""
    resp = requests.get(
        CONTENT_TUNING_CSV,
        headers={"User-Agent": "Mythistone-static-collector"},
        timeout=60,
    )
    resp.raise_for_status()
    counts = Counter()
    for row in csv.DictReader(io.StringIO(resp.text)):
        try:
            v = int(row.get("MaxLevelSquish") or 0)
        except (TypeError, ValueError):
            continue
        if 0 < v <= ceiling:
            counts[v] += 1
    brackets = [v for v, c in counts.items() if c >= min_occurrences]
    if brackets:
        return max(brackets)
    return max(counts) if counts else None


def main():
    token = get_access_token()
    all_regions_data = {}
    highest_season_id = 0
    for region in regions:
        print(f"Fetching data for region: {region}")
        namespace = f"dynamic-{region}"
        # Get current season index
        idx_resp = blizzard_get(
            season_index_url.format(region=region),
            params={"namespace": namespace, "locale": "en_US"},
            token=token,
        )
        season_id = idx_resp["current_season"]["id"]
        if season_id > highest_season_id:
            highest_season_id = season_id

        # Get season details to extract period IDs
        season_resp = blizzard_get(
            season_details_url.format(region=region, season_id=season_id),
            params={"namespace": namespace, "locale": "en_US"},
            token=token,
        )
        periods = season_resp.get("periods", [])
        season_start = season_resp["start_timestamp"]

        # For each period, fetch start and end timestamps
        region_periods = []
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()
            for p in periods:
                print(f"Processing period ID: {p['id']}")
                pid = p["id"]
                per_resp = blizzard_get(
                    period_details_url.format(region=region, period_id=pid),
                    params={"namespace": namespace, "locale": "en_US"},
                    token=token,
                )
                # Blizzard lists the period *preceding* the season start too
                # (e.g. period 1055 ends exactly at the season-17 start). That
                # pre-season week has no runs and would shift week numbering
                # off by one, so drop it here and from season_periods.
                if per_resp["end_timestamp"] <= season_start:
                    print(f"Skipping pre-season period {pid} for {region}")
                    continue
                region_periods.append(
                    {
                        "id": per_resp["id"],
                        "start_timestamp": per_resp["start_timestamp"],
                        "end_timestamp": per_resp["end_timestamp"],
                    }
                )
                databaseConnector.insert_season_periods(
                    conn,
                    cursor,
                    region,
                    pid,
                    per_resp["start_timestamp"],
                    per_resp["end_timestamp"],
                    season_id,
                )
            databaseConnector.commit_changes(conn)
        all_regions_data[region] = {"season_id": season_id, "periods": region_periods}

    season_info = fetch_rio_season()
    print(season_info)
    CURRENT_SEASON = None
    max_season_id = max(s.get("blizzard_season_id", 0) for s in season_info)
    if max_season_id >= highest_season_id:
        for season in season_info:
            print(season)
            if season.get("blizzard_season_id") == highest_season_id:
                CURRENT_SEASON = season
                break
    else:
        print(
            f"Warning: No season in Raider.IO data matches the current Blizzard season {highest_season_id}. Using {max_season_id} as a fallback."
        )
        for season in season_info:
            print(season)
            if season.get("blizzard_season_id") == max_season_id:
                CURRENT_SEASON = season
                break
    if not CURRENT_SEASON:
        raise ValueError(
            f"Could not find RaiderIO season matching Blizzard season ID {highest_season_id}. Is the expansion correct?"
        )
    # prefer Blizzard's season name (text inside parentheses) when available
    try:
        bliz_region = "us"
        bliz_namespace = f"dynamic-{bliz_region}"
        bliz_season = blizzard_get(
            season_details_url.format(region=bliz_region, season_id=highest_season_id),
            params={"namespace": bliz_namespace, "locale": "en_US"},
            token=token,
        )
        bliz_full_name = bliz_season.get("season_name", "") or ""
        m = re.search(r"\(([^)]+)\)", bliz_full_name)
        if m:
            extracted = m.group(1).strip()
        else:
            extracted = bliz_full_name.strip()

        # keep original raider.io structure but override 'name' for frontend display
        CURRENT_SEASON["blizzard_season_name"] = bliz_full_name
        CURRENT_SEASON["name"] = extracted
    except Exception as e:
        print(f"Failed to fetch/parse Blizzard season name: {e}")

    # current player max level (derived, not hardcoded) for downstream consumers
    # such as the SimulationCraft BiS collector.
    try:
        max_level = fetch_max_character_level()
        if max_level:
            CURRENT_SEASON["max_character_level"] = max_level
            print(f"Derived current max character level: {max_level}")
        else:
            print("Warning: could not derive max character level from ContentTuning")
    except Exception as e:
        print(f"Failed to derive max character level: {e}")

    # A season flip is destructive to the outgoing season's identity: name, slug
    # and end dates are simply overwritten. Keep the previous file so a final
    # snapshot of that season can still be rendered afterwards (MYTHISTONE_SEASON_INFO
    # -> commonUtils.load_season_info); that snapshot is what gates the DB wipe.
    if os.path.exists(SEASON_INFO_JSON):
        try:
            with open(SEASON_INFO_JSON, "r", encoding="utf-8") as f:
                previous = json.load(f)
        except Exception as e:
            raise RuntimeError(
                f"Could not read the existing {SEASON_INFO_JSON} before overwriting "
                f"it; refusing to lose the outgoing season's identity: {e}"
            )
        if previous.get("blizzard_season_id") != CURRENT_SEASON.get("blizzard_season_id"):
            with open(SEASON_INFO_PREV_JSON, "w", encoding="utf-8") as f:
                json.dump(previous, f, indent=2)
            print(
                f"Season flip {previous.get('blizzard_season_id')} -> "
                f"{CURRENT_SEASON.get('blizzard_season_id')}; wrote "
                f"{SEASON_INFO_PREV_JSON}"
            )

    # persist season info
    with open(SEASON_INFO_JSON, "w", encoding="utf-8") as f:
        json.dump(CURRENT_SEASON, f, indent=2)
    # Write to JSON file
    output_path = os.path.join("data", "static")
    os.makedirs(output_path, exist_ok=True)
    with open(os.path.join(output_path, "periods.json"), "w") as f:
        json.dump(all_regions_data, f, indent=2)

    print(f"Generated periods.json for regions: {', '.join(regions)}")

    # patch release list for the dashboard's patch annotations; written last so
    # a wago.tools failure fails the job without touching the files above
    patches = fetch_patch_list()
    with open(PATCHES_JSON, "w", encoding="utf-8") as f:
        json.dump(patches, f, indent=2)
    print(f"Generated patches.json with {len(patches)} patch versions")


if __name__ == "__main__":
    main()
