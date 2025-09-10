
import asyncio
import json
import os
from datetime import datetime, timezone
import aiohttp
from aiolimiter import AsyncLimiter
from aiohttp import ClientError
from typing import Any


API_URL = "https://raider.io/api/v1/mythic-plus/runs"
REGION = "world"
RATE_LIMIT = AsyncLimiter(max_rate=280, time_period=60)

DUNGEONS_JSON = os.path.join("data","static","dungeons.json")
SPECS_JSON    = os.path.join("data","static","specs.json")
OUTPUT_JSON = os.path.join("data","static","routeData.json")
COMPS_JSON = os.path.join("data","static","compRoutes.json")
CURRENT_SEASON = ""
SEASON_INFO_JSON = os.path.join("data","static","seasonInfo.json")
run_details_cache: dict[int, dict] = {}

API_KEY = os.getenv("RAIDERIO_API_KEY")
if not API_KEY:
    raise EnvironmentError("RAIDERIO_API_KEY env var is required")

async def fetch_run_details(session, run_id: int, season: str) -> dict:
    """
    Respects RATE_LIMIT.
    Retries on 429 using the Retry-After header (up to 5 attempts).
    """
    global run_details_cache
    if run_id in run_details_cache:
        return run_details_cache[run_id]
    params = {"id": run_id, "season": season}
    attempt = 0

    while attempt < 5:
        await RATE_LIMIT.acquire()  # explicitly wait for a token
        try:
            async with session.get(
                "https://raider.io/api/v1/mythic-plus/run-details",
                params=params
            ) as resp:
                if resp.status == 429:
                    # read Retry-After (in seconds) or default to exponential backoff
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else 2 ** attempt
                    await asyncio.sleep(wait)
                    attempt += 1
                    continue
                
                resp.raise_for_status()
                full = await resp.json()
                roster_specs = sorted(
                    member.get("character", {}).get("spec", {}).get("id")
                    for member in full.get("roster", [])
                    if member.get("character", {}).get("spec", {}).get("id") is not None
                )
                reduced = {
                    "route_key": full.get("logged_details", {}).get("route_key"),
                    "mythic_level": full.get("mythic_level"),
                    "dungeon_id": full.get("dungeon", {}).get("map_challenge_mode_id"),
                    "roster_specs": roster_specs,
                    "duration": full.get("clear_time_ms"),
                    'timestamp':  int(datetime.fromisoformat(full.get("completed_at")).timestamp()),
                }
                run_details_cache[run_id] = reduced
                return reduced
        except ClientError as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR {run_id}: {e}")
            return {}
    print(f"[{datetime.now(timezone.utc).isoformat()}] FAILED {run_id} after {attempt} retries")
    return {}

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def build_params(dungeon_slug, page):
    return {
        "region": REGION,
        "dungeon": dungeon_slug,
        "page": page,
        "access_key": API_KEY,
    }

async def fetch_page(session, dungeon_slug, page):
    params = build_params(dungeon_slug, page)
    await RATE_LIMIT.acquire()
    try:       
        async with session.get(API_URL, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()
    except ClientError as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR fetching page {page} for dungeon “{dungeon_slug}”: {e}")
        # return an empty page so pagination will stop gracefully
        return {"rankings": [], "params": {}}

async def collect_for_dungeon(session, dungeon_slug, spec_ids):
    """
    Paginate through all runs for a dungeon, collecting all keystone_run_ids for each spec_id.
    Continues until no more pages.
    Prints progress including how many unique runs collected per spec.
    """
    global CURRENT_SEASON
    found = {spec: set() for spec in spec_ids}
    page = 1
    while page < 100:  # current raider.io API max is 100 pages
        data = await fetch_page(session, dungeon_slug, page)
        rankings = data.get("rankings", [])
        if not rankings:
            break
        for entry in rankings:
            run = entry.get("run")
            if not run or run.get("logged_run_id") is None:
                continue
            keystone_id = run["keystone_run_id"]
            if CURRENT_SEASON == "":
                CURRENT_SEASON = data.get("params", {}).get("season", "")
                print(f"[{datetime.now(timezone.utc).isoformat()}] Detected current season: {CURRENT_SEASON}")
            for member in run.get("roster", []):
                char_spec = member.get("character", {}).get("spec", {}).get("id")
                if char_spec in found and len(found[char_spec]) < 100:
                    found[char_spec].add(keystone_id)
        page += 1
    missing = sum(1 for s, runs in found.items() if not runs)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Completed dungeon {dungeon_slug} collection. Specs remaining: {missing}")
    # convert sets to sorted lists for JSON
    return {str(spec): sorted(list(runs)) for spec, runs in found.items() if runs}

async def main():
    # Load slugs and spec IDs
    print(f"[{datetime.now(timezone.utc).isoformat()}] Loading dungeon and spec data...")
    dungeons = load_json(DUNGEONS_JSON)
    specs    = load_json(SPECS_JSON)
    dungeon_slugs = [info['slug'] for info in dungeons.values()]
    spec_ids      = [int(k) for k in specs.keys()]
    comp_routes: dict[tuple[int,...], dict[str, Any]] = {}

    output = {}
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting data collection for {len(dungeon_slugs)} dungeons...")
    async with aiohttp.ClientSession() as session:
        tasks = [collect_for_dungeon(session, slug, spec_ids) for slug in dungeon_slugs]
        results = await asyncio.gather(*tasks)
        for slug, res in zip(dungeon_slugs, results):
            output[slug] = res

        print(f"[{datetime.now(timezone.utc).isoformat()}] Collected data for {len(output)} dungeons.")
        spec_to_run_ids: dict[int, set[int]] = {}
        for dungeon_specs in output.values():
            for spec_str, runs in dungeon_specs.items():
                sid = int(spec_str)
                spec_to_run_ids.setdefault(sid, set()).update(runs)
        print(f"[{datetime.now(timezone.utc).isoformat()}] Found {len(spec_to_run_ids)} specs with runs.")
        spec_routes: dict[int, dict[str, list]] = {
            sid: {"dungeons": []}
            for sid in spec_ids
        }

        for dungeon_slug, dungeon_specs in output.items():
            for spec_str, run_ids in dungeon_specs.items():
                sid = int(spec_str)
                # fetch details for this spec-in-this-dungeon
                coros = [
                    fetch_run_details(session, rid, CURRENT_SEASON)
                    for rid in run_ids
                ]
                details_list = await asyncio.gather(*coros)

                # aggregate per-route
                grouped: dict[str, list[tuple[int,int,tuple[int,...],int,int]]] = {}
                for details, rid in zip(details_list, run_ids):
                    if not details:
                        continue
                    key = details.get("route_key")
                    lvl = details.get("mythic_level")
                    if not key:
                        continue

                    comp        = tuple(details["roster_specs"])
                    duration    = details.get("duration")
                    timestamp   = details.get("timestamp")

                    # store a 5‑tuple: (level, run_id, comp, duration, timestamp)
                    grouped.setdefault(key, []).append((lvl, rid, comp, duration, timestamp))

                    existing = comp_routes.get(comp)
                    if not existing or lvl > existing["level"]:
                       comp_routes[comp] = {
                            "level":    lvl,
                            "dungeon":  dungeon_slug,
                            "route_key": key,
                            "run_id":   rid,
                            "duration": duration,
                            "timestamp": timestamp,
                        }

                # compute stats array
                routes_summary = []
                for route_key, entries in grouped.items():
                    levels     = [lvl              for lvl, *_ in entries]
                    run_ids    = [rid              for _, rid, *_ in entries]
                    comp_lists = [comp             for *_, comp, _, _ in entries]
                    durations  = [duration         for *_, duration, _ in entries]
                    timestamps = [timestamp        for *_, _, timestamp in entries]
                    max_idx    = levels.index(max(levels))
                    max_duration  = durations[max_idx]
                    max_timestamp = timestamps[max_idx]

                    routes_summary.append({
                        "route_key":       route_key,
                        "count":           len(entries),
                        "highest_key":     max(levels),
                        "keystone_run_ids":run_ids,
                        "comps":           comp_lists,
                        # two new fields:
                        "duration":        max_duration,
                        "timestamp":       max_timestamp,
                    })
                spec_routes[sid]["dungeons"].append({
                    "dungeon_id": dungeon_slug,
                    "routes": routes_summary
                })

        final_stats = spec_routes
    print("\n# Per-spec route stats:")
    with open(OUTPUT_JSON, "w") as f:
        json.dump(final_stats, f, indent=2)
    with open(COMPS_JSON, "w") as f:
        json.dump({",".join(map(str,k)):v for k,v in comp_routes.items()}, f, indent=2)
    with open(SEASON_INFO_JSON, "w") as f:
        json.dump({"current_season": CURRENT_SEASON}, f, indent=2)    
    print(f"[{datetime.now(timezone.utc).isoformat()}] Data collection complete. Total dungeons: {len(output)} for {CURRENT_SEASON}")

if __name__ == '__main__':
    asyncio.run(main())
