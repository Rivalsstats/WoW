#!/usr/bin/env python3
import asyncio
import json
import os
import threading
import queue as threading_queue
import concurrent.futures
from datetime import datetime, timezone
from collections import defaultdict
from contextlib import closing
from typing import Any
import aiohttp
from aiohttp import ClientError, BasicAuth
from aiolimiter import AsyncLimiter

import databaseConnector 

# ---------------- Config ----------------
API_URL = "https://raider.io/api/v1/mythic-plus/runs"
RAIDER_RUN_DETAILS_URL = "https://raider.io/api/v1/mythic-plus/run-details"
KEYSTONE_ROUTE_URL = "https://keystone.guru/api/v1/route/{}"
REGION = "world"

DUNGEONS_JSON = os.path.join("data", "static", "dungeons.json")
SPECS_JSON = os.path.join("data", "static", "specs.json")
SEASON_INFO_JSON = os.path.join("data", "static", "seasonInfo.json")

# required env vars
API_KEY = os.getenv("RAIDERIO_API_KEY")
DB_HOST = os.environ.get("DATABASE_HOST")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASS = os.environ.get("DATABASE_PASSWORD")
DB_NAME = os.environ.get("DATABASE_NAME")
DB_PORT = os.environ.get("DATABASE_PORT")
KEYSTONE_USER = os.environ.get("KEYSTONE_GURU_USER")
KEYSTONE_PW = os.environ.get("KEYSTONE_GURU_PW")

if not API_KEY:
    raise EnvironmentError("RAIDERIO_API_KEY env var is required")
if not all([DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT]):
    raise EnvironmentError(
        "DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME env vars are required"
    )
if not (KEYSTONE_USER and KEYSTONE_PW):
    raise EnvironmentError(
        "KEYSTONE_GURU_USER and KEYSTONE_GURU_PW env vars are required"
    )

# rate limit configuration (overridable by env)
RAIDER_RATE_MAX = int(os.getenv("RAIDER_RATE_MAX", "900"))
RAIDER_RATE_PERIOD = int(os.getenv("RAIDER_RATE_PERIOD", "60"))
KEYSTONE_RATE_MAX = int(os.getenv("KEYSTONE_RATE_MAX", "100"))
KEYSTONE_RATE_PERIOD = int(os.getenv("KEYSTONE_RATE_PERIOD", "60"))

RAIDER_RATE_LIMIT = AsyncLimiter(
    max_rate=RAIDER_RATE_MAX, time_period=RAIDER_RATE_PERIOD
)
KEYSTONE_RATE_LIMIT = AsyncLimiter(
    max_rate=KEYSTONE_RATE_MAX, time_period=KEYSTONE_RATE_PERIOD
)

# initialize DB pool (use small pool; the worker will take a single connection)
databaseConnector.init_connection_pool(
    DB_HOST,
    DB_USER,
    DB_PASS,
    DB_NAME,
    DB_PORT,
    1,
)
CURRENT_SEASON = ""
run_details_cache: dict[int, dict] = {}

# ---------------- Utilities (top-level) ----------------


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_params(dungeon_slug: str, page: int) -> dict:
    return {
        "region": REGION,
        "dungeon": dungeon_slug,
        "page": page,
        "access_key": API_KEY,
    }


async def fetch_page(
    session: aiohttp.ClientSession, dungeon_slug: str, page: int
) -> dict:
    params = build_params(dungeon_slug, page)
    await RAIDER_RATE_LIMIT.acquire()
    try:
        async with session.get(API_URL, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()
    except ClientError as e:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] ERROR fetching page {page} for dungeon '{dungeon_slug}': {e}"
        )
        return {"rankings": [], "params": {}}


async def fetch_run_details(
    session: aiohttp.ClientSession, run_id: int, season: str
) -> dict:
    """
    Fetch reduced run details from raider.io (rate-limited). Caches in-memory per run_id.
    """
    global run_details_cache
    if run_id in run_details_cache:
        return run_details_cache[run_id]

    params = {"id": run_id, "season": season}
    attempt = 0
    while attempt < 5:
        await RAIDER_RATE_LIMIT.acquire()
        try:
            async with session.get(RAIDER_RUN_DETAILS_URL, params=params) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else (2**attempt)
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

                completed_at = full.get("completed_at")
                ts = None
                if completed_at:
                    try:
                        ts = int(
                            datetime.fromisoformat(
                                completed_at.replace("Z", "+00:00")
                            ).timestamp()
                        )
                    except Exception:
                        ts = None

                reduced = {
                    "route_key": full.get("logged_details", {}).get("route_key"),
                    "mythic_level": full.get("mythic_level"),
                    "dungeon_id": full.get("dungeon", {}).get("map_challenge_mode_id"),
                    "roster_specs": roster_specs,
                    "duration": full.get("clear_time_ms"),
                    "timestamp": ts,
                    "keystone_run_id": full.get("keystone_run_id"),
                    "completed_at": completed_at,
                    "full_roster": full.get("roster", []),
                    "raw": full,
                }
                run_details_cache[run_id] = reduced
                return reduced
        except ClientError as e:
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] ERROR fetch_run_details {run_id}: {e}"
            )
            return {}
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] FAILED fetching run details {run_id} after {attempt} retries"
    )
    return {}


async def collect_for_dungeon(
    session: aiohttp.ClientSession, dungeon_slug: str, spec_ids: list[int]
) -> dict:
    """
    Paginate runs for a dungeon and gather keystone_run_ids per spec.
    """
    global CURRENT_SEASON
    found = {spec: set() for spec in spec_ids}
    page = 1
    while page < 500:
        data = await fetch_page(session, dungeon_slug, page)
        rankings = data.get("rankings", [])
        if not rankings:
            break
        if CURRENT_SEASON == "":
            CURRENT_SEASON = data.get("params", {}).get("season", "") or CURRENT_SEASON
            if CURRENT_SEASON:
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] Detected current season: {CURRENT_SEASON}"
                )

        for entry in rankings:
            run = entry.get("run")
            if not run or run.get("logged_run_id") is None:
                continue
            keystone_id = run.get("keystone_run_id")
            for member in run.get("roster", []):
                char_spec = member.get("character", {}).get("spec", {}).get("id")
                if char_spec in found and len(found[char_spec]) < 100:
                    found[char_spec].add(keystone_id)
        page += 1

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Completed dungeon {dungeon_slug} collection."
    )
    return {str(spec): sorted(list(runs)) for spec, runs in found.items() if runs}


async def fetch_keystone_route(session: aiohttp.ClientSession, route_key: str) -> dict:
    """
    Fetch keystone.guru route data with Basic Auth (rate-limited).
    """
    url = KEYSTONE_ROUTE_URL.format(route_key)
    await KEYSTONE_RATE_LIMIT.acquire()
    auth = BasicAuth(login=KEYSTONE_USER, password=KEYSTONE_PW)
    try:
        async with session.get(url, auth=auth) as resp:
            resp.raise_for_status()
            j = await resp.json()
            return j.get("data", {}) or {}
    except ClientError as e:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] ERROR fetching keystone.guru route {route_key}: {e}"
        )
        return {}


def aggregate_enemies_occurrence(pull: dict) -> dict:
    """
    Count occurrences of npcId inside a keystone.guru pull's enemies array.
    Returns mapping npcId -> count
    """
    counts = defaultdict(int)
    for e in pull.get("enemies", []):
        npc = e.get("npcId")
        if npc is None:
            continue
        counts[int(npc)] += 1
    return counts


# ---------------- DB worker (single thread, top-level) ----------------
# Job type: ("insert_route", raider_reduced, keystone_route, future)


def db_worker_thread(job_queue: threading_queue.Queue):
    """
    Dedicated DB worker thread that uses ONE connection from the pool.
    All SQL interaction must go through databaseConnector.* functions.
    """
    try:
        conn = databaseConnector.get_connection()
    except Exception as e:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] DB worker failed to get connection: {e}"
        )
        # Fail pending futures
        while True:
            try:
                job = job_queue.get(block=False)
            except Exception:
                break
            if job and isinstance(job[-1], concurrent.futures.Future):
                job[-1].set_result(False)
        return

    cursor = conn.cursor()
    while True:
        job = job_queue.get()
        if job is None:
            break  # shutdown sentinel
        try:
            if job[0] == "insert_route":
                _, raider_reduced, keystone_route, fut = job
                route_key = keystone_route.get("publicKey") or raider_reduced.get(
                    "route_key"
                )
                try:
                    # Insert route_data using keystone info (mapping_version, enemyForces) + raider reduced fields
                    rio_run_id = int(raider_reduced.get("keystone_run_id") or 0)
                    mapping_version = int(keystone_route.get("mappingVersion") or 0)
                    enemy_forces = int(keystone_route.get("enemyForces") or 0)
                    timestamp = int(raider_reduced.get("timestamp") or 0)
                    keystone_level = int(raider_reduced.get("mythic_level") or 0)
                    duration = int(raider_reduced.get("duration") or 0)
                    dungeon_id = raider_reduced.get("dungeon_id")

                    databaseConnector.insert_route_data(
                        conn,
                        cursor,
                        rio_run_id,
                        mapping_version,
                        enemy_forces,
                        timestamp,
                        keystone_level,
                        duration,
                        dungeon_id,
                        route_key,
                    )
                    # Insert specs
                    specs = set()
                    for s in raider_reduced.get("roster_specs", []):
                        try:
                            specs.add(int(s))
                        except Exception:
                            pass
                    for spec_id in specs:
                        try:
                            databaseConnector.insert_route_spec(
                                conn, cursor, route_key, spec_id
                            )
                        except Exception as e:
                            # ignore duplicate/minor errors
                            print(
                                f"[{datetime.now(timezone.utc).isoformat()}] insert_route_spec ignored: {e}"
                            )

                    # Insert pulls and aggregated enemies/spells
                    pulls = keystone_route.get("pulls", []) or []
                    for pull in pulls:
                        try:
                            new_pull_id = databaseConnector.insert_route_pull(
                                conn, cursor, route_key
                            )
                        except Exception as e:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            fut.set_result(False)
                            print(
                                f"[{datetime.now(timezone.utc).isoformat()}] insert_route_pull failed for {route_key}: {e}"
                            )
                            break

                        counts = aggregate_enemies_occurrence(pull)
                        for npc_id, cnt in counts.items():
                            try:
                                databaseConnector.insert_pull_enemies(
                                    conn,
                                    cursor,
                                    route_key,
                                    new_pull_id,
                                    int(npc_id),
                                    int(cnt),
                                )
                            except Exception as e:
                                # ignore duplicate/integrity errors
                                print(
                                    f"[{datetime.now(timezone.utc).isoformat()}] insert_pull_enemies ignored: {e}"
                                )

                        spells = set(pull.get("spells") or [])
                        for spell in spells:
                            try:
                                databaseConnector.insert_pull_spells(
                                    conn, cursor, route_key, new_pull_id, int(spell)
                                )
                            except Exception as e:
                                print(
                                    f"[{datetime.now(timezone.utc).isoformat()}] insert_pull_spells ignored: {e}"
                                )
                    else:
                        # commit when all inserts succeeded
                        try:
                            conn.commit()
                        except Exception:
                            pass
                        fut.set_result(True)
                        print(
                            f"[{datetime.now(timezone.utc).isoformat()}] Inserted route {route_key} (rio_run_id={rio_run_id})."
                        )

                except Exception as e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    fut.set_result(False)
                    print(
                        f"[{datetime.now(timezone.utc).isoformat()}] DB insert_route failed for {route_key}: {e}"
                    )

            else:
                # unknown job
                if isinstance(job[-1], concurrent.futures.Future):
                    job[-1].set_result(False)

        finally:
            job_queue.task_done()

    # cleanup
    try:
        cursor.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


# ---------------- Async orchestration (top-level) ----------------

DB_JOB_QUEUE = threading_queue.Queue()
DB_WORKER_THREAD = threading.Thread(
    target=db_worker_thread, args=(DB_JOB_QUEUE,), daemon=True
)
DB_WORKER_THREAD.start()


async def enqueue_insert_route(raider_reduced: dict, keystone_route: dict) -> bool:
    fut = concurrent.futures.Future()
    DB_JOB_QUEUE.put(("insert_route", raider_reduced, keystone_route, fut))
    return await asyncio.wrap_future(fut)


async def process_run_if_needed(session: aiohttp.ClientSession, run_id: int) -> bool:
    """
    Flow:
      1) fetch raider run-details
      2) if no route_key -> skip
      3) fetch keystone.guru route
      4) check keystone.enemyForces >= keystone.enemyForcesRequired -> only then enqueue DB insert
    """
    raider = await fetch_run_details(session, run_id, CURRENT_SEASON)
    if not raider:
        return False
    
    ts = raider.get("timestamp")
    if not ts:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Discarding run {run_id}: missing timestamp"
        )
        return False

    now_ts = int(datetime.now(timezone.utc).timestamp())
    four_weeks_seconds = 28 * 24 * 60 * 60  # 28 days
    cutoff_ts = now_ts - four_weeks_seconds

    if int(ts) < cutoff_ts:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Discarding run {run_id}: timestamp {ts} is older than 4 weeks (cutoff {cutoff_ts})"
        )
        return False

    route_key = raider.get("route_key")
    if not route_key:
        return False

    # fetch keystone route (only when run has a route_key)
    keystone = await fetch_keystone_route(session, route_key)
    if not keystone:
        return False

    # validate enemy forces threshold: only insert when actual >= required
    ef_actual = keystone.get("enemyForces")
    ef_required = keystone.get("enemyForcesRequired")
    if ef_actual is None or ef_required is None:
        # missing data -> discard as invalid
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Discarding route {route_key}: missing enemyForces or enemyForcesRequired"
        )
        return False

    try:
        if int(ef_actual) < int(ef_required):
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] Discarding route {route_key}: enemyForces {ef_actual} < required {ef_required}"
            )
            return False
    except Exception:
        # non-int values -> discard
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Discarding route {route_key}: invalid enemyForces values"
        )
        return False

    # passed validation -> enqueue DB insert (single job)
    inserted = await enqueue_insert_route(raider, keystone)
    return bool(inserted)


async def process_single_with_semaphore(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, run_id: int
) -> bool:
    async with semaphore:
        return await process_run_if_needed(session, run_id)


async def process_runs_concurrently(
    session: aiohttp.ClientSession, run_ids: list[int], concurrency: int = 10
) -> int:
    semaphore = asyncio.Semaphore(concurrency)
    tasks = [process_single_with_semaphore(session, semaphore, rid) for rid in run_ids]
    results = await asyncio.gather(*tasks)
    return sum(1 for r in results if r)

async def main():
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Loading dungeon and spec data..."
    )
    dungeons = load_json(DUNGEONS_JSON)
    specs = load_json(SPECS_JSON)
    dungeon_slugs = [info["slug"] for info in dungeons.values()]
    spec_ids = [int(k) for k in specs.keys()]

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Starting data collection for {len(dungeon_slugs)} dungeons..."
    )
    async with aiohttp.ClientSession() as session:
        # collect keystone_run_id lists per dungeon/spec
        tasks = [collect_for_dungeon(session, slug, spec_ids) for slug in dungeon_slugs]
        results = await asyncio.gather(*tasks)
        output = {slug: res for slug, res in zip(dungeon_slugs, results)}
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Collected run id lists for {len(output)} dungeons."
        )

        # flatten run ids
        run_ids_set = set()
        for dungeon_specs in output.values():
            for runs in dungeon_specs.values():
                run_ids_set.update(runs)
        run_ids_list = sorted(run_ids_set)
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Found {len(run_ids_list)} unique runs to consider."
        )

        inserted_count = await process_runs_concurrently(
            session, run_ids_list, concurrency=10
        )
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Inserted {inserted_count} new valid routes into the DB."
        )

    # persist season info
    with open(SEASON_INFO_JSON, "w", encoding="utf-8") as f:
        json.dump({"current_season": CURRENT_SEASON}, f, indent=2)

    # shutdown DB worker cleanly
    DB_JOB_QUEUE.put(None)
    DB_WORKER_THREAD.join(timeout=5)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Done.")


if __name__ == "__main__":
    asyncio.run(main())
