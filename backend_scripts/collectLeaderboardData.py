import os
import re
import json
import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
import time
import csv
from pathlib import Path
from aiohttp import (
    ClientSession,
    ClientTimeout,
    BasicAuth,
    ClientResponseError,
    ClientConnectionResetError,
)
from aiohttp_retry import RetryClient, ExponentialRetry
import aiohttp
from aiolimiter import AsyncLimiter
from collections import Counter, defaultdict
import argparse
import databaseConnector
import commonUtils
import traceback
from contextlib import closing, suppress
import shutil
from dotenv import load_dotenv
import stats
import discordHandler
import simcBis
from urllib.parse import quote_plus

# Load environment variables first as we need it for some of the configs
load_dotenv()


def getenv_clean(key, default=None):
    v = os.environ.get(key, default)
    if isinstance(v, str):
        return v.rstrip("\r\n")
    return v


# Queue settings
QUEUE_MAXSIZE = 1000
GHA_TIMEOUT = 60 * 60 * 24
HARD_TIMEOUT = GHA_TIMEOUT + 30 * 60  # force cancel after 30 minutes past GHA_TIMEOUT
cancel_event = asyncio.Event()
shutdown_event = asyncio.Event()
# Set when the process must restart to pick up state it only resolves at startup
# (season + active period). runner() winds the collector down so the container
# supervisor (`restart: always`) brings us back with fresh values.
restart_event = asyncio.Event()
MAX_GLOBAL_BACKOFF = 60.0
WORKERS_PER_REALM = int(getenv_clean("WORKERS_PER_REALM", "5"))
POLL_INTERVAL_SECONDS = int(getenv_clean("POLL_INTERVAL_SECONDS", "300"))
TOP_PLAYER_LOADOUTS_TARGET = int(getenv_clean("TOP_PLAYER_LOADOUTS_TARGET", "50"))
TOP_PLAYER_LOADOUTS_PAGE_LIMIT = int(getenv_clean("TOP_PLAYER_LOADOUTS_PAGE_LIMIT", "200"))
TOP_PLAYER_LOADOUTS_DAYS = int(getenv_clean("TOP_PLAYER_LOADOUTS_DAYS", "14"))
TOP_PLAYER_LOADOUTS_PAGE_SLEEP = float(getenv_clean("TOP_PLAYER_LOADOUTS_PAGE_SLEEP", "4"))

# queues
simple_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
advanced_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
database_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
route_db_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
GLOBAL_STATS = stats.StatsCollector(
    window_seconds=300,
    simple_queue=simple_queue,
    advanced_queue=advanced_queue,
    database_queue=database_queue,
    route_db_queue=route_db_queue,
)

# --- Season-rollover write gate (see database.sql wipe_control / ev_season_wipe) ---
# During a season wipe the DB is blanket-cleared; this gate lets the collector
# quiesce its writers so the clear runs against idle tables. The wipe_watch()
# coroutine polls wipe_control, pauses the gate while a wipe is pending, and acks
# collector_paused=1 once every in-flight write section has drained.
WIPE_POLL_SECONDS = float(getenv_clean("WIPE_POLL_SECONDS", "30"))
# Failsafe: never stay paused longer than this without the wipe completing, so a
# stuck or never-approved request can't halt collection forever. Must comfortably
# exceed a full nightly agg-pipeline run: the pipeline holds GET_LOCK('agg_pipeline'),
# which is exactly what keeps ev_season_wipe from firing, so a value shorter than
# the pipeline makes the failsafe trip on a *healthy* wipe.
WIPE_MAX_PAUSE_SECONDS = float(getenv_clean("WIPE_MAX_PAUSE_SECONDS", str(4 * 60 * 60)))
# After giving up on a request, wait this long before trying the handshake again.
# Without this the give-up is permanent for the process lifetime and the wipe can
# never happen — re-running the CI job doesn't help, because it re-raises the same
# request_season we already blacklisted.
WIPE_FAILSAFE_RETRY_SECONDS = float(
    getenv_clean("WIPE_FAILSAFE_RETRY_SECONDS", str(int(WIPE_MAX_PAUSE_SECONDS)))
)
# Grace period for the collector to wind down after a post-wipe restart request
# before we cancel it outright. Kept short on purpose: the write gate stays paused
# across the restart, so anything still queued is old-season work we do not want
# written anyway.
WIPE_RESTART_GRACE_SECONDS = float(getenv_clean("WIPE_RESTART_GRACE_SECONDS", "30"))


class WriteGate:
    """Cooperative pause for DB writers during a season-rollover wipe.

    Wrap a counted write section with ``begin()``/``end()`` (or ``async with``):
    a paused gate blocks new sections from starting, and ``is_quiesced()`` is true
    only once every in-flight section has exited — that is the signal the DB event
    waits for before it truncates. ``pause_point()`` is a lighter, uncounted check
    for periodic writers to stop *between* units of work.
    """

    def __init__(self):
        self._allowed = asyncio.Event()
        self._allowed.set()
        self._in_flight = 0

    async def begin(self):
        await self._allowed.wait()
        self._in_flight += 1

    def end(self):
        self._in_flight -= 1

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.end()
        return False

    async def pause_point(self):
        await self._allowed.wait()

    def pause(self):
        self._allowed.clear()

    def resume(self):
        self._allowed.set()

    @property
    def paused(self):
        return not self._allowed.is_set()

    def is_quiesced(self):
        return (not self._allowed.is_set()) and self._in_flight == 0


WRITE_GATE = WriteGate()

try:
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
except Exception:
    GLOBAL_STATS.console_log("Not on Windows, skipping event loop policy set.")

# Configuration

parser = argparse.ArgumentParser()
parser.add_argument(
    "--region",
    help="If set, only collect this Blizzard region (e.g. 'us').",
    choices=["us", "eu", "kr", "tw"],
)

args = parser.parse_args()

GLOBAL_STATS.console_log("Initializing database connection pool…")

DATABASE_WORKERS = int(getenv_clean("DATABASE_WORKERS", "1"))
databaseConnector.init_connection_pool(
    getenv_clean("DATABASE_HOST"),
    getenv_clean("DATABASE_USER"),
    getenv_clean("DATABASE_PASSWORD"),
    getenv_clean("DATABASE_NAME"),
    getenv_clean("DATABASE_PORT"),
    DATABASE_WORKERS + 4,  # +route_db_worker +run_raiderio_top_loadouts +simworker +wipe_watch
)

if args.region:
    REGIONS = [args.region]
else:
    REGIONS = getenv_clean("REGIONS", "us,eu,kr,tw").split(",")

# Blizzard API settings
API_BASE = "https://{region}.api.blizzard.com"
OAUTH_BASE = "https://oauth.battle.net/token"
NAMESPACE_DYNAMIC = "dynamic-{region}"
LOCALE = getenv_clean("LOCALE", "en_US")

# Paths and constants
DATA_DIR = Path("data")
RUNS_DIR = DATA_DIR / "runs"
DUNGEON_STATIC = DATA_DIR / "static" / "dungeons.json"

# Current-season dungeon set: the keys of dungeons.json are the current
# map_challenge_mode_ids. Top players still carry runs in previous-season
# dungeons, so loadouts for those zones must never be stored (they pollute
# top_player_loadouts and skew the per-spec hero-tree badge). Fail loud if the
# static file is unreadable so a bad deploy cannot silently store everything.
CURRENT_DUNGEON_MAP_IDS = {int(k) for k in json.loads(DUNGEON_STATIC.read_text()).keys()}

TALENTS_STATIC = DATA_DIR / "static" / "talents.json"
CHOICE_NODE_IDS = set()
with open(TALENTS_STATIC, "r", encoding="utf-8") as f:
    talents_data = json.load(f)
    for spec in talents_data:
        for node_type in ("classNodes", "specNodes", "heroNodes"):
            for node in spec.get(node_type, []):
                if node.get("type") == "choice":
                    CHOICE_NODE_IDS.add(node["id"])

print(f"Loaded {len(CHOICE_NODE_IDS)} choice node IDs from talents data.")
# rio settings to get Highest Key Levels
PAGE_TO_FETCH = 100
KEYLEVELS_DOWN = 5

# Rate limits (they are 30k/hour and 100/second globally)
per_second_limiter = AsyncLimiter(90, 1)
per_hour_limiter = AsyncLimiter(29500, 3600)

# Rate limits (per‑region) Assumes we use one api key per region and not one for all of them
REGION_LIMITERS: dict[str, dict[str, AsyncLimiter]] = {
    region: {
        "per_second": AsyncLimiter(90, 1),
        "per_hour": AsyncLimiter(29500, 3600),
    }
    for region in REGIONS
}

# backoff, per region
region_backoff_until: dict[str, float] = {region: 0.0 for region in REGIONS}
region_backoff_lock: dict[str, asyncio.Lock] = {
    region: asyncio.Lock() for region in REGIONS
}
MAX_GLOBAL_BACKOFF = 60.0
BASE_BACKOFF = 1.0

# stat tracking variables
fetched_runs = 0
fetched_profiles = 0

BATCH_SIZE = int(getenv_clean("DB_BATCH_SIZE", "50"))
# Blizzard OAuth

REGION_CREDENTIALS: dict[str, dict[str, str]] = {}

RAIDERIO_API_KEY = getenv_clean("RAIDERIO_API_KEY")
KEYSTONE_USER = getenv_clean("KEYSTONE_GURU_USER")
KEYSTONE_PW = getenv_clean("KEYSTONE_GURU_PW")

RAIDER_RATE_LIMIT = AsyncLimiter(int(getenv_clean("RAIDER_RATE_MAX", "500")), int(getenv_clean("RAIDER_RATE_PERIOD", "60")))
KEYSTONE_RATE_LIMIT = AsyncLimiter(int(getenv_clean("KEYSTONE_RATE_MAX", "100")), int(getenv_clean("KEYSTONE_RATE_PERIOD", "60")))

GLOBAL_RAIDER_BACKOFF = {"until": 0, "lock": asyncio.Lock()}
run_details_cache: dict[int, dict] = {}
CACHE_MAX_SIZE = 5000

for region in REGIONS:
    id_var = f"BLIZ_CLIENT_ID_{region.upper()}"
    sec_var = f"BLIZ_CLIENT_SECRET_{region.upper()}"
    cid = getenv_clean(id_var)
    csec = getenv_clean(sec_var)
    if not cid or not csec:
        raise RuntimeError(f"{id_var} and {sec_var} must be set")
    REGION_CREDENTIALS[region] = {
        "client_id": cid,
        "client_secret": csec,
    }
_token_cache = {}

# In-memory cache for previously fetched data
processed_runs: set[str] = set()
enqueued_profiles: dict[str, dict[str, Path]] = {}


CURRENT_SEASON = ""

# --- Partial-rollover season floor (see database.sql wipe_control) ---
# US resets first and triggers the rollover wipe while EU/KR/TW are still finishing
# the old season. Each region resolves its OWN current season independently, so
# after the wipe (which advances wipe_control.done_season to the NEW season and then
# restarts the collector) the lagging regions would resolve the OLD season and
# re-insert old-season runs into the freshly-cleared tables — and since done_season
# now blocks any further wipe, those rows would survive the whole new season.
#
# SEASON_FLOOR is that done_season: the collector refuses to collect or write any
# run whose season is strictly below it. 0 means "no floor" — the wipe feature is
# not deployed, or no wipe has ever run — so this is a no-op on older DBs. It is
# resolved once at startup like season/period; a completed wipe restarts the
# process, so a fresh value is always read on the next launch, and a lagging region
# is picked up automatically on a later restart once it too rolls over.
SEASON_FLOOR = 0


# Utilities and route fetch logic
def aggregate_talents(talents_list: list) -> list:
    counts = defaultdict(int)
    for t in talents_list:
        tid = t.get("id")
        rank = t.get("rank", 1)
        if tid is not None:
            counts[tid] += rank
            if tid in CHOICE_NODE_IDS:
                tooltip = t.get("tooltip", {})
                choice_id = tooltip.get("talent", {}).get("id")
                if choice_id is not None:
                    counts[choice_id] += rank
    return list(counts.items())

def aggregate_enemies_occurrence(pull: dict) -> dict:
    counts = defaultdict(int)
    for e in pull.get("enemies", []):
        npc = e.get("npcId")
        if npc is None:
            continue
        counts[int(npc)] += 1
    return counts

async def fetch_raider_page(session: ClientSession, dungeon_slug: str, page: int) -> dict:
    url = "https://raider.io/api/v1/mythic-plus/runs"
    params = {
        "region": "world",
        "dungeon": dungeon_slug,
        "page": page,
        "access_key": RAIDERIO_API_KEY,
    }
    await RAIDER_RATE_LIMIT.acquire()
    try:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        GLOBAL_STATS.console_log(f"ERROR fetching page {page} for dungeon '{dungeon_slug}': {e}")
        return {"rankings": [], "params": {}}

async def fetch_run_details(session: ClientSession, run_id: int, season: str) -> dict:
    global run_details_cache
    if run_id in run_details_cache:
        return run_details_cache[run_id]

    url = "https://raider.io/api/v1/mythic-plus/run-details"
    params = {"id": run_id, "season": season}
    attempt = 0
    while attempt < 5:
        async with GLOBAL_RAIDER_BACKOFF["lock"]:
            now = time.time()
            if GLOBAL_RAIDER_BACKOFF["until"] > now:
                await asyncio.sleep(GLOBAL_RAIDER_BACKOFF["until"] - now)
        await RAIDER_RATE_LIMIT.acquire()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    ra = resp.headers.get("Retry-After")
                    wait = float(ra) if ra else (2**attempt)
                    async with GLOBAL_RAIDER_BACKOFF["lock"]:
                        GLOBAL_RAIDER_BACKOFF["until"] = time.time() + wait
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
                        ts = int(datetime.fromisoformat(completed_at.replace("Z", "+00:00")).timestamp())
                    except:
                        pass

                reduced = {
                    "route_key": full.get("logged_details", {}).get("route_key"),
                    "mythic_level": full.get("mythic_level"),
                    "dungeon_id": full.get("dungeon", {}).get("map_challenge_mode_id"),
                    "roster_specs": roster_specs,
                    "duration": full.get("clear_time_ms"),
                    "timestamp": ts,
                    "keystone_run_id": full.get("keystone_run_id"),
                    "completed_at": completed_at,
                }

                if len(run_details_cache) >= CACHE_MAX_SIZE:
                    keys = list(run_details_cache.keys())[:int(CACHE_MAX_SIZE * 0.1)]
                    for k in keys:
                        del run_details_cache[k]

                run_details_cache[run_id] = reduced
                return reduced
        except Exception as e:
            attempt += 1
            await asyncio.sleep(2**attempt)
    return {}

async def fetch_keystone_route(session: ClientSession, route_key: str) -> dict:
    url = f"https://keystone.guru/api/v1/route/{route_key}"
    await KEYSTONE_RATE_LIMIT.acquire()
    auth = BasicAuth(login=KEYSTONE_USER, password=KEYSTONE_PW)
    try:
        async with session.get(url, auth=auth) as resp:
            resp.raise_for_status()
            j = await resp.json()
            return j.get("data", {}) or {}
    except Exception as e:
        GLOBAL_STATS.console_log(f"ERROR fetching keystone.guru route {route_key}: {e}")
        if e.response is not None and e.response.status == 401:
            raise RuntimeError("Unauthorized access to keystone.guru API - check credentials and rate limits.")
        return {}

async def route_db_worker(name: str):
    """
    Pull route payloads from route_db_queue and write them using databaseConnector.
    """
    try:
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()
            while not cancel_event.is_set():
                try:
                    job = await asyncio.wait_for(route_db_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    if cancel_event.is_set() and route_db_queue.empty():
                        break
                    continue

                if job is None:
                    route_db_queue.task_done()
                    break

                raider_reduced, keystone_route = job
                route_key = keystone_route.get("publicKey") or raider_reduced.get("route_key")
                await WRITE_GATE.begin()  # yield to a pending season wipe
                try:
                    print(f"[{name}] Processing route {route_key} for run {raider_reduced.get('keystone_run_id')}")
                    rio_run_id = int(raider_reduced.get("keystone_run_id") or 0)
                    mapping_version = int(keystone_route.get("mappingVersion") or 0)
                    enemy_forces = int(keystone_route.get("enemyForces") or 0)
                    timestamp = int(raider_reduced.get("timestamp") or 0)
                    keystone_level = int(raider_reduced.get("mythic_level") or 0)
                    duration = int(raider_reduced.get("duration") or 0)
                    dungeon_id = raider_reduced.get("dungeon_id")

                    if not route_key or not rio_run_id or not mapping_version:
                        raise ValueError("Invalid parameters")

                    rowcount = databaseConnector.insert_route_data(
                        conn, cursor, rio_run_id, mapping_version, enemy_forces, timestamp, keystone_level, duration, dungeon_id, route_key
                    )
                    if rowcount == 0:
                        # Duplicate route, skip inserting specs and pulls
                        conn.rollback() # Or commit(), doesn't matter, just skip
                        await GLOBAL_STATS.increment("duplicate_routes")
                        print(f"[{name}] Route {route_key} already exists in DB, skipping.")
                        continue
                    
                    for s in raider_reduced.get("roster_specs", []):
                        try:
                            databaseConnector.insert_route_spec(conn, cursor, route_key, int(s))
                        except Exception as e:
                            print(f"[{name}] Error inserting route spec for route {route_key}: {e}")
                    
                    for pull in keystone_route.get("pulls", []) or []:
                        try:
                            new_pull_id = databaseConnector.insert_route_pull(conn, cursor, route_key)
                        except Exception as e:
                            conn.rollback()
                            raise
                            
                        counts = aggregate_enemies_occurrence(pull)
                        for npc_id, cnt in counts.items():
                            try:
                                databaseConnector.insert_pull_enemies(conn, cursor, route_key, new_pull_id, int(npc_id), int(cnt))
                            except Exception as e:
                                print(f"[{name}] Error inserting pull enemies for route {route_key}: {e}")
                        for spell in set(pull.get("spells") or []):
                            try:
                                databaseConnector.insert_pull_spells(conn, cursor, route_key, new_pull_id, int(spell))
                            except Exception as e:
                                print(f"[{name}] Error inserting pull spell for route {route_key}: {e}")
                    print(f"[{name}] Successfully inserted route {route_key} with {len(raider_reduced.get('roster_specs', []))} specs and {len(keystone_route.get('pulls', []))} pulls.")       
                    conn.commit()
                    await GLOBAL_STATS.increment("db_insert_route")
                except Exception as e:
                    conn.rollback()
                    GLOBAL_STATS.console_log(f"[{name}] DB insert route error: {e}")
                finally:
                    WRITE_GATE.end()
                    route_db_queue.task_done()
    except Exception as e:
        GLOBAL_STATS.console_log(f"[{name}] CRITICAL ERROR starting route worker (connection pool?): {e}")

async def route_poller_task(session: ClientSession):
    global CURRENT_SEASON
    dungeons = json.loads(DUNGEON_STATIC.read_text())
    specs = json.loads((DATA_DIR / "static" / "specs.json").read_text())
    dungeon_slugs = [info["slug"] for info in dungeons.values()]
    spec_ids = [int(k) for k in specs.keys()]

    while not cancel_event.is_set():
        for slug in dungeon_slugs:
            if cancel_event.is_set(): break
            found_runs = {spec: set() for spec in spec_ids}
            page = 1
            while page < 500 and not cancel_event.is_set():
                data = await fetch_raider_page(session, slug, page)
                await GLOBAL_STATS.increment("rio_pages_checked")
                rankings = data.get("rankings", [])
                if not rankings: break
                
                if not CURRENT_SEASON:
                    CURRENT_SEASON = data.get("params", {}).get("season", "")

                for entry in rankings:
                    run = entry.get("run")
                    if not run or run.get("logged_run_id") is None: continue
                    keystone_id = run.get("keystone_run_id")
                    for member in run.get("roster", []):
                        char_spec = member.get("character", {}).get("spec", {}).get("id")
                        if char_spec in found_runs and len(found_runs[char_spec]) < 50:
                            found_runs[char_spec].add(keystone_id)
                
                page += 1

            run_ids_set = set()
            for runs in found_runs.values():
                run_ids_set.update(runs)

            for run_id in sorted(run_ids_set):
                if cancel_event.is_set(): break
                raider = await fetch_run_details(session, run_id, CURRENT_SEASON)
                await GLOBAL_STATS.increment("rio_routes_checked")
                if not raider: 
                    continue

                ts = raider.get("timestamp")
                if not ts or int(ts) < int(time.time()) - (28 * 24 * 60 * 60):
                    continue

                route_key = raider.get("route_key")
                if not route_key: 
                    continue

                keystone = await fetch_keystone_route(session, route_key)
                await GLOBAL_STATS.increment("kg_routes_fetched")
                if not keystone: 
                    continue
                ef_actual = keystone.get("enemyForces")
                ef_required = keystone.get("enemyForcesRequired")
                if ef_actual is None or ef_required is None or int(ef_actual) < int(ef_required):
                    continue
                
                await route_db_queue.put((raider, keystone))
                
                await asyncio.sleep(10) # slow running

        await asyncio.sleep(3600) # wait an hour before fetching again

# Utils
async def get_max_keys_by_dungeon(session: ClientSession) -> dict[int, int]:
    """Returns a map dungeon_id -> highest mythic_level seen on Raider.IO."""
    # load slug lookup
    static = json.loads((DUNGEON_STATIC).read_text())
    global CURRENT_SEASON
    max_keys = {}
    for did, info in static.items():
        slug = info["slug"]
        url = "https://raider.io/api/v1/mythic-plus/runs"
        static_params = {
            "access_key": RAIDERIO_API_KEY,
            "region": "world",
            "dungeon": slug,
            "page": PAGE_TO_FETCH,
        }
        try:
            async with session.get(url, params=static_params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            # Raider.IO may include the season slug in several places; check known locations
            try:
                season_slug = None
                if isinstance(data, dict):
                    season_slug = data.get("params", {}).get("season")
                    if not season_slug:
                        rankings_block = data.get("rankings")
                        if isinstance(rankings_block, dict):
                            season_slug = rankings_block.get("ui", {}).get("season")
                    if not season_slug:
                        season_slug = data.get("ui", {}).get("season")

                if season_slug and not CURRENT_SEASON:
                    CURRENT_SEASON = season_slug
                    GLOBAL_STATS.console_log(f"Captured Raider.IO season slug: {CURRENT_SEASON}")
            except Exception:
                pass

            # Extract mythic levels from the returned shape (be defensive about structure)
            found_levels = []
            try:
                rankings_block = data.get("rankings") if isinstance(data, dict) else None
                if isinstance(rankings_block, list):
                    for r in rankings_block:
                        if isinstance(r, dict):
                            run = r.get("run")
                            if isinstance(run, dict) and run.get("mythic_level") is not None:
                                found_levels.append(int(run["mythic_level"]))
                            else:
                                runs = r.get("runs") or []
                                if isinstance(runs, list):
                                    for ru in runs:
                                        if isinstance(ru, dict) and ru.get("mythic_level") is not None:
                                            found_levels.append(int(ru.get("mythic_level")))

                elif isinstance(rankings_block, dict):
                    # common inner lists
                    for key in ("rankedCharacters", "players", "characters"):
                        lst = rankings_block.get(key)
                        if isinstance(lst, list):
                            for char in lst:
                                if not isinstance(char, dict):
                                    continue
                                runs = char.get("runs") or []
                                if isinstance(runs, list):
                                    for ru in runs:
                                        if isinstance(ru, dict) and ru.get("mythic_level") is not None:
                                            found_levels.append(int(ru.get("mythic_level")))
                else:
                    # try top-level lists
                    for key in ("rankedCharacters", "players"):
                        lst = data.get(key)
                        if isinstance(lst, list):
                            for char in lst:
                                runs = char.get("runs") or []
                                if isinstance(runs, list):
                                    for ru in runs:
                                        if isinstance(ru, dict) and ru.get("mythic_level") is not None:
                                            found_levels.append(int(ru.get("mythic_level")))

                if found_levels:
                    # choose the maximum seen mythic level for this dungeon
                    max_keys[int(did)] = max(found_levels)
            except Exception:
                pass
        except Exception as e:
            GLOBAL_STATS.console_log(
                f"Error fetching max keys for dungeon {did} ({slug}): {e}"
            )
            pass
    return max_keys


async def load_processed_runs(session):
    GLOBAL_STATS.console_log("Loading previously processed runs…")
    ensure_dir(RUNS_DIR)
    active_seasons = {}
    active_periods = {}
    for region in REGIONS:
        current_season = await get_current_season_id(session, region)
        active_period = max(await get_season_periods(session, region, current_season))
        active_seasons[region] = str(current_season)
        active_periods[region] = str(active_period)

    try:
        runs_list = list(RUNS_DIR.rglob("*.csv"))
    except FileNotFoundError:
        runs_list = []

    for runs_csv in runs_list:
        # File may have been removed since we took the snapshot
        if not runs_csv.exists():
            continue
        try:
            rel = runs_csv.relative_to(RUNS_DIR)
        except Exception:
            # file outside expected root; skip
            continue

        parts = rel.parts
        if len(parts) < 4:
            # unexpected layout, skip
            continue

        region, realm, season_part, period_file = parts[0], parts[1], parts[2], parts[3]
        period = Path(period_file).stem

        active_season = active_seasons.get(region)
        active_period = active_periods.get(region)

        if active_season is None or active_period is None:
            continue

        is_active = season_part == active_season and period == active_period

        if is_active:
            print(f"Loading runs from active season/period {season_part}/{period} for region {region} and realm {realm} from {runs_csv}")
            with runs_csv.open(newline="") as f:
                reader = csv.reader(f)
                print(f"Reading rows from {runs_csv}")
                for row in reader:
                    if row:
                        processed_runs.add(row[0])
                print(f"Finished loading {len(processed_runs)} runs from {runs_csv}")
        else:
            try:
                runs_csv.unlink()
                GLOBAL_STATS.console_log(f"Removed old runs file: {runs_csv}")
                parent = runs_csv.parent
                while parent != RUNS_DIR and parent.exists():
                    if any(parent.iterdir()):
                        break
                    parent.rmdir()
                    parent = parent.parent
            except Exception as e:
                GLOBAL_STATS.console_log(f"failed to remove {runs_csv}: {e}")

    return processed_runs


def load_existing_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def hash_object(obj: dict) -> str:
    payload = json.dumps(obj, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


async def get_access_token(session: ClientSession, region: str) -> str:
    creds = REGION_CREDENTIALS[region]
    cache = _token_cache.get(region)

    if cache and cache["expires_at"] > time.time() + 3600:
        return cache["access_token"]
    async with session.post(
        OAUTH_BASE,
        auth=BasicAuth(creds["client_id"], creds["client_secret"]),
        data={"grant_type": "client_credentials"},
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        token = data["access_token"]
        expires = data.get("expires_in", 0)
        _token_cache[region] = {
            "access_token": token,
            "expires_at": time.time() + expires,
        }
        return token


async def fetch_json(
    session: RetryClient, url: str, params: dict, region: str
) -> dict | None:
    """
    Single-shot fetch through an aiohttp_retry.RetryClient — rate-limited per-region,
    with shared backoff if we see a 429 (via Retry-After).
    """
    # shutdown short-circuit
    if shutdown_event.is_set():
        return None

    #  honor any shared region backoff
    now = time.time()
    until = region_backoff_until[region]
    if now < until:
        await asyncio.sleep(until - now)

    # grab a fresh token and acquire our rate‑limit permits
    token = await get_access_token(session, region)
    headers = {"Authorization": f"Bearer {token}"}
    limiters = REGION_LIMITERS[region]
    await limiters["per_second"].acquire()
    await limiters["per_hour"].acquire()

    try:
        # fire the request — RetryClient will automatically retry on
        #    configured statuses/exceptions (429, 5xx, network errors, etc).
        async with session.get(
            url, params=params, headers=headers, raise_for_status=False
        ) as resp:
            # short‑circuit for 404 (private profile)
            if resp.status == 404:
                return None

            # if Blizzard still sends us a 429 (and we want to throttle globally),
            #    extract Retry-After and bump region_backoff_until
            if resp.status == 429:
                ra = resp.headers.get("Retry-After")
                backoff = float(ra) if ra else base_backoff
                expiry = time.time() + backoff
                async with region_backoff_lock[region]:
                    region_backoff_until[region] = max(
                        region_backoff_until[region], expiry
                    )
                # let the RetryClient also see the 429 and retry if it wants,
                # or we can just return None here if it’s past its retry count
                return None

            # for everything else, propagate errors & parse JSON
            resp.raise_for_status()
            return await resp.json()

    except ClientResponseError as e:
        # non-retryable server errors (400, 403, etc)
        GLOBAL_STATS.console_log(f"HTTP {e.status} for {url}")
        return None

    except (
        asyncio.TimeoutError,
        aiohttp.ClientConnectionError,
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientPayloadError,
        ClientConnectionResetError,
    ) as e:
        # *re*-raise so RetryClient can back off & retry
        raise

    except Exception as e:
        # any other unexpected error—log and re-raise
        GLOBAL_STATS.console_log(f"Unexpected error {type(e).__name__}: {e}")
        raise


async def fetch_leaderboard_and_queue(
    session, season, region, realm, period, dungeon, max_keys
):
    lb = await get_leaderboard(session, region, realm, dungeon["dungeon_id"], period)
    if not lb or "leading_groups" not in lb:
        GLOBAL_STATS.console_log(
            f"No Correct leaderboard data for {region}/{realm}/{dungeon['dungeon_id']}/{period} got unexpected result."
        )
        return

    await GLOBAL_STATS.increment("checked_runs", len(lb.get("leading_groups", [])))

    for group in lb["leading_groups"]:
        # build run_hash, skip processed, then:
        run_hash = hash_object(
            {
                "dungeon": dungeon["dungeon_id"],
                "period": period,
                "members": [m["profile"]["id"] for m in group["members"]],
                "timestamp": group["completed_timestamp"],
            }
        )
        if run_hash in processed_runs:
            continue
        processed_runs.add(run_hash)
        for member in group["members"]:
            member["profile"]["timestamp"] = (
                datetime.now(timezone.utc).date().isoformat()
            )
        group.update(
            {
                "run_hash": run_hash,
                "dungeon_id": dungeon["dungeon_id"],
                "keystone_level": group["keystone_level"],
                "duration": group["duration"],
                "completed_timestamp": group["completed_timestamp"],
                "members": group["members"],
                "majority_faction": Counter(
                    [m["faction"]["type"] for m in group["members"]]
                ).most_common(1)[0][0]
                if group["members"]
                else None,
                "member_hashes": [hash_object(m["profile"]) for m in group["members"]],
            }
        )

        # decide queue by age
        completed = datetime.fromtimestamp(
            group["completed_timestamp"] / 1000, tz=timezone.utc
        )
        run_level = group["keystone_level"]
        top_level = max_keys.get(dungeon["dungeon_id"], 0)
        threshold = max(0, top_level - KEYLEVELS_DOWN)
        if (
            datetime.now(timezone.utc) - completed <= timedelta(days=1)
            and run_level >= threshold
        ):
            await advanced_queue.put((region, season, period, realm, dungeon, group))
        else:
            await simple_queue.put((region, season, period, realm, dungeon, group))


async def process_realm(
    region: str,
    realm: int,
    session: ClientSession,
    current_season: int,
    periods: list[int],
    max_keys,
):
    try:
        if cancel_event.is_set():
            GLOBAL_STATS.console_log(f"{region} cancellation requested, stopping")
            return
        GLOBAL_STATS.console_log(f"{region} enqueuing realm {realm}")

        # Enqueue all period/dungeon work onto this realm's queue
        for period in periods:
            if cancel_event.is_set():
                GLOBAL_STATS.console_log(f"{period} - cancellation requested, stopping")
                return
            GLOBAL_STATS.console_log(f"{region} {realm} enqueuing period {period} ")
            dungeons = await get_leaderboard_index(session, region, realm)
            for dungeon in dungeons:
                if cancel_event.is_set():
                    GLOBAL_STATS.console_log(
                        f"{dungeon} - cancellation requested, stopping"
                    )
                    return
                try:
                    await fetch_leaderboard_and_queue(
                        session,
                        current_season,
                        region,
                        realm,
                        period,
                        dungeon,
                        max_keys,
                    )
                except Exception as e:
                    GLOBAL_STATS.console_log(f"{dungeon} - error occurred: {e}")
                    traceback.print_exc()
    except Exception as e:
        GLOBAL_STATS.console_log(
            f"[process realm] failed for realm {realm} in {region} : {e}"
        )
        traceback.print_exc()


async def get_connected_realms(session: ClientSession, region: str) -> list[int]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/index"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or "connected_realms" not in data:
        return []
    return [
        int(r["href"].split("/")[-1].split("?")[0]) for r in data["connected_realms"]
    ]


async def get_current_season_id(session: ClientSession, region: str) -> int:
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/index"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or not data.get("seasons"):
        return None
    return data["current_season"]["id"]


async def get_season_periods(
    session: ClientSession, region: str, season_id: int
) -> list[int]:
    url = (
        f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/{season_id}"
    )
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or "periods" not in data:
        return []

    return [p["id"] for p in data["periods"]]


async def get_leaderboard_index(
    session: ClientSession, region: str, realm_id: int
) -> list[dict]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/{realm_id}/mythic-leaderboard/index"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or "current_leaderboards" not in data:
        return []
    return [
        {"dungeon_id": lb["id"], "name": lb["name"]}
        for lb in data["current_leaderboards"]
    ]


def slugify(name: str) -> str:
    if not name:
        return ""
    s = name.lower().strip()
    s = s.replace(" ", "-").replace("'", "").replace(".", "")
    return s


async def fetch_spec_rankings(session, class_slug, spec_slug, season, page=0):
    url = "https://raider.io/api/mythic-plus/rankings/specs"
    params = {
        "region": "world",
        "class": class_slug,
        "spec": spec_slug,
        "page": page,
    }
    if season:
        params["season"] = season
    await RAIDER_RATE_LIMIT.acquire()
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 404:
                return None
            if resp.status == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra else 2.0
                await asyncio.sleep(wait)
                return None
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        GLOBAL_STATS.console_log(f"ERROR fetching spec rankings {class_slug}/{spec_slug} page {page}: {e}")
        return None


async def fetch_character_loadouts(session, region, realm, name):
    # realm may be a dict from Raider.IO payloads; normalize to a slug string
    realm_slug = None
    try:
        if isinstance(realm, dict):
            realm_slug = (
                realm.get("slug")
                or realm.get("altSlug")
                or realm.get("alt_slug")
                or realm.get("name")
                or realm.get("altName")
                or realm.get("alt_name")
                or (str(realm.get("id")) if realm.get("id") is not None else None)
            )
            if realm_slug:
                realm_slug = slugify(str(realm_slug))
        else:
            realm_slug = str(realm)
    except Exception:
        realm_slug = str(realm)

    if not realm_slug:
        GLOBAL_STATS.console_log(f"ERROR: could not determine realm slug for character {name}: {realm!r}")
        return None

    # name may also be an object in some payloads
    name_str = name.get("name") if isinstance(name, dict) and name.get("name") else str(name)

    # realm and name should be URL-safe
    realm_q = quote_plus(realm_slug)
    name_q = quote_plus(name_str)
    url = f"https://raider.io/api/characters/{region}/{realm_q}/{name_q}/loadouts"
    params = {"access_key": RAIDERIO_API_KEY}
    await RAIDER_RATE_LIMIT.acquire()
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 404:
                return None
            if resp.status == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra else 2.0
                await asyncio.sleep(wait)
                return None
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        GLOBAL_STATS.console_log(f"ERROR fetching loadouts for {name_str}@{realm_slug}: {e}")
        return None


async def fetch_loadout_detail(session, loadout_id , realm, name, region):
    url = f"https://raider.io/api/characters/{region}/{realm}/{name}?loadout={loadout_id}"
    await RAIDER_RATE_LIMIT.acquire()
    try:
        async with session.get(url) as resp:
            if resp.status == 404:
                return None
            if resp.status == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra else 2.0
                await asyncio.sleep(wait)
                return None
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        GLOBAL_STATS.console_log(f"ERROR fetching loadout detail {loadout_id}: {e}")
        return None

RAIDERIO_SLOT_MAP = {
    "back": "BACK",
    "chest": "CHEST",
    "feet": "FEET",
    "finger1": "FINGER_1",
    "finger2": "FINGER_2",
    "finger": "FINGER_1",
    "hands": "HANDS",
    "head": "HEAD",
    "legs": "LEGS",
    "mainhand": "MAIN_HAND",
    "offhand": "OFF_HAND",
    "neck": "NECK",
    "shirt": None,  # ignore shirt
    "shoulder": "SHOULDERS",
    "trinket1": "TRINKET_1",
    "trinket2": "TRINKET_2",
    "trinket": "TRINKET_1",
    "waist": "WAIST",
    "wrist": "WRIST",
}
def _normalize_bonus_ids(raw):
    """Normalise a Raider.IO item bonus field to a comma-joined string of ids.

    Accepts a list of ints/strs/dicts, a comma/space-separated string, or None.
    Returns None when there is nothing usable so the DB column stays NULL.
    """
    if not raw:
        return None
    ids = []
    if isinstance(raw, (list, tuple)):
        items = raw
    elif isinstance(raw, str):
        items = re.split(r"[,\s]+", raw.strip())
    else:
        items = [raw]
    for b in items:
        if isinstance(b, dict):
            b = b.get("id") or b.get("bonus_id") or b.get("bonusId")
        if b is None or b == "":
            continue
        try:
            ids.append(int(b))
        except (ValueError, TypeError):
            continue
    if not ids:
        return None
    return ",".join(str(i) for i in ids)


def parse_loadout_for_db(spec_id, season, rank, loadout_detail):
    items_rows = []
    gems_rows = []
    talents_rows = []
    enchants_rows = []
    # items may appear under itemDetails.items or items
    item_details = {}
    if isinstance(loadout_detail, dict):
        item_details = loadout_detail.get("itemDetails") or loadout_detail.get("items") or {}
        # also support nested structure from characterDetails -> itemDetails
        if not item_details and loadout_detail.get("characterDetails"):
            item_details = loadout_detail["characterDetails"].get("itemDetails", {})
    if isinstance(item_details, dict):
        items_map = item_details.get("items") if "items" in item_details else item_details
        # Map common Raider.IO slot names to Blizzard slot names used in DB
        for slot, obj in items_map.items():
            try:
                # normalize incoming slot key for lookup
                if not slot:
                    continue
                norm_slot = str(slot).lower().replace(" ", "").replace("-", "").replace("/", "").replace("\\", "").replace("_", "")
                bliz_slot = RAIDERIO_SLOT_MAP.get(norm_slot)
                if bliz_slot is None:
                    continue
                item_id = obj.get("item_id") or obj.get("id") or obj.get("itemId") or None
                if item_id is None:
                    continue
                item_level = obj.get("item_level") or obj.get("item_level_equipped") or None
                enchant = obj.get("enchant") or obj.get("enchantment") or obj.get("enchantment_id") or None
                # Bonus ids drive missive/embellishment identification (same as the
                # general equipment path). Raider.IO itemDetails may expose these
                # under a few different keys; parse tolerantly and normalise to a
                # comma-joined string of ids (NULL when absent).
                raw_bonuses = (
                    obj.get("bonuses")
                    or obj.get("bonus_id")
                    or obj.get("bonusIds")
                    or obj.get("bonus_list")
                    or None
                )
                bonus_ids = _normalize_bonus_ids(raw_bonuses)
                # don't include enchant in items_rows; store separately
                items_rows.append((spec_id, season, rank, bliz_slot, int(item_id), int(item_level) if item_level else None, bonus_ids))
                # normalize enchant id if present
                try:
                    if enchant:
                        if isinstance(enchant, dict):
                            ench_id = enchant.get("item_id") or enchant.get("id") or enchant.get("enchantment_id")
                        else:
                            ench_id = enchant
                        if ench_id:
                            enchants_rows.append((spec_id, season, rank, bliz_slot, int(ench_id)))
                except Exception:
                    pass
                gems = obj.get("gems") or obj.get("gems_detail") or []
                for g in gems:
                    # Only store the gem item id; do not record socket indices or slot
                    if isinstance(g, dict):
                        gem_id = g.get("item_id") or g.get("id") or g.get("itemId") or None
                    else:
                        gem_id = g
                    if gem_id:
                        try:
                            gems_rows.append((spec_id, season, rank, int(gem_id)))
                        except Exception:
                            continue
            except Exception:
                continue
    # Real Blizzard v2 export string the player used in game, captured verbatim
    # from characterDetails.character.talentLoadout.loadoutText. This is exactly
    # what simc and the game accept, so the tierlist top50 sim uses it directly
    # instead of reconstructing a code from the per-node rows below. A missing or
    # empty string just leaves the column NULL (top50 degrades gracefully); the
    # per-node rows are still written for the spec page's node-usage stats.
    loadout_text = None
    try:
        talent_loadout = loadout_detail.get("characterDetails", {}).get("character", {}).get("talentLoadout", {}) if isinstance(loadout_detail, dict) else {}
        raw_text = talent_loadout.get("loadoutText") if isinstance(talent_loadout, dict) else None
        if isinstance(raw_text, str) and raw_text.strip():
            loadout_text = raw_text.strip()
    except Exception:
        loadout_text = None
    # talents under characterDetails.character.talentLoadout.nodes
    try:
        nodes = loadout_detail.get("characterDetails", {}).get("character", {}).get("talentLoadout", {}).get("nodes", []) if isinstance(loadout_detail, dict) else []
        for n in nodes:
            node_obj = n.get("node") or n.get("node_id") or {}
            node_id = node_obj.get("id") if isinstance(node_obj, dict) else node_obj
            node_rank = int(n.get("rank") or 1)
            # Selected choice: entryIndex points into node.entries. Needed so
            # choice nodes can show which option the top players actually pick.
            entry_id = None
            spell_id = None
            entries = node_obj.get("entries") if isinstance(node_obj, dict) else None
            entry_index = n.get("entryIndex")
            if isinstance(entries, list) and isinstance(entry_index, int) and 0 <= entry_index < len(entries):
                selected = entries[entry_index]
                if isinstance(selected, dict):
                    entry_id = selected.get("id")
                    spell = selected.get("spell")
                    if isinstance(spell, dict):
                        spell_id = spell.get("id")
            if node_id:
                talents_rows.append((spec_id, season, rank, int(node_id), node_rank, entry_id, spell_id))
    except Exception:
        pass
    return items_rows, gems_rows, talents_rows, enchants_rows, loadout_text


def extract_rankings_from_spec_response(data):
    """Normalize Raider.IO /rankings/specs response to a list of ranking entries.

    The Raider.IO API returns several shapes depending on endpoint/version:
    - {'rankings': [ ... ]}
    - {'rankings': {'rankedCharacters': [ ... ], 'ui': {...}}}
    - {'rankedCharacters': [ ... ]}
    - a plain list

    This helper tries common variants and returns an empty list if none found.
    """
    if not data:
        return []

    # direct list response
    if isinstance(data, list):
        return data

    # direct 'rankings' key
    rankings = data.get("rankings") if isinstance(data, dict) else None
    if isinstance(rankings, list):
        return rankings
    if isinstance(rankings, dict):
        # look for common inner list keys
        for key in ("rankedCharacters", "players", "rankings"):
            val = rankings.get(key)
            if isinstance(val, list):
                return val
        # fallback: return first list value inside the dict
        for v in rankings.values():
            if isinstance(v, list):
                return v

    # top-level list-like keys
    for key in ("rankedCharacters", "players", "rankings"):
        val = data.get(key)
        if isinstance(val, list):
            return val

    return []


async def run_raiderio_top_loadouts(session):
    """Collect best N verified loadouts per spec and write to DB."""
    GLOBAL_STATS.console_log(f"Starting top-player loadouts collector (target={TOP_PLAYER_LOADOUTS_TARGET})")
    specs = json.loads((DATA_DIR / "static" / "specs.json").read_text())
    class_lookup = json.loads((DATA_DIR / "static" / "classes.json").read_text())

    global CURRENT_SEASON

    # season: prefer CURRENT_SEASON, then environment override; otherwise attempt to fetch from Raider.IO
    season = CURRENT_SEASON or getenv_clean("RAIDERIO_SEASON") or getenv_clean("TOP_PLAYER_LOADOUTS_SEASON") or ""

    if not season:
        try:
            # pick a sample dungeon and read its runs page to learn the current Raider.IO season param
            dungeons = json.loads(DUNGEON_STATIC.read_text())
            sample_info = next(iter(dungeons.values()))
            sample_slug = sample_info.get("slug")
            if sample_slug:
                data = await fetch_raider_page(session, sample_slug, 1)
                season = data.get("params", {}).get("season") if data else ""
                if season:
                    CURRENT_SEASON = season
                    GLOBAL_STATS.console_log(f"Determined Raider.IO season: {season}")
        except Exception as e:
            GLOBAL_STATS.console_log(f"Unable to determine Raider.IO season automatically: {e}")
            season = ""

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()

        for spec_id_str, spec_info in specs.items():
            if cancel_event.is_set():
                break
            await WRITE_GATE.pause_point()  # hold between specs while a season wipe runs
            try:
                spec_id = int(spec_id_str)
                spec_name = spec_info.get("name")
                class_id = spec_info.get("classID")
                class_name = class_lookup.get(str(class_id), {}).get("name") if class_id else None
                class_slug = slugify(class_name)
                spec_slug = slugify(spec_name)

                if not class_slug or not spec_slug:
                    GLOBAL_STATS.console_log(f"Skipping spec {spec_id} missing slug mapping: {spec_name}")
                    continue

                GLOBAL_STATS.console_log(f"Collecting top loadouts for spec {spec_id} ({class_slug}/{spec_slug})")
                collected = {}
                page = 0
                while len(collected) < TOP_PLAYER_LOADOUTS_TARGET and page < TOP_PLAYER_LOADOUTS_PAGE_LIMIT:
                    data = await fetch_spec_rankings(session, class_slug, spec_slug, season, page)
                    if not data:
                        page += 1
                        await asyncio.sleep(TOP_PLAYER_LOADOUTS_PAGE_SLEEP)
                        continue
                    rankings = extract_rankings_from_spec_response(data)
                    if not rankings:
                        raise ValueError(f"No rankings found in response for spec {spec_id} page {page}: {data!r}")
                        break

                    # determine region for character/profile requests from the rankings page
                    region_slug = None
                    if isinstance(data, dict):
                        region_slug = data.get("ui", {}).get("region") or (data.get("region") or {}).get("slug")
                    if not region_slug:
                        region_slug = "world"
                    for idx, entry in enumerate(rankings):
                        print(f"Processing spec {spec_id} page {page} entry {idx} (collected so far: {len(collected)})")
                        if len(collected) >= TOP_PLAYER_LOADOUTS_TARGET:
                            break
                        if not isinstance(entry, dict):
                            await GLOBAL_STATS.increment("skipped_ranking_entries")
                            continue

                        # compute rank robustly
                        pos = entry.get("position") or entry.get("rank")
                        try:
                            rank = int(pos) if pos is not None else (page * 100 + idx + 1)
                        except Exception:
                            rank = page * 100 + idx + 1

                        char = entry.get("character")
                        if not isinstance(char, dict):
                            await GLOBAL_STATS.increment("skipped_character_entries")
                            continue
                        char_name = char.get("name")
                        char_realm = None
                        try:
                            char_realm = (char.get("realm") or {}).get("slug")
                        except Exception:
                            char_realm = None
                        if not char_name or not char_realm:
                            continue

                        # use collection-order (1..N) as the stored "rank" so persisted
                        # rows represent the Nth datapoint we collected rather than
                        # the original leaderboard position (avoids stale data issues)
                        collected_rank = len(collected) + 1

                        # gather highest key per dungeon for this ranked character from the ranking entry
                        runs_list = entry.get("runs") or char.get("runs") or []
                        runs_by_zone = {}
                        for ru in runs_list:
                            if not isinstance(ru, dict):
                                continue
                            zid = ru.get("zoneId") or ru.get("zone_id") or (ru.get("zone") or {}).get("id")
                            lvl = ru.get("mythicLevel") or ru.get("mythic_level") or ru.get("level") or ru.get("keystone_level")
                            try:
                                zid_i = int(zid) if zid is not None else None
                            except Exception:
                                zid_i = None
                            try:
                                lvl_i = int(lvl) if lvl is not None else 0
                            except Exception:
                                lvl_i = 0
                            if zid_i:
                                runs_by_zone[zid_i] = max(runs_by_zone.get(zid_i, 0), lvl_i)

                        # fetch verified loadouts for character (prefer character region if available)
                        char_region_slug = (char.get("region") or {}).get("slug") or region_slug or "world"
                        loadouts_resp = await fetch_character_loadouts(session, char_region_slug, char_realm, char_name)
                        await asyncio.sleep(TOP_PLAYER_LOADOUTS_PAGE_SLEEP)
                        if not loadouts_resp:
                            continue
                        candidate_loadouts = loadouts_resp.get("loadouts") if isinstance(loadouts_resp, dict) and "loadouts" in loadouts_resp else loadouts_resp
                        if not candidate_loadouts or not isinstance(candidate_loadouts, list):
                            continue

                        # For this ranked character, pick at most one loadout per dungeon where a verified loadout exists
                        per_dungeon_selected = []
                        for zid, highest_key in runs_by_zone.items():
                            allowed_min = max(0, highest_key - 1)
                            # find matching loadouts for this dungeon (match by zone.id == zid)
                            matches = []
                            for ld in candidate_loadouts:
                                if not isinstance(ld, dict):
                                    continue
                                z = ld.get("zone") or {}
                                try:
                                    ld_zone_id = int(z.get("id")) if isinstance(z, dict) and z.get("id") is not None else None
                                except Exception:
                                    ld_zone_id = None
                                if ld_zone_id != zid:
                                    continue
                                # get mythic level for the loadout (logged-mplus)
                                lvl = ld.get("mythic_level") or ld.get("mythicLevel") or 0
                                try:
                                    lvl_i = int(lvl) if lvl is not None else 0
                                except Exception:
                                    lvl_i = 0
                                if lvl_i >= allowed_min:
                                    # prefer higher key, then newer loadoutDate
                                    date_str = ld.get("loadoutDate") or ld.get("createdAt") or ld.get("updatedAt")
                                    try:
                                        date_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else datetime.fromtimestamp(0, tz=timezone.utc)
                                    except Exception:
                                        date_dt = datetime.fromtimestamp(0, tz=timezone.utc)
                                    matches.append((lvl_i, date_dt, ld))
                            if not matches:
                                continue
                            # choose best match
                            matches.sort(key=lambda x: (-x[0], -int(x[1].timestamp())))
                            chosen = matches[0][2]
                            # prepare detail (may need fetching)
                            loadout_id = chosen.get("optionKey") or chosen.get("id")
                            detail = None
                            try:
                                if loadout_id:
                                    detail = await fetch_loadout_detail(session, loadout_id, char_realm, char_name, char_region_slug)
                                    await asyncio.sleep(TOP_PLAYER_LOADOUTS_PAGE_SLEEP)
                            except Exception:
                                detail = None
                            if not detail:
                                detail = chosen

                            items_rows, gems_rows, talents_rows, enchants_rows, loadout_text = parse_loadout_for_db(spec_id, season, collected_rank, detail)

                            per_dungeon_selected.append(
                                {
                                    "meta": {
                                        "region": char_region_slug,
                                        "character_id": chosen.get("character_id") or chosen.get("id"),
                                        "character_name": char_name,
                                        "realm": char_realm,
                                        "loadout_key": loadout_id,
                                        "loadout_text": loadout_text,
                                        "loadout_updated_at": chosen.get("loadoutDate") or chosen.get("createdAt"),
                                        "keystone_level": chosen.get("mythic_level") or chosen.get("mythicLevel") or None,
                                        "zone_id": zid,
                                        "map_challenge_mode_id": (chosen.get("zone") or {}).get("map_challenge_mode_id") if isinstance(chosen.get("zone"), dict) else None,
                                    },
                                    "items": items_rows,
                                    "gems": gems_rows,
                                    "talents": talents_rows,
                                    "enchants": enchants_rows,
                                }
                            )

                        if per_dungeon_selected:
                            # store list of per-dungeon entries under this collection ordinal
                            if collected_rank not in collected:
                                collected[collected_rank] = []
                            collected[collected_rank].extend(per_dungeon_selected)

                    page += 1
                    await asyncio.sleep(TOP_PLAYER_LOADOUTS_PAGE_SLEEP)

                # persist top N ranks in DB (ensure ranks 1..N canonical)
                ranks_sorted = sorted(collected.keys())[:TOP_PLAYER_LOADOUTS_TARGET]
                # cache Blizzard numeric season id per region to avoid repeated API calls
                season_id_cache: dict[str, int] = {}
                for r in ranks_sorted:
                    entries = collected.get(r) or []
                    # resolve DB season id cache per entry region as needed
                    for entry in entries:
                        meta = entry.get("meta") or {}
                        items = entry.get("items") or []
                        gems = entry.get("gems") or []
                        talents = entry.get("talents") or []
                        enchants = entry.get("enchants") or []

                        # convert loadout_updated_at to DATETIME string if present
                        lut = None
                        if meta.get("loadout_updated_at"):
                            try:
                                lut_dt = datetime.fromisoformat(str(meta["loadout_updated_at"]).replace("Z", "+00:00"))
                                lut = lut_dt.strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                lut = None

                        gated = False
                        try:
                            # determine integer DB season id
                            db_season = None
                            try:
                                if isinstance(season, int):
                                    db_season = int(season)
                                elif isinstance(season, str) and season.isdigit():
                                    db_season = int(season)
                            except Exception:
                                db_season = None

                            if db_season is None:
                                entry_region = meta.get("region") or (REGIONS[0] if REGIONS else "us")
                                if entry_region == "world" or entry_region not in REGIONS:
                                    entry_region = REGIONS[0] if REGIONS else "us"
                                if entry_region in season_id_cache:
                                    db_season = season_id_cache[entry_region]
                                else:
                                    try:
                                        sid = await get_current_season_id(session, entry_region)
                                        if sid:
                                            season_id_cache[entry_region] = int(sid)
                                            db_season = int(sid)
                                    except Exception:
                                        db_season = None

                            if db_season is None:
                                raise ValueError(f"Unable to determine numeric season id for DB for spec {spec_id}")

                            # map challenge id expected by DB
                            map_challenge_mode_id = meta.get("map_challenge_mode_id") or meta.get("zone_id")
                            if map_challenge_mode_id is None:
                                # nothing to insert for this dungeon
                                continue
                            # skip off-rotation dungeons: top players still have runs
                            # in previous-season dungeons, but storing those loadouts
                            # pollutes top_player_loadouts and skews the hero-tree badge.
                            if int(map_challenge_mode_id) not in CURRENT_DUNGEON_MAP_IDS:
                                continue

                            # Hold the write gate across this loadout's writes. The
                            # pause_point() between specs is not enough on its own:
                            # it is uncounted, so is_quiesced() could report idle
                            # mid-loadout and let ev_season_wipe truncate
                            # top_player_* while these inserts are still landing.
                            await WRITE_GATE.begin()
                            gated = True

                            # Delete existing meta (cascades children), then insert meta + children in a transaction
                            deleted = databaseConnector.delete_top_player_meta(conn, cursor, spec_id, r, int(map_challenge_mode_id))
                            GLOBAL_STATS.console_log(f"DEBUG delete_top_player_meta rowcount={deleted} spec={spec_id} rank={r} map_challenge_mode_id={map_challenge_mode_id}")

                            databaseConnector.insert_top_player_meta(
                                conn,
                                cursor,
                                spec_id,
                                db_season,
                                r,
                                int(map_challenge_mode_id),
                                meta.get("region"),
                                meta.get("character_id"),
                                meta.get("character_name"),
                                meta.get("realm"),
                                str(meta.get("loadout_key")),
                                lut,
                                meta.get("keystone_level"),
                                meta.get("loadout_text"),
                            )

                            # batch insert children - ensure season is numeric DB id
                            if items:
                                items_to_insert = []
                                for it in items:
                                    try:
                                        # it format: (spec_id, season, rank, slot, item_id, item_level, bonus_ids)
                                        bonus_ids = it[6] if len(it) > 6 else None
                                        items_to_insert.append((it[0], db_season, r, int(map_challenge_mode_id), it[3], it[4], it[5], bonus_ids))
                                    except Exception:
                                        continue
                                if items_to_insert:
                                    databaseConnector.insert_top_player_items_batch(conn, cursor, items_to_insert)

                            # insert enchantments (moved out of items table)
                            if enchants:
                                enchants_to_insert = []
                                for e in enchants:
                                    try:
                                        # e format: (spec_id, season, rank, slot, enchantment_id)
                                        enchants_to_insert.append((e[0], db_season, r, int(map_challenge_mode_id), e[3], e[4]))
                                    except Exception:
                                        continue
                                if enchants_to_insert:
                                    databaseConnector.insert_top_player_enchants_batch(conn, cursor, enchants_to_insert)

                            if gems:
                                # Aggregate gem usage counts across all items in this loadout
                                gem_counts = Counter()
                                for g in gems:
                                    try:
                                        # g format: (spec_id, season, rank, gem_item_id)
                                        gem_id = int(g[3])
                                        gem_counts[gem_id] += 1
                                    except Exception:
                                        continue
                                gems_to_insert = []
                                for gem_id, cnt in gem_counts.items():
                                    gems_to_insert.append((spec_id, db_season, r, int(map_challenge_mode_id), gem_id, cnt))
                                if gems_to_insert:
                                    databaseConnector.insert_top_player_gems_batch(conn, cursor, gems_to_insert)

                            if talents:
                                talents_to_insert = []
                                for t in talents:
                                    try:
                                        # t format: (spec_id, season, rank, node_id, node_rank, entry_id, spell_id)
                                        entry_id = t[5] if len(t) > 5 else None
                                        spell_id = t[6] if len(t) > 6 else None
                                        talents_to_insert.append((t[0], db_season, r, int(map_challenge_mode_id), t[3], t[4], entry_id, spell_id))
                                    except Exception:
                                        continue
                                if talents_to_insert:
                                    databaseConnector.insert_top_player_talents_batch(conn, cursor, talents_to_insert)

                            databaseConnector.commit_with_retry(conn)
                            await GLOBAL_STATS.increment("db_top_loadout_insert")
                        except Exception as e:
                            conn.rollback()
                            GLOBAL_STATS.console_log(f"DB error inserting top loadout for spec {spec_id} rank {r}: {e}")
                            traceback.print_exc()
                            continue
                        finally:
                            if gated:
                                WRITE_GATE.end()

            except Exception as e:
                GLOBAL_STATS.console_log(f"Error processing spec {spec_id}: {e}")
                traceback.print_exc()

    GLOBAL_STATS.console_log("Completed top-player loadouts collector.")



async def get_leaderboard(
    session: ClientSession, region: str, realm_id: int, dungeon_id: int, period_id: int
) -> dict:
    url = (
        f"{API_BASE.format(region=region)}/data/wow/connected-realm/"
        f"{realm_id}/mythic-leaderboard/{dungeon_id}/period/{period_id}"
    )
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    return await fetch_json(session, url, params, region)


async def get_equipment(
    session: ClientSession, region: str, realm_slug: str, name: str
) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/equipment"
    params = {"namespace": f"profile-{region}", "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or "equipped_items" not in data:
        return []
    return data.get("equipped_items", [])


MAINSTATS = ["strength", "agility", "intellect"]
NORMALSTATS = []
VALUESTATS = ["mastery", "lifesteal", "speed"]
RATINGBONUSSTATS = ["avoidance"]
CRITSTATS = ["spell_crit"]
HASTESTATS = ["spell_haste"]
VERSASTATS = ["versatility", "versatility_damage_done_bonus"]
RAWSTATS = []


def normalize_stats(data):
    normalized = {}
    versatility = {}
    for key, value in data.items():
        if key in MAINSTATS:
            if (
                not normalized.get("mainstat")
                or value["effective"] > normalized["mainstat"]
            ):
                normalized["mainstat"] = (
                    value.get("effective", 0) if value.get("effective", 0) > 0 else 0
                )
        elif key in CRITSTATS:
            if not normalized.get("crit") or value["rating_normalized"] > normalized[
                "crit"
            ].get("rating", 0):
                normalized["crit"] = {
                    "percent": value.get("value", 0)
                    if value.get("value", 0) > 0
                    else 0,
                    "rating": value.get("rating_normalized", 0)
                    if value.get("rating_normalized", 0) > 0
                    else 0,
                }
        elif key in HASTESTATS:
            if not normalized.get("haste") or value["rating_normalized"] > normalized[
                "haste"
            ].get("rating", 0):
                normalized["haste"] = {
                    "percent": value.get("value", 0)
                    if value.get("value", 0) > 0
                    else 0,
                    "rating": value.get("rating_normalized", 0)
                    if value.get("rating_normalized", 0) > 0
                    else 0,
                }
        elif key in VALUESTATS:
            normalized[key] = {
                "percent": value.get("value", 0) if value.get("value", 0) > 0 else 0,
                "rating": value.get("rating_normalized", 0)
                if value.get("rating_normalized", 0) > 0
                else 0,
            }
        elif key in RATINGBONUSSTATS:
            normalized[key] = {
                "percent": value.get("rating_bonus", 0)
                if value.get("rating_bonus", 0) > 0
                else 0,
                "rating": value.get("rating_normalized", 0)
                if value.get("rating_normalized", 0) > 0
                else 0,
            }
        elif key in RAWSTATS:
            normalized[key] = value
        elif key in VERSASTATS:
            versatility[key] = value
        elif key in NORMALSTATS:
            normalized[key] = value.get("effective", 0)

    normalized["versatility"] = {
        "rating": versatility.get("versatility", 0)
        if versatility.get("versatility", 0) > 0
        else 0,
        "percent": versatility.get("versatility_damage_done_bonus", 0)
        if versatility.get("versatility_damage_done_bonus", 0) > 0
        else 0,
    }
    return normalized


async def get_stats(
    session: ClientSession, region: str, realm_slug: str, name: str
) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/statistics"
    params = {"namespace": f"profile-{region}", "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data:
        return {}
    return normalize_stats(data)


async def get_specializations(
    session: ClientSession, region: str, realm_slug: str, name: str
) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/specializations"
    params = {"namespace": f"profile-{region}", "locale": LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or "specializations" not in data:
        return []
    return data.get("specializations", [])


def process_group(
    region,
    season,
    period_id,
    realm_id,
    run_hash,
):
    seen_file = RUNS_DIR / region / str(realm_id) / str(season) / f"{period_id}.csv"
    ensure_dir(seen_file.parent)
    with open(seen_file, "a", newline="") as f:
        csv.writer(f).writerow([run_hash])


async def realm_poller(region: str, session: ClientSession, max_keys):
    # fetch once
    current_season = await get_current_season_id(session, region)
    active_period = max(await get_season_periods(session, region, current_season))
    realms = await get_connected_realms(session, region)

    while True:
        for realm in realms:
            dungeons = await get_leaderboard_index(session, region, realm)
            await GLOBAL_STATS.increment("checked_realm")
            for dungeon in dungeons:
                GLOBAL_STATS.console_log(
                    f"{region} {realm} checking dungeon {dungeon['dungeon_id']} for period {active_period}"
                )
                # fetch the leaderboard
                await fetch_leaderboard_and_queue(
                    session,
                    current_season,
                    region,
                    realm,
                    active_period,
                    dungeon,
                    max_keys,
                )
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def simple_worker(name: str, session: ClientSession):
    """
    Pulls runs from simple_queue, writes the seen‐file+CSV, then
    enqueues a minimal run_obj into load_queue for the loader to persist.
    """
    global fetched_runs
    while not cancel_event.is_set():
        region, season, period_id, realm_id, dungeon, group = await simple_queue.get()
        try:
            # build the minimal payload
            run_obj = {
                "period_id": period_id,
                "run_hash": group["run_hash"],
                "season": season,
                "region": region,
                "realm": realm_id,
                "dungeon_id": dungeon["dungeon_id"],
                "keystone_level": group["keystone_level"],
                "duration": group["duration"],
                "timestamp": group["completed_timestamp"],
                "faction": group["majority_faction"],
                "members": [],
            }
            for member in group["members"]:
                run_obj["members"].append(
                    {
                        "spec_id": member["specialization"]["id"],
                        "loadout": None,
                        "hero_talent_id": None,
                        "class_talents": [],
                        "spec_talents": [],
                        "hero_talents": [],
                        "equipment": [],
                    }
                )

            # hand off to loader
            await database_queue.put(run_obj)
            await GLOBAL_STATS.increment("enqueued_runs")
            fetched_runs += 1

        except Exception:
            GLOBAL_STATS.console_log(f"[{name}] Error enqueuing simple run")
            traceback.print_exc()

        finally:
            simple_queue.task_done()


async def advanced_worker(name: str, session: ClientSession):
    """Does everything simple_worker does, then fetches equipment+all specs for each member."""
    global fetched_runs, fetched_profiles, enqueued_profiles
    while not cancel_event.is_set():
        region, season, period_id, realm_id, dungeon, group = await advanced_queue.get()
        try:
            # run metadata
            run_obj = dict(
                period_id=period_id,
                run_hash=group["run_hash"],
                season=season,
                region=region,
                realm=realm_id,
                dungeon_id=dungeon["dungeon_id"],
                keystone_level=group["keystone_level"],
                duration=group["duration"],
                timestamp=group["completed_timestamp"],
                faction=group["majority_faction"],
                members=[],
            )

            # for each member, fetch equipment & active spec
            for member in group["members"]:
                profile = member["profile"]
                profile_hash = hash_object(profile)
                if profile_hash in enqueued_profiles:
                    member_id = enqueued_profiles[profile_hash]
                    run_obj["members"].append(
                        {
                            "member_id": member_id["id"],
                        }
                    )
                else:
                    name_l = profile["name"].lower()
                    realm_slug = profile["realm"]["slug"].lower()

                    eq_data = await get_equipment(session, region, realm_slug, name_l)
                    spec_all = await get_specializations(
                        session, region, realm_slug, name_l
                    )
                    stats = await get_stats(session, region, realm_slug, name_l)
                    await GLOBAL_STATS.increment("fetched_profile")
                    try:
                        active_spec = next(
                            s
                            for s in spec_all
                            if s["specialization"]["id"]
                            == member["specialization"]["id"]
                        )
                    except StopIteration:
                        active_spec = {
                            "id": member["specialization"]["id"],
                            "name": "",
                            "loadouts": [],
                        }
                        await GLOBAL_STATS.increment("no_active_spec")
                    if active_spec.get("loadouts"):
                        active_loadout = next(
                            l for l in active_spec["loadouts"] if l["is_active"]
                        )
                    else:
                        active_loadout = {
                            "talent_loadout_code": None,
                            "selected_hero_talent_tree": {"id": None},
                            "selected_class_talents": [],
                            "selected_spec_talents": [],
                            "selected_hero_talents": [],
                        }

                    run_obj["members"].append(
                        {
                            "spec_id": member["specialization"]["id"],
                            "loadout": active_loadout["talent_loadout_code"],
                            "hero_talent_id": active_loadout[
                                "selected_hero_talent_tree"
                            ]["id"],
                            "class_talents": aggregate_talents(active_loadout.get("selected_class_talents", [])),
                            "spec_talents": aggregate_talents(active_loadout.get("selected_spec_talents", [])),
                            "hero_talents": aggregate_talents(active_loadout.get("selected_hero_talents", [])),
                            "equipment": [
                                {
                                    "slot": item["slot"]["type"],
                                    "item_id": item["item"]["id"],
                                    "item_level": item["level"]["value"],
                                    "enchantments": [
                                        e["enchantment_id"]
                                        for e in item.get("enchantments", [])
                                        if e
                                    ],
                                    "sockets": [
                                        (s["socket_type"]["type"], s["item"]["id"])
                                        for s in item.get("sockets", [])
                                        if s.get("socket_type") and s.get("item")
                                    ],
                                    "bonuses": [
                                        b for b in item.get("bonus_list", []) if b
                                    ],
                                }
                                for item in eq_data
                                if item.get("item")
                            ],
                            "stats": stats,
                        }
                    )
                    fetched_profiles += 1

            # enqueue the whole run object
            await database_queue.put(run_obj)
            await GLOBAL_STATS.increment("enqueued_runs")

        except Exception as e:
            GLOBAL_STATS.console_log(f"[{name}] fetch error: {e}", flush=True)
            traceback.print_exc()
        finally:
            advanced_queue.task_done()


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


async def process_batch(name, conn, cursor, batch, stats_collector=None):
    new_batch = []
    run_ids: list[int] = []

    # Try to insert each run individually, skip if already exists
    for r in batch:
        # Belt-and-suspenders against the partial-rollover hazard: never write a
        # run for a season the DB has already been wiped past (see SEASON_FLOOR).
        # The per-region skip in main() normally keeps these out of the queue, so
        # this only catches a stale in-flight item; mark it seen and move on.
        try:
            if SEASON_FLOOR and int(r["season"]) < SEASON_FLOOR:
                process_group(
                    r["region"], r["season"], r["period_id"], r["realm"], r["run_hash"]
                )
                continue
        except (TypeError, ValueError):
            pass
        try:
            # INSERT IGNORE will skip duplicates (requires IGNORE keyword)
            databaseConnector.insert_run(
                conn,
                cursor,
                r["season"],
                r["region"],
                r["dungeon_id"],
                r["keystone_level"],
                r["duration"],
                r["timestamp"],
                r["faction"],
            )
            # lastrowid == 0 means the row was ignored (duplicate)
            run_id = cursor.lastrowid
            if run_id:
                run_ids.append(run_id)
                new_batch.append(r)
                databaseConnector.commit_changes(conn)
            else:
                continue
        except Exception as e:
            GLOBAL_STATS.console_log(
                f"[{name}] "
                f"Error inserting run {r['season']}-{r['region']}-"
                f"{r['realm']}-{r['dungeon_id']}-{r['timestamp']}: {e}"
            )
            continue
    # nothing new?
    if not new_batch or len(new_batch) == 0:
        batch.clear()
        for r in batch:
            process_group(
                r["region"], r["season"], r["period_id"], r["realm"], r["run_hash"]
            )
        return

    if stats_collector:
        await stats_collector.increment("db_insert_run", len(run_ids))

    # now process members/etc only for new_batch
    batch = new_batch
    # members: separate existing vs new
    member_vals = []
    existing_member_ids = []
    counts = []
    # distinct talent dictionary sets seen in this batch: set_id -> list of
    # (set_id, tree, talent_id, rank) rows. One set_id covers all three trees for
    # a member; INSERT IGNORE de-duplicates against sets already in the DB.
    talent_sets: dict[bytes, list[tuple]] = {}
    # per-member talent picks, split by tree, counted for every new member (not
    # deduplicated by set) so the Discord embed shows a real class/spec/hero
    # breakdown. talent_sets below stays the distinct-set counter.
    class_talent_rows = 0
    spec_talent_rows = 0
    hero_talent_rows = 0
    for r in batch:
        counts.append(len(r["members"]))
        for m in r["members"]:
            if "member_id" in m:
                existing_member_ids.append(m["member_id"])
            else:
                tsid = commonUtils.talent_set_hash(
                    m["class_talents"], m["spec_talents"], m["hero_talents"]
                )
                member_vals.append(
                    (m["spec_id"], m["loadout"], m["hero_talent_id"], tsid)
                )
                # per-member talent picks (counted for every new member, not
                # deduplicated by set)
                class_talent_rows += len(m["class_talents"])
                spec_talent_rows += len(m["spec_talents"])
                hero_talent_rows += len(m["hero_talents"])
                if tsid is not None and tsid not in talent_sets:
                    rows = []
                    for t, rk in m["class_talents"]:
                        rows.append((tsid, 0, t, rk))
                    for t, rk in m["spec_talents"]:
                        rows.append((tsid, 1, t, rk))
                    for t, rk in m["hero_talents"]:
                        rows.append((tsid, 2, t, rk))
                    talent_sets[tsid] = rows
    # insert only new members
    if member_vals:
        first_new_id = databaseConnector.insert_members_batch(conn, cursor, member_vals)
        new_member_ids = list(range(first_new_id, first_new_id + len(member_vals)))
    else:
        new_member_ids = []
    databaseConnector.commit_changes(conn)

    # persist the distinct talent sets referenced by this batch's new members
    if talent_sets:
        ts_rows = [row for rows in talent_sets.values() for row in rows]
        for sub in chunked(ts_rows, BATCH_SIZE):
            databaseConnector.insert_talent_sets(conn, cursor, sub)
            databaseConnector.commit_changes(conn)

    if stats_collector and new_member_ids:
        await stats_collector.increment("db_insert_member", len(new_member_ids))
    # reconstruct full member_id list in original order
    mem_ids = []
    new_idx = 0
    exist_idx = 0
    for r in batch:
        for m in r["members"]:
            if "member_id" in m:
                mem_ids.append(existing_member_ids[exist_idx])
                exist_idx += 1
            else:
                mem_ids.append(new_member_ids[new_idx])
                new_idx += 1

    # run_members
    rm_vals = []
    idx = 0
    for run_idx, cnt in enumerate(counts):
        rid = run_ids[run_idx]
        for _ in range(cnt):
            rm_vals.append((rid, mem_ids[idx]))
            idx += 1
    databaseConnector.insert_run_members_batch(conn, cursor, rm_vals)
    databaseConnector.commit_changes(conn)
    # for only newly‑inserted members, insert equipment & stats. Talents are no
    # longer stored per member: they were hashed into the talent_sets dictionary
    # above and referenced by members.talent_set_id.
    ench_vals = []
    sock_vals = []
    stat_vals = []
    # distinct bonus dictionary sets seen in this batch: set_id -> list of
    # (set_id, bonus_id) rows. One set_id covers an equipped item's whole bonus-id
    # set; INSERT IGNORE de-duplicates against sets already in the DB.
    bonus_sets: dict[bytes, list[tuple]] = {}
    # per-item bonus id count (counted for every equipped item, not deduplicated
    # by set)
    bonus_id_rows = 0
    # offset into new_member_ids to map to batch members
    new_idx = 0

    for r in batch:
        for m in r["members"]:
            if "member_id" in m:
                continue  # skip existing
            mid = new_member_ids[new_idx]
            new_idx += 1
            # collect stats
            for stat, value in m.get("stats", {}).items():
                if isinstance(value, dict):
                    stat_vals.append(
                        (mid, stat, value.get("rating", 0), value.get("percent", 0))
                    )
                else:
                    stat_vals.append((mid, stat, value, None))
            # collect equipment
            for e in m["equipment"]:
                bsid = commonUtils.bonus_set_hash(e["bonuses"])
                eq_id = databaseConnector.insert_equipment(
                    conn, cursor, mid, e["slot"], e["item_id"], e["item_level"], bsid
                )
                for en in e["enchantments"]:
                    ench_vals.append((eq_id, en))
                for stype, iid in e["sockets"]:
                    sock_vals.append((eq_id, stype, iid))
                bonus_id_rows += len(e["bonuses"])
                if bsid is not None and bsid not in bonus_sets:
                    bonus_sets[bsid] = [
                        (bsid, b) for b in sorted({int(x) for x in e["bonuses"]})
                    ]
    if (
        len(talent_sets) > 0
        or len(ench_vals) > 0
        or len(sock_vals) > 0
        or len(bonus_sets) > 0
        or len(stat_vals) > 0
    ):
        GLOBAL_STATS.console_log(
            f"[{name}] Inserting equipment for {len(talent_sets)} talent sets, {len(ench_vals)} enchantments, {len(sock_vals)} sockets and {len(bonus_sets)} bonus sets and {len(stat_vals)} stats"
        )

    if stats_collector:
        GLOBAL_STATS.console_log("Incrementing stats with talents and equipment counts")
        if len(talent_sets) > 0:
            GLOBAL_STATS.console_log(f"Talent sets: {len(talent_sets)}")
            await stats_collector.increment("talent_sets", len(talent_sets))
        if class_talent_rows > 0:
            GLOBAL_STATS.console_log(f"Class talents: {class_talent_rows}")
            await stats_collector.increment("class_talents", class_talent_rows)
        if spec_talent_rows > 0:
            GLOBAL_STATS.console_log(f"Spec talents: {spec_talent_rows}")
            await stats_collector.increment("spec_talents", spec_talent_rows)
        if hero_talent_rows > 0:
            GLOBAL_STATS.console_log(f"Hero talents: {hero_talent_rows}")
            await stats_collector.increment("hero_talents", hero_talent_rows)
        if len(ench_vals) > 0:
            GLOBAL_STATS.console_log(f"Enchantments: {len(ench_vals)}")
            await stats_collector.increment("enchantments", len(ench_vals))
        if len(sock_vals) > 0:
            GLOBAL_STATS.console_log(f"Sockets: {len(sock_vals)}")
            await stats_collector.increment("sockets", len(sock_vals))
        if bonus_id_rows > 0:
            GLOBAL_STATS.console_log(f"Bonus ids: {bonus_id_rows}")
            await stats_collector.increment("bonus_ids", bonus_id_rows)
        if len(bonus_sets) > 0:
            GLOBAL_STATS.console_log(f"Bonus sets: {len(bonus_sets)}")
            await stats_collector.increment("bonus_sets", len(bonus_sets))
        if len(stat_vals) > 0:
            GLOBAL_STATS.console_log(f"Stats: {len(stat_vals)}")
            await stats_collector.increment("stats", len(stat_vals))

    if stat_vals and len(stat_vals) > 0:
        for sub in chunked(stat_vals, BATCH_SIZE):
            try:
                databaseConnector.insert_stats_batch(conn, cursor, sub)
                databaseConnector.commit_changes(conn)
            except Exception as e:
                GLOBAL_STATS.console_log(f"sub: {sub}")
                GLOBAL_STATS.console_log(f"Error inserting stats batch: {e}")
    if ench_vals and len(ench_vals) > 0:
        for sub in chunked(ench_vals, BATCH_SIZE):
            databaseConnector.insert_enchantments(conn, cursor, sub)
            databaseConnector.commit_changes(conn)
    if sock_vals and len(sock_vals) > 0:
        for sub in chunked(sock_vals, BATCH_SIZE):
            databaseConnector.insert_sockets(conn, cursor, sub)
            databaseConnector.commit_changes(conn)
    if bonus_sets:
        bs_rows = [row for rows in bonus_sets.values() for row in rows]
        for sub in chunked(bs_rows, BATCH_SIZE):
            databaseConnector.insert_bonus_sets(conn, cursor, sub)
            databaseConnector.commit_changes(conn)
    for r in batch:
        process_group(
            r["region"], r["season"], r["period_id"], r["realm"], r["run_hash"]
        )


async def database_worker(name: str):
    """
    Pull full run payloads from load_queue in batches and write them using
    databaseConnector's batch insert helpers.
    """
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        batch: list[dict] = []

        while True:
            # pull next payload or break on shutdown
            try:
                run_obj = await asyncio.wait_for(database_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                if cancel_event.is_set() and database_queue.empty():
                    break
                continue
            batch.append(run_obj)
            database_queue.task_done()
            if run_obj is None:
                if batch:
                    # same gating as the steady-state flush below: a shutdown
                    # landing mid-wipe must not write into tables the DB event
                    # is about to truncate
                    await WRITE_GATE.begin()
                    try:
                        await process_batch(name, conn, cursor, batch, GLOBAL_STATS)
                    finally:
                        WRITE_GATE.end()
                break
            if len(batch) >= BATCH_SIZE:
                await WRITE_GATE.begin()  # yield to a pending season wipe
                try:
                    await process_batch(name, conn, cursor, batch, GLOBAL_STATS)
                    batch.clear()
                except Exception as err:
                    GLOBAL_STATS.console_log(
                        f"{name}: batch failed skipping {len(batch)} rows: {err}"
                    )
                    traceback.print_exc()
                    conn.rollback()
                    continue
                finally:
                    WRITE_GATE.end()

        # `with closing(...)` returns the pooled connection exactly once on exit;
        # do not call conn.close() here or it double-returns and injects a surplus
        # connection into the shared pool ("Failed adding connection; queue is full").
        cursor.close()


async def wipe_watch():
    """Poll wipe_control and drive the collector side of the season-wipe handshake.

    While a wipe is pending (request_season > done_season) the write gate is paused
    and we ack collector_paused=1 once writers are quiesced; the DB event then does
    the blanket clear. Once it finishes we request a process restart rather than
    simply resuming — see the `armed_for` branch below. A failsafe resumes writers
    if a request lingers too long without completing so collection can never be
    halted indefinitely by an unapproved/stuck request.

    Every tick is individually guarded: this coroutine owns the only path that can
    un-pause the write gate, so it must never die. A transient DB error that killed
    it while paused would silently halt all collection (asyncio.gather in main()
    swallows the traceback) and take the failsafe down with it."""
    loop = asyncio.get_running_loop()
    paused_since = None
    failsafe_request = None  # request_season we've given up waiting on
    failsafe_until = 0.0  # monotonic deadline after which we retry it
    armed_for = None  # request_season this process actually quiesced for
    restarting = False  # leaving deliberately, with writers still gated

    try:
        while not cancel_event.is_set():
            try:
                state = await loop.run_in_executor(
                    None, databaseConnector.read_wipe_control
                )

                now_ms = int(time.time() * 1000)
                now_mono = time.monotonic()
                pending = bool(state) and state["request_season"] > state["done_season"]
                req = state["request_season"] if state else 0
                done = state["done_season"] if state else 0

                # Re-arm a request we previously gave up on, so a transient blocker
                # (typically a long nightly pipeline holding agg_pipeline) can't
                # strand the wipe for the lifetime of this process.
                if failsafe_request is not None and (
                    failsafe_request != req or now_mono >= failsafe_until
                ):
                    if failsafe_request == req:
                        GLOBAL_STATS.console_log(
                            f"wipe_watch: retrying season wipe request {req} after "
                            "the failsafe cooldown"
                        )
                    failsafe_request = None

                if not pending:
                    if armed_for is not None and done >= armed_for:
                        # The wipe we quiesced for completed. Season and active
                        # period were resolved once at startup, so resuming would
                        # re-insert rows for the season that was just cleared —
                        # and wipe_control.done_season now blocks any further
                        # wipe, so those rows would survive the whole season.
                        # Restart instead and pick the new season up cleanly.
                        #
                        # The gate deliberately stays paused: everything still
                        # queued is old-season work, and runner() cancels us
                        # shortly anyway.
                        restarting = True
                        GLOBAL_STATS.console_log(
                            f"wipe_watch: season wipe {armed_for} completed; "
                            "requesting restart to pick up the new season/period"
                        )
                        await loop.run_in_executor(
                            None,
                            databaseConnector.set_collector_wipe_state,
                            False,
                            now_ms,
                        )
                        restart_event.set()
                        return
                    # nothing (or no longer anything) requested
                    if WRITE_GATE.paused:
                        WRITE_GATE.resume()
                        GLOBAL_STATS.console_log(
                            "wipe_watch: wipe cleared; resuming DB writers"
                        )
                    paused_since = None
                    armed_for = None
                    await loop.run_in_executor(
                        None, databaseConnector.set_collector_wipe_state, False, now_ms
                    )
                elif failsafe_request == req:
                    # still inside the cooldown for this request; stay resumed and
                    # report not paused so the DB event keeps waiting
                    await loop.run_in_executor(
                        None, databaseConnector.set_collector_wipe_state, False, now_ms
                    )
                else:
                    if not WRITE_GATE.paused:
                        WRITE_GATE.pause()
                        GLOBAL_STATS.console_log(
                            f"wipe_watch: season wipe requested (season {req}); "
                            "pausing DB writers"
                        )
                    # also covers the case where main() pre-paused us at startup
                    if paused_since is None:
                        paused_since = now_mono
                    armed_for = req

                    if (now_mono - paused_since) > WIPE_MAX_PAUSE_SECONDS:
                        GLOBAL_STATS.console_log(
                            "ERROR: wipe_watch failsafe tripped — paused > "
                            f"{WIPE_MAX_PAUSE_SECONDS:.0f}s without the wipe completing; "
                            "resuming writers and retrying in "
                            f"{WIPE_FAILSAFE_RETRY_SECONDS:.0f}s"
                        )
                        WRITE_GATE.resume()
                        paused_since = None
                        armed_for = None
                        failsafe_request = req
                        failsafe_until = now_mono + WIPE_FAILSAFE_RETRY_SECONDS
                        await loop.run_in_executor(
                            None,
                            databaseConnector.set_collector_wipe_state,
                            False,
                            now_ms,
                        )
                    else:
                        # report quiescence so the DB event may proceed once writers idle
                        await loop.run_in_executor(
                            None,
                            databaseConnector.set_collector_wipe_state,
                            WRITE_GATE.is_quiesced(),
                            now_ms,
                        )
            except Exception as e:
                GLOBAL_STATS.console_log(f"wipe_watch: poll failed: {e}")
                traceback.print_exc()

            await asyncio.sleep(WIPE_POLL_SECONDS)
    finally:
        # Nothing else can un-pause the gate. If we are leaving for any reason
        # other than a deliberate post-wipe restart, writers must not stay blocked.
        if WRITE_GATE.paused and not restarting:
            WRITE_GATE.resume()
            GLOBAL_STATS.console_log(
                "ERROR: wipe_watch exited with the write gate still paused; "
                "resuming DB writers"
            )


async def main():
    connector = aiohttp.TCPConnector(
        limit=100,
        keepalive_timeout=75,
        force_close=False,
        enable_cleanup_closed=True,
        ssl=False,
    )
    timeout = ClientTimeout(total=60)
    retry_options = ExponentialRetry(
        attempts=5,  # total tries (initial + 4 retries)
        start_timeout=1,  # initial backoff delay in seconds
        max_timeout=30,  # maximum backoff
        statuses={429, 500, 502, 503, 504},  # HTTP statuses to retry
        exceptions={  # also retry on these client errors
            asyncio.TimeoutError,
            aiohttp.ClientConnectionError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientPayloadError,
            aiohttp.ClientOSError,
            ClientConnectionResetError,
        },
    )
    async with RetryClient(
        connector=connector,
        timeout=timeout,
        retry_options=retry_options,
        raise_for_status=False,
    ) as session:
        GLOBAL_STATS.console_log(
            f"Starting data collection for regions: {', '.join(REGIONS)}"
        )

        processed_runs = await load_processed_runs(session)
        GLOBAL_STATS.console_log(
            f"Loaded {len(processed_runs)} previously processed runs."
        )
        max_keys = await get_max_keys_by_dungeon(session)
        GLOBAL_STATS.console_log(f"Fetched max keys for dungeons: {max_keys}")
        reporter = discordHandler.DiscordReporter(
            session, GLOBAL_STATS, interval_seconds=300
        )
        await reporter.start()

        # If a season wipe is already pending, start writers paused so we never
        # write into tables the DB event is about to clear (closes the startup gap
        # before wipe_watch's first poll). This has to happen before the first
        # writer task is created: every `await` below yields the loop, and a task
        # created earlier would sail straight through its pause_point().
        global SEASON_FLOOR
        try:
            _wipe_state = await asyncio.get_running_loop().run_in_executor(
                None, databaseConnector.read_wipe_control
            )
            if _wipe_state:
                # The season the DB was last wiped INTO: never collect/write a
                # region still on an older season (see SEASON_FLOOR docs).
                SEASON_FLOOR = int(_wipe_state.get("done_season") or 0)
                if SEASON_FLOOR:
                    GLOBAL_STATS.console_log(
                        f"Season floor set to {SEASON_FLOOR} from wipe_control; "
                        "regions below it will be skipped until they roll over"
                    )
            if _wipe_state and _wipe_state["request_season"] > _wipe_state["done_season"]:
                WRITE_GATE.pause()
                GLOBAL_STATS.console_log(
                    "Season wipe already pending at startup; DB writers start paused"
                )
        except Exception as e:
            GLOBAL_STATS.console_log(f"Could not read initial wipe state: {e}")

        tasks = []
        for region in REGIONS:
            GLOBAL_STATS.console_log(f"Processing region: {region}")
            if cancel_event.is_set():
                GLOBAL_STATS.console_log(f"{region} - cancellation requested, stopping")
                return
            current_season = await get_current_season_id(session, region)
            if current_season is None:
                GLOBAL_STATS.console_log(f"{region} - no season data, skipping")
                continue

            # Partial-rollover guard: a region still on a season the DB has already
            # been wiped past must not be collected — its old-season runs would
            # repopulate the cleared tables and outlive the new season (done_season
            # blocks any further wipe). It is picked up on a later restart once it
            # too rolls over. See SEASON_FLOOR.
            if SEASON_FLOOR and int(current_season) < SEASON_FLOOR:
                GLOBAL_STATS.console_log(
                    f"{region} - still on season {current_season}, below wiped "
                    f"season floor {SEASON_FLOOR}; skipping until it rolls over"
                )
                continue

            # anything under data/runs/<region>/*/<season> that isn't current_season, 0 or "nil" can go

            region_dir = RUNS_DIR / region
            if region_dir.exists():
                for realm_dir in region_dir.iterdir():
                    if not realm_dir.is_dir():
                        continue
                    for season_dir in realm_dir.iterdir():
                        name = season_dir.name
                        # skip our current season, "0", or "nil"
                        if current_season == 0 or int(name) == current_season:
                            continue
                        # delete the entire season folder
                        try:
                            shutil.rmtree(season_dir)
                            GLOBAL_STATS.console_log(
                                f"Deleted old season folder: {season_dir}"
                            )
                        except Exception as e:
                            GLOBAL_STATS.console_log(
                                f"Warning: could not delete {season_dir}: {e}"
                            )

            all_periods = await get_season_periods(session, region, current_season)
            if not all_periods:
                GLOBAL_STATS.console_log(f"{region} - no periods, skipping")
                continue

            realms = await get_connected_realms(session, region)
            if not realms:
                GLOBAL_STATS.console_log(f"{region} - no realms, skipping")
                continue

            # For each realm, create its single worker
            tasks.append(asyncio.create_task(realm_poller(region, session, max_keys)))

        tasks.append(asyncio.create_task(route_poller_task(session)))
        tasks.append(asyncio.create_task(run_raiderio_top_loadouts(session)))
        # Season-rollover wipe handshake: pauses DB writers while a wipe runs.
        tasks.append(asyncio.create_task(wipe_watch()))

        # SimulationCraft per-slot BiS collector (runs continuously, one spec at a
        # time). Gated behind SIMC_ENABLED so deployments without a simc image /
        # docker socket can opt out.
        if getenv_clean("SIMC_ENABLED", "true").lower() in ("1", "true", "yes"):
            try:
                simc_season = await get_current_season_id(session, REGIONS[0])
            except Exception as e:
                GLOBAL_STATS.console_log(f"simc: could not resolve season: {e}")
                simc_season = None
            tasks.append(
                asyncio.create_task(
                    simcBis.run_simc_bis(
                        session,
                        cancel_event=cancel_event,
                        stats=GLOBAL_STATS,
                        get_season=(lambda conn, cursor: simc_season) if simc_season else None,
                        reporter=reporter,
                        pause_check=WRITE_GATE.pause_point,
                        write_gate=WRITE_GATE,
                    )
                )
            )

        GLOBAL_STATS.console_log(
            f"Started {len(tasks)} tasks for processing realms across all regions."
        )
        simple_workers = [
            asyncio.create_task(simple_worker(f"simple-{i}", session))
            for i in range(WORKERS_PER_REALM)
        ]
        advanced_workers = [
            asyncio.create_task(advanced_worker(f"adv-{i}", session))
            for i in range(WORKERS_PER_REALM)
        ]
        database_workers = [
            asyncio.create_task(database_worker(f"db-{i}"))
            for i in range(DATABASE_WORKERS)
        ]
        
        database_workers.append(asyncio.create_task(route_db_worker("route-db-0")))

        await asyncio.gather(*tasks, return_exceptions=True)
        # Wait for all queues to be processed
        GLOBAL_STATS.console_log(
            "All work enqueued, waiting for queues to finish processing…"
        )
        GLOBAL_STATS.console_log(f"Simple queue size: {simple_queue.qsize()}")
        GLOBAL_STATS.console_log(f"Advanced queue size: {advanced_queue.qsize()}")
        await simple_queue.join()
        for w in simple_workers:
            w.cancel()
        await asyncio.gather(*simple_workers, return_exceptions=True)
        GLOBAL_STATS.console_log(
            f"Finished processing simple queue waiting for {advanced_queue.qsize()} runs in advanced queue..."
        )
        await advanced_queue.join()
        for w in advanced_workers:
            w.cancel()
        await asyncio.gather(*advanced_workers, return_exceptions=True)
        GLOBAL_STATS.console_log(
            "Finished processing advanced queue, waiting for database workers to finish…"
        )
        await database_queue.join()
        await route_db_queue.join()
        for w in database_workers:
            w.cancel()
        await reporter.stop()
        await asyncio.gather(*database_workers, return_exceptions=True)


async def timeout_watcher(collector_task: asyncio.Task):
    """Sleep for GHA_TIMEOUT seconds, then signal cancellation."""
    GLOBAL_STATS.console_log(f"Starting timeout watcher for {GHA_TIMEOUT} seconds…")
    await asyncio.sleep(GHA_TIMEOUT)
    GLOBAL_STATS.console_log("Soft timeout reached; signaling cancellation…")
    cancel_event.set()

    # give the collector up to 30 min to wind down
    await asyncio.sleep(HARD_TIMEOUT - GHA_TIMEOUT)
    shutdown_event.set()
    await asyncio.sleep(300)  # give it 5 minutes to finish up
    collector_task.cancel()


async def runner():
    # Kick off both main collector and the timeout watcher
    GLOBAL_STATS.console_log("Starting data collection runner…")
    collector = asyncio.create_task(main(), name="collector")
    timer = asyncio.create_task(timeout_watcher(collector), name="timeout_watcher")
    # A completed season wipe asks for a restart: season and active period are
    # resolved once at startup, so the process has to come back to see the new
    # ones. realm_poller loops forever and never observes cancel_event, so the
    # only reliable wind-down is to cancel the collector task ourselves.
    restart = asyncio.create_task(restart_event.wait(), name="restart_watcher")

    # Wait until main() completes, the timer fires, or a restart is requested
    done, pending = await asyncio.wait(
        {collector, timer, restart}, return_when=asyncio.FIRST_COMPLETED
    )

    if restart in done:
        GLOBAL_STATS.console_log(
            "Restart requested (season wipe completed); winding down so the "
            "container supervisor can restart us…"
        )
        cancel_event.set()
        shutdown_event.set()
        timer.cancel()
        try:
            await asyncio.wait_for(collector, timeout=WIPE_RESTART_GRACE_SECONDS)
        except asyncio.TimeoutError:
            GLOBAL_STATS.console_log(
                f"Collector did not wind down within {WIPE_RESTART_GRACE_SECONDS:.0f}s; "
                "cancelling it."
            )
            collector.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await collector
        with suppress(asyncio.CancelledError):
            await timer
        return

    restart.cancel()
    with suppress(asyncio.CancelledError):
        await restart

    if timer in done:
        # Timer fired -> we set cancel_event above; now wait for collector to finish cleanly
        GLOBAL_STATS.console_log("Waiting for in-flight runs to complete…")
        await collector
    else:
        # main() finished before the timeout
        GLOBAL_STATS.console_log("Data collection finished before timeout.")
        timer.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(runner())
    except asyncio.CancelledError:
        GLOBAL_STATS.console_log("Data collection cancelled by timeout.")
    except Exception as e:
        GLOBAL_STATS.console_log(f"Error during data collection: {e}")
    GLOBAL_STATS.console_log("Committing changes to the database…")
    GLOBAL_STATS.console_log("All tasks done.")
    GLOBAL_STATS.console_log(
        f"Fetched runs: {fetched_runs}, Fetched profiles: {fetched_profiles}"
    )
