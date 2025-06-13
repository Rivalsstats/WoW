import os
import json
import asyncio
import hashlib
import time
import datetime
import csv
from pathlib import Path
from aiohttp import ClientSession, ClientTimeout, BasicAuth, ClientResponseError
from aiolimiter import AsyncLimiter
import random

# Configuration
REGIONS = os.environ.get('REGIONS', 'us,eu,kr,tw').split(',')
API_BASE = 'https://{region}.api.blizzard.com'
OAUTH_BASE = 'https://{region}.battle.net'
NAMESPACE_DYNAMIC = 'dynamic-{region}'
LOCALE = os.environ.get('LOCALE', 'en_US')
DATA_DIR = Path('data')
RUNS_DIR = DATA_DIR / 'runs'

# Rate limits
per_second_limiter = AsyncLimiter(90, 1)
per_hour_limiter = AsyncLimiter(29500, 3600)

# Queue settings
QUEUE_MAXSIZE = 1000
GHA_TIMEOUT = 4 * 3600  # 5 hours
cancel_event = asyncio.Event()
MAX_GLOBAL_BACKOFF = 60.0

# global_backoff_until is a POSIX timestamp; workers wait until time.time() >= this
global_backoff_until = 0.0
backoff_lock = asyncio.Lock()
base_backoff = 1.0  # base backoff in seconds

# stat variables
fetched_runs = 0
fetched_profiles = 0

# Blizzard OAuth
CLIENT_ID = os.getenv('BLIZ_CLIENT_ID')
CLIENT_SECRET = os.getenv('BLIZ_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("BLIZ_CLIENT_ID and BLIZ_CLIENT_SECRET must be set in the environment.")

_token_cache = {}

# In-memory cache for fetched profiles
enqueued_profiles = set()

# Utils
def hash_object(obj: dict) -> str:
    payload = json.dumps(obj, sort_keys=True).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

async def get_access_token(session: ClientSession, region: str) -> str:
    cache = _token_cache.get(region)
    if cache and cache['expires_at'] > time.time() + 60:
        return cache['access_token']
    url = f"{OAUTH_BASE.format(region=region)}/oauth/token"
    async with session.post(
        url,
        auth=BasicAuth(CLIENT_ID, CLIENT_SECRET),
        data={'grant_type': 'client_credentials'}
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        token = data['access_token']
        expires = data.get('expires_in', 0)
        _token_cache[region] = {'access_token': token, 'expires_at': time.time() + expires}
        return token


async def fetch_json(
    session: ClientSession,
    url: str,
    params: dict,
    region: str,
    retries: int = 3
) -> dict | None:
    """
    Retry up to `retries` times on 429, but always honor one shared global backoff.
    """
    global global_backoff_until
    token = await get_access_token(session, region)
    headers = {'Authorization': f'Bearer {token}'}
    
    for attempt in range(1, retries + 1):
        
        now = time.time()
        if now < global_backoff_until:
            wait = global_backoff_until - now
            await asyncio.sleep(wait)

       
        await per_second_limiter.acquire()
        await per_hour_limiter.acquire()

    

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

        except ClientResponseError as e:
            if e.status == 429:
                # determine how long to back off: use Retry-After if given, else MAX_GLOBAL_BACKOFF
                ra = e.headers.get('Retry-After')
                backoff = float(ra) if ra else MAX_GLOBAL_BACKOFF

                # set global backoff expiry
                expiry = time.time() + backoff
                async with backoff_lock:
                    global_backoff_until = max(global_backoff_until, expiry)

                until_iso = datetime.datetime.fromtimestamp(
                    global_backoff_until, datetime.timezone.utc
                ).isoformat()

                # if we still have retries left, loop again (which will sleep until expiry)
                if attempt < retries:
                    continue
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] "
                      f"[429] OUT OF Retries for {url} after {retries} attempts")
                # out of retries
                return None
            elif 500 <= e.status < 600:
                # compute exponential backoff with jitter
                delay = min(base_backoff * 2**(attempt - 1), max_backoff)
                delay = delay * random.uniform(0.5, 1.5)
                print(f"[{datetime.datetime.utcnow().isoformat()}] [{e.status}] transient error, retrying in {delay:.1f}s (attempt {attempt}/{retries})")
                if attempt < retries:
                    await asyncio.sleep(delay)
                    continue
                # last attempt, give up
                print(f"[{datetime.datetime.utcnow().isoformat()}] [{e.status}] out of retries for {url}")
                return None
            elif e.status == 404:
                return None  # Profile private

            else:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] [{e.status}] Error fetching {url}: {e}")
                return None

    return None


async def get_connected_realms(session: ClientSession, region: str) -> list[int]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or 'connected_realms' not in data:
        return []
    return [int(r['href'].split('/')[-1].split('?')[0]) for r in data['connected_realms']]

async def get_current_season_id(session: ClientSession, region: str) -> int:
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or not data.get('seasons'):
        return None
    return data['current_season']['id']

async def get_season_periods(session: ClientSession, region: str, season_id: int) -> list[int]:
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/{season_id}"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or 'periods' not in data:
        return []
    return [p['id'] for p in data['periods']]

async def get_leaderboard_index(session: ClientSession, region: str, realm_id: int) -> list[dict]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/{realm_id}/mythic-leaderboard/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or 'current_leaderboards' not in data:
        return []
    return [{'dungeon_id': lb['id'], 'name': lb['name']} for lb in data['current_leaderboards']]

async def get_leaderboard(session: ClientSession, region: str, realm_id: int, dungeon_id: int, period_id: int) -> dict:
    url = (f"{API_BASE.format(region=region)}/data/wow/connected-realm/" 
           f"{realm_id}/mythic-leaderboard/{dungeon_id}/period/{period_id}")
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    return await fetch_json(session, url, params, region)

async def get_equipment(session: ClientSession, region: str, realm_slug: str, name: str) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/equipment"
    params = {'namespace': f'profile-{region}', 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or 'equipped_items' not in data:
        return []
    return data.get('equipped_items', [])

async def get_specializations(session: ClientSession, region: str, realm_slug: str, name: str) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/specializations"
    params = {'namespace': f'profile-{region}', 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    if not data or 'specializations' not in data:
        return []
    return data.get('specializations', [])

# Worker logic
async def process_run(session: ClientSession, region: str, season: int, period_id: int, realm_id: int, dungeon: dict):
    global fetched_runs, fetched_profiles, enqueued_profiles
    lb = await get_leaderboard(session, region, realm_id, dungeon['dungeon_id'], period_id)
    if lb is None:
        return
    period_dir = DATA_DIR / region / str(realm_id) / str(season) / str(period_id)
    ensure_dir(period_dir)
    # runs.csv setup
    runs_csv = period_dir / 'runs.csv'
    if not runs_csv.exists():
        with open(runs_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hash', 'dungeon_id', 'keystone_level', 'duration', 'timestamp', 'faction', 'members'])
    # seen
    seen_file = RUNS_DIR / region / str(realm_id) / str(season) / f"{period_id}.csv"
    ensure_dir(seen_file.parent)
    seen: set[str] = set()

    if seen_file.exists():
         with open(seen_file, newline='') as f:
            reader = csv.reader(f)
            # if you wrote a header, skip it; otherwise just read all rows
            for row in reader:
                if row:
                    seen.add(row[0])
    else:
        with open(seen_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['run_hash'])

    ensure_dir(seen_file.parent)

    for group in lb.get('leading_groups', []):
        if cancel_event.is_set():
            break
        run_hash = hash_object({
            'realm': realm_id,
            'dungeon': dungeon['dungeon_id'],
            'period': period_id,
            'members': [m['profile']['id'] for m in group['members']],
            'timestamp': group['completed_timestamp']
        })
        if run_hash in seen:
            continue
        seen.add(run_hash)
        # append CSV row
        
        with open(seen_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([run_hash])
        
        with open(runs_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            members_hashes = ';'.join(hash_object(m['profile']) for m in group['members'])
            writer.writerow([run_hash, dungeon['dungeon_id'], group['keystone_level'], group['duration'], group['completed_timestamp'], group['members'][0]['faction']['type'], members_hashes])

        fetched_runs = fetched_runs + 1
        # fetch unique profiles
        for member in group['members']:
            if cancel_event.is_set():
                break
            profile = member['profile']
            profile['timestamp'] = datetime.datetime.now(datetime.timezone.utc)
            profile_hash = hash_object(profile)
            if profile_hash in enqueued_profiles:
                continue
            enqueued_profiles.add(profile_hash)
            name = profile['name'].lower()
            realm_slug = profile['realm']['slug'].lower()
            # equipment folder
            eq_dir = period_dir / 'equipment'
            ensure_dir(eq_dir)
            eq_data = await get_equipment(session, region, realm_slug, name)
            (eq_dir / f'{profile_hash}.json').write_text(json.dumps(eq_data))
            # specializations folder
            spec_dir = period_dir / 'specializations'
            ensure_dir(spec_dir)
            raw_specs = await get_specializations(session, region, realm_slug, name)

            spec_data = {
                "specializations": raw_specs,
                "active_specialization": member['specialization']['id']
            }
            (spec_dir / f'{profile_hash}.json').write_text(json.dumps(spec_data))
            fetched_profiles = fetched_profiles + 1
async def worker(name: str, queue: asyncio.Queue, session: ClientSession):
    try:
        while True:
            try:
                region, season, period_id, realm_id, dungeon = await asyncio.wait_for(queue.get(), timeout=1)
            except asyncio.TimeoutError:
                # No work this second — re‐check for cancellation
                if cancel_event.is_set():
                    break
                continue

            try:
                await process_run(session, region, season, period_id, realm_id, dungeon)
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] [{name}] Error: {e}")
            finally:
                queue.task_done()

    except asyncio.CancelledError:
        return

async def main():
    timeout = ClientTimeout(total=600)
    async with ClientSession(timeout=timeout) as session:
        # Track all per-realm queues and workers
        print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] Starting data collection for regions: {', '.join(REGIONS)}")
        realm_queues: dict[tuple[str, int], asyncio.Queue] = {}
        realm_workers: list[asyncio.Task] = []

        for region in REGIONS:
            current_season = await get_current_season_id(session, region)
            if current_season is None:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] {region} – no season data, skipping")
                continue

            periods = await get_season_periods(session, region, current_season)
            if not periods:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] {region} – no periods, skipping")
                continue

            realms = await get_connected_realms(session, region)
            if not realms:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] {region} – no realms, skipping")
                continue

            # For each realm, create its own queue and single worker
            for realm in realms:
                print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] {region} – processing realm {realm}")
                q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
                realm_queues[(region, realm)] = q
                task = asyncio.create_task(worker(f"{region}-{realm}", q, session))
                realm_workers.append(task)

                # Enqueue all period/dungeon work onto this realm's queue
                for period in periods:
                    dungeons = await get_leaderboard_index(session, region, realm)
                    for dungeon in dungeons:
                        await q.put((region, current_season, period, realm, dungeon))

        # Wait for all queues to be processed
        await asyncio.gather(*(q.join() for q in realm_queues.values()))

        # Cancel and await all workers
        for w in realm_workers:
            w.cancel()
        await asyncio.gather(*realm_workers, return_exceptions=True)
if __name__ == '__main__':
    try:
        asyncio.run(
            asyncio.wait_for(main(), timeout=GHA_TIMEOUT)
        )
        print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}] All tasks completed successfully.")
        print(f"Fetched runs: {fetched_runs}, Fetched profiles: {fetched_profiles}")
    except asyncio.TimeoutError:
        print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}]  {GHA_TIMEOUT}s elapsed — canceling all tasks…")
        # set the flag (in case any workers are mid-queue.get)
        print(f"Fetched runs: {fetched_runs}, Fetched profiles: {fetched_profiles}")
        cancel_event.set()
