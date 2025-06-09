import os
import json
import asyncio
import hashlib
import time
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
per_second_limiter = AsyncLimiter(95, 1)
per_hour_limiter = AsyncLimiter(29500, 3600)

# Queue settings
QUEUE_MAXSIZE = 1000
GHA_TIMEOUT = 180#5 * 3600 + 1800 # 5 and a half hours
cancel_event = asyncio.Event()
global_backoff = 0
GLOBAL_DECAY = 0.75


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
    global global_backoff

    
    if global_backoff > 0:
        print(f"[GLOBAL BACKOFF] sleeping {global_backoff:.1f}s before request")
        await asyncio.sleep(global_backoff)

    
    token = await get_access_token(session, region)
    headers = {'Authorization': f'Bearer {token}'}

    
    local_backoff = 1.0

    for attempt in range(1, retries + 1):
        
        await per_second_limiter.acquire()
        await per_hour_limiter.acquire()

        try:
            
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json()

                
                if global_backoff > 0:
                    global_backoff = max(global_backoff * GLOBAL_DECAY, 0.0)
                return data

        except ClientResponseError as e:
            
            if e.status == 429 and attempt < retries:
                ra = e.headers.get('Retry-After')
                wait = float(ra) if ra else local_backoff + random.random()
                print(f"[429] {url} — retrying in {wait:.1f}s (attempt {attempt}/{retries})")
                await asyncio.sleep(wait)
                local_backoff *= 2
                continue

            if e.status == 429:
                
                if global_backoff < local_backoff:
                    global_backoff = local_backoff
                else:
                    global_backoff *= 2
                print(f"[429-GIVEUP] {url} — setting global backoff to {global_backoff:.1f}s")
                return None
            
            raise

    return None

async def get_connected_realms(session: ClientSession, region: str) -> list[int]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    return [int(r['href'].split('/')[-1].split('?')[0]) for r in data['connected_realms']]

async def get_current_season_id(session: ClientSession, region: str) -> int:
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    return max(s['id'] for s in data['seasons'])

async def get_season_periods(session: ClientSession, region: str, season_id: int) -> list[int]:
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/{season_id}"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    return [p['id'] for p in data['periods']]

async def get_leaderboard_index(session: ClientSession, region: str, realm_id: int) -> list[dict]:
    url = f"{API_BASE.format(region=region)}/data/wow/connected-realm/{realm_id}/mythic-leaderboard/index"
    params = {'namespace': NAMESPACE_DYNAMIC.format(region=region), 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
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
    return data.get('equipped_items', [])

async def get_specializations(session: ClientSession, region: str, realm_slug: str, name: str) -> list:
    url = f"{API_BASE.format(region=region)}/profile/wow/character/{realm_slug}/{name}/specializations"
    params = {'namespace': f'profile-{region}', 'locale': LOCALE}
    data = await fetch_json(session, url, params, region)
    return data.get('specializations', [])

# Worker logic
async def process_run(session: ClientSession, region: str, period_id: int, realm_id: int, dungeon: dict):
    lb = await get_leaderboard(session, region, realm_id, dungeon['dungeon_id'], period_id)
    if lb is None:
        return
    period_dir = DATA_DIR / region / str(realm_id)/ str(period_id)
    ensure_dir(period_dir)
    # runs.csv setup
    runs_csv = period_dir / 'runs.csv'
    if not runs_csv.exists():
        with open(runs_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hash', 'dungeon_id', 'keystone_level', 'duration', 'timestamp', 'faction', 'members'])
    # seen
    seen_file = RUNS_DIR /region/ str(realm_id) / f"{period_id}.json"
    if seen_file.exists():
        seen = json.loads(seen_file.read_text())
    else:
        seen = []

    ensure_dir(seen_file.parent)

    seen = json.loads(seen_file.read_text()) if seen_file.exists() else []

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
        seen.append(run_hash)
        seen_file.write_text(json.dumps(seen))
        # append CSV row
        with open(runs_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            members_hashes = ';'.join(hash_object(m['profile']) for m in group['members'])
            writer.writerow([run_hash, dungeon['dungeon_id'], group['keystone_level'], group['duration'], group['completed_timestamp'], group['members'][0]['faction']['type'], members_hashes])

        # fetch unique profiles
        for member in group['members']:
            if cancel_event.is_set():
                break
            profile = member['profile']
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
            spec_data = await get_specializations(session, region, realm_slug, name)
            (spec_dir / f'{profile_hash}.json').write_text(json.dumps(spec_data))
        print(f"Processed run {run_hash} for {region} - Period: {period_id}, Realm: {realm_id}, Dungeon: {dungeon['name']}")

async def worker(name: str, queue: asyncio.Queue, session: ClientSession):
    try:
        while True:
            try:
                region, period_id, realm_id, dungeon = await asyncio.wait_for(queue.get(), timeout=1)
            except asyncio.TimeoutError:
                # No work this second — re‐check for cancellation
                if cancel_event.is_set():
                    break
                continue

            try:
                await process_run(session, region, period_id, realm_id, dungeon)
            except Exception as e:
                print(f"[{name}] Error: {e}")
            finally:
                queue.task_done()

    except asyncio.CancelledError:
        return

async def main():
    timeout = ClientTimeout(total=600)
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    async with ClientSession(timeout=timeout) as session:
        workers = [asyncio.create_task(worker(f"w{i}", queue, session)) for i in range(20)]
        for region in REGIONS:
            current_season = await get_current_season_id(session, region)
            print(f"{region} - Current Season ID: {current_season}")
            periods = await get_season_periods(session, region, current_season)
            print(f"{region} - Periods: {periods}")
            realms = await get_connected_realms(session, region)
            print(f"{region} - Realms: {realms}")
            for period in periods:
                for realm in realms:
                    print(f"Enqueuing {region} - Period: {period}, Realm: {realm}")
                    dungeons = await get_leaderboard_index(session, region, realm)
                    for dungeon in dungeons:
                        await queue.put((region, period, realm, dungeon))

        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

if __name__ == '__main__':
    try:
        asyncio.run(
            asyncio.wait_for(main(), timeout=GHA_TIMEOUT)
        )
    except asyncio.TimeoutError:
        print(f"⏱️ {GHA_TIMEOUT}s elapsed — canceling all tasks…")
        # set the flag (in case any workers are mid-queue.get)
        cancel_event.set()
