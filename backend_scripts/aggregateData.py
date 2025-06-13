import argparse, os, csv, json
from collections import defaultdict
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def parse_runs_async(season_path, executor):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: list(parse_runs(season_path)))

async def load_specialization_async(season_path, member_hash, executor):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: load_specialization(season_path, member_hash))

async def load_equipment_async(season_path, member_hash, executor):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: load_equipment(season_path, member_hash))

async def process_run(season_path, run, stats, executor, sem):
    d = int(run['dungeon_id'])
    k = int(run['keystone_level'])
    key = (d, k)

    # Ensure thread-safe stats entry
    if key not in stats:
        stats[key] = {
            'total_runs': 0,
            'spec_counts': defaultdict(int),
            'talent_counts': defaultdict(lambda: defaultdict(int)),
            'item_bonus': defaultdict(set)
        }
    rec = stats[key]

    rec['total_runs'] += 1

    tasks = []
    # Enforce concurrency limits
    for m in run['members']:
        # load spec and equipment in parallel
        tasks.append(_process_member(season_path, m, rec, executor, sem))

    await asyncio.gather(*tasks)

async def _process_member(season_path, member_hash, rec, executor, sem):
    async with sem:
        spec = await load_specialization_async(season_path, member_hash, executor)
        if spec:
            sid = spec['spec_id']
            rec['spec_counts'][sid] += 1
            for t in spec['talents']:
                rec['talent_counts'][sid][t['id']] += 1

        eqs = await load_equipment_async(season_path, member_hash, executor)
        for eq in eqs:
            rec['item_bonus'][eq['item_id']].update(eq['bonus_list'])

def find_seasons(branches_dir):
    """
    Yield full paths to each season folder in every branch-worktree.
    """
    for br in os.listdir(branches_dir):
        if 'origin' in br:
            continue  # Skip origin branches
        data_root = os.path.join(branches_dir, br, 'data')
        if not os.path.isdir(data_root):
            print(f"[DEBUG] No data/ in {data_root}")
            continue
        for region in os.listdir(data_root):
            region_path = os.path.join(data_root, region)
            for realm in os.listdir(region_path):
                realm_path = os.path.join(region_path, realm)
                for season in os.listdir(realm_path):
                    season_path = os.path.join(realm_path, season)
                    yield season_path

def parse_runs(season_path):
    runs_csv = os.path.join(season_path, 'runs.csv')
    if not os.path.isfile(runs_csv):
        print(f"[DEBUG] No runs.csv found in {season_path}, skipping...")    
        return
    with open(runs_csv, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['members'] = row['members'].split(';')
            yield row

def load_specialization(season_path, member_hash):
    fn = os.path.join(season_path, 'specializations', f'{member_hash}.json')
    if not os.path.isfile(fn):
        return None

    data = json.load(open(fn))
    if not data or isinstance(data, list):
        return None
    active_spec_id = data.get('active_specialization')
    specs = data.get('specializations', [])
    spec_entry = next(
        (s for s in specs
         if s.get('specialization', {}).get('id') == active_spec_id),
        None
    )
    if spec_entry is None:
        return None
    loadouts = spec_entry.get('loadouts', [])
    active_loadout = next((l for l in loadouts if l.get('is_active')), {})

    return {
        # report back the id we matched on
        'spec_id': active_spec_id,
        'talents': [
            {'id': t['id'], 'rank': t['rank']}
            for t in active_loadout.get('selected_class_talents', [])
        ]
    }


def load_equipment(season_path, member_hash):
    fn = os.path.join(season_path, 'equipment', f'{member_hash}.json')
    if not os.path.isfile(fn):
        return []
    arr = json.load(open(fn))
    return [
        {
          'item_id': e['item']['id'],
          'bonus_list': e.get('bonus_list', [])
        }
        for e in arr
    ]

async def main_async(branches_dir, output_dir, max_workers=10, max_concurrency=50):
    executor = ThreadPoolExecutor(max_workers=max_workers)
    sem = asyncio.Semaphore(max_concurrency)
    stats = {}

    loop = asyncio.get_event_loop()
    seasons = list(find_seasons(branches_dir))

    # For each season, parse runs concurrently
    for season in seasons:
        runs = await parse_runs_async(season, executor)
        run_tasks = [process_run(season, run, stats, executor, sem) for run in runs]
        await asyncio.gather(*run_tasks)

    # Write out results
    for (d, k), rec in stats.items():
        entry = {
            'dungeon_id': d,
            'keystone_level': k,
            'total_runs': rec['total_runs'],
            'specializations': [
                {
                    'spec_id': sid,
                    'picked': count,
                    'talents': [
                        {'talent_id': tid, 'count': rec['talent_counts'][sid][tid]}
                        for tid in sorted(rec['talent_counts'][sid])
                    ]
                }
                for sid, count in rec['spec_counts'].items()
            ],
            'items': [
                {'item_id': iid, 'bonus_list': sorted(bl)}
                for iid, bl in rec['item_bonus'].items()
            ]
        }

        dungeon_dir = os.path.join(output_dir, str(d))
        os.makedirs(dungeon_dir, exist_ok=True)
        out_fn = os.path.join(dungeon_dir, f'{k}.json')
        with open(out_fn, 'w') as f:
            json.dump(entry, f, indent=2)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--branches-dir', required=True,
                   help='Directory where each branch worktree lives (e.g. worktrees/)')
    p.add_argument('--output-dir', required=True,
                   help='Where to write data/aggregated/{dungeon}/{key}.json')
    args = p.parse_args()
    asyncio.run(main_async(args.branches_dir, args.output_dir))