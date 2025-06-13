import argparse, os, csv, json
from collections import defaultdict
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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

def find_seasons(data_dir):
    """
    Yield (region, realm, season, period, full_path) for each leaf folder under data/.
    """
    data_dir = Path(data_dir)
    for region_dir in data_dir.iterdir():
        if not region_dir.is_dir(): 
            continue
        for realm_dir in region_dir.iterdir():
            if not realm_dir.is_dir(): 
                continue
            for season_dir in realm_dir.iterdir():
                if not season_dir.is_dir():
                   continue
                for period_dir in season_dir.iterdir():
                    if not period_dir.is_dir():
                        continue
                    yield region_dir.name, realm_dir.name, season_dir.name, period_dir.name, str(period_dir)

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
    loop = asyncio.get_event_loop()
    seasons = list(find_seasons(branches_dir))

    # For each <region>/<realm>/<season>/<period>
    for region_name, realm_name, season_name, period_name, season_path in seasons:
        # Derive region, season, and period
        # season_path = .../data/{region}/{realm}/{season}/{period}
        period_name = os.path.basename(season_path)
        season_name = os.path.basename(os.path.dirname(season_path))
        region_name = os.path.basename(
            os.path.dirname(os.path.dirname(os.path.dirname(season_path)))
        )

        stats = {}

        # Parse all runs in this season_path
        runs = await parse_runs_async(season_path, executor)
        run_tasks = [
            process_run(season_path, run, stats, executor, sem)
            for run in runs
        ]
        await asyncio.gather(*run_tasks)

        for (dungeon_id, key_level), rec in stats.items():
            entry = {
                'region':       region_name,
                'season':       season_name,
                'period':       period_name,
                'dungeon_id': dungeon_id,
                'keystone_level': key_level,
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

            out_dir = os.path.join(
                output_dir,
                region_name,
                season_name,
                str(dungeon_id),
                period_name
            )
            os.makedirs(out_dir, exist_ok=True)

            out_path = os.path.join(out_dir, f'{key_level}.json')
            with open(out_path, 'w') as f:
                json.dump(entry, f, indent=2)
if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--branches-dir', required=True,
                   help='Directory where each branch worktree lives (e.g. worktrees/)')
    p.add_argument('--output-dir', required=True,
                   help='Where to write data/aggregated/{dungeon}/{key}.json')
    args = p.parse_args()
    asyncio.run(main_async(args.branches_dir, args.output_dir))