import argparse, os, csv, json
from collections import defaultdict

def find_seasons(branches_dir):
    """
    Yield full paths to each season folder in every branch-worktree.
    """
    for br in os.listdir(branches_dir):
        if 'origin' in br:
            continue  # Skip origin branches
        data_root = os.path.join(branches_dir, br, 'data')
        print(f"[DEBUG] Checking branch worktree: {data_root}")
        if not os.path.isdir(data_root):
            print(f"[DEBUG]   ❌ No data/ in {data_root}")
            continue
        for region in os.listdir(data_root):
            region_path = os.path.join(data_root, region)
            print(f"[DEBUG]   ✅ Found region folder: {region_path}")
            for realm in os.listdir(region_path):
                realm_path = os.path.join(region_path, realm)
                for season in os.listdir(realm_path):
                    season_path = os.path.join(realm_path, season)
                    print(f"[DEBUG]     • Season dir: {season_path}")
                    yield season_path

def parse_runs(season_path):
    runs_csv = os.path.join(season_path, 'runs.csv')
    if not os.path.isfile(runs_csv):
        print(f"[DEBUG] No runs.csv found in {season_path}, skipping...")    
        return
    with open(runs_csv, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            row['members'] = row['members'].split(';')
            yield row

def load_specialization(season_path, member_hash):
    fn = os.path.join(season_path, 'specializations', f'{member_hash}.json')
    if not os.path.isfile(fn):
        return None
    data = json.load(open(fn))
    active = next((l for l in data.get('loadouts', []) if l.get('is_active')), {})
    return {
        'spec_id': data['specialization']['id'],
        'talents': [
            {'id': t['id'], 'rank': t['rank']}
            for t in active.get('selected_class_talents', [])
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

def main(branches_dir, output_dir):
    # key: (dungeon_id, keystone_level)
    stats = {}

    for season in find_seasons(branches_dir):
        print(f"[DEBUG] Processing season: {season}")
        for run in parse_runs(season):
            print(f"[DEBUG] Processing run: {run}")
            d = int(run['dungeon_id'])
            k = int(run['keystone_level'])
            key = (d, k)
            rec = stats.setdefault(key, {
                'total_runs': 0,
                'spec_counts': defaultdict(int),
                'talent_counts': defaultdict(lambda: defaultdict(int)),
                'item_bonus': defaultdict(set)
            })

            rec['total_runs'] += 1

            for m in run['members']:
                spec = load_specialization(season, m)
                if spec:
                    sid = spec['spec_id']
                    rec['spec_counts'][sid] += 1
                    for t in spec['talents']:
                        rec['talent_counts'][sid][t['id']] += 1

                for eq in load_equipment(season, m):
                    rec['item_bonus'][eq['item_id']].update(eq['bonus_list'])

    # write out per-(dungeon,key) JSON
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
    main(args.branches_dir, args.output_dir)