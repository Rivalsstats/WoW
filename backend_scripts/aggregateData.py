import argparse, os, csv, json
from collections import defaultdict
from pathlib import Path

def find_periods(data_dir):
    """
    Yield (region, season, period, period_path) under data/{region}/{realm}/{season}/{period}.
    """
    for region_dir in Path(data_dir).iterdir():
        if not region_dir.is_dir(): continue
        region = region_dir.name
        for realm_dir in region_dir.iterdir():
            if not realm_dir.is_dir(): continue
            for season_dir in realm_dir.iterdir():
                if not season_dir.is_dir(): continue
                season = season_dir.name
                for period_dir in season_dir.iterdir():
                    if not period_dir.is_dir(): continue
                    period = period_dir.name
                    yield region, season, period, period_dir

def parse_runs(csv_path):
    if not csv_path.is_file(): return
    with csv_path.open(newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['members'] = row['members'].split(';')
            yield row

def load_specialization(spec_dir, member_hash):
    fn = spec_dir / f"{member_hash}.json"
    if not fn.exists(): return None
    data = json.loads(fn.read_text())
    active = data.get('active_specialization')
    specs  = data.get('specializations', [])
    spec_entry = next((s for s in specs
                       if s.get('specialization',{}).get('id') == active), None)
    if not spec_entry: return None
    loadouts = spec_entry.get('loadouts', [])
    active_lo = next((l for l in loadouts if l.get('is_active')), {})
    return {
      'spec_id': active,
      'talents': [
        {'id': t['id'], 'rank': t['rank']}
        for t in active_lo.get('selected_class_talents', [])
      ]
    }

def load_equipment(equip_dir, member_hash):
    fn = equip_dir / f"{member_hash}.json"
    if not fn.exists(): return []
    arr = json.loads(fn.read_text())
    return [
      {'item_id': e['item']['id'],
       'bonus_list': e.get('bonus_list', [])}
      for e in arr
    ]

def load_existing(output_dir):
    """
    Read every JSON under output_dir into the same stats dict structure.
    """
    stats = {}
    for path in Path(output_dir).rglob("*.json"):
        entry = json.loads(path.read_text())
        key = (entry['region'], entry['season'], entry['period'],
               entry['dungeon_id'], entry['keystone_level'])
        # rebuild the accumulators
        rec = {
          'total_runs': entry['total_runs'],
          'spec_counts': defaultdict(int, {
              s['spec_id']: s['picked']
              for s in entry['specializations']
          }),
          'talent_counts': defaultdict(lambda: defaultdict(int), {
              s['spec_id']: {t['talent_id']: t['count']
                             for t in s['talents']}
              for s in entry['specializations']
          }),
          'item_bonus': defaultdict(set, {
              i['item_id']: set(i['bonus_list'])
              for i in entry['items']
          })
        }
        stats[key] = rec
    return stats

def main(data_dir, output_dir, incremental=False):
    # 1) if incremental, pre-load any existing aggregates
    stats = load_existing(output_dir) if incremental else {}

    # 2) walk this *branch*’s data tree, adding into stats
    for region, season, period, period_path in find_periods(data_dir):
        spec_dir  = period_path / 'specializations'
        equip_dir = period_path / 'equipment'
        for run in parse_runs(period_path / 'runs.csv'):
            d = int(run['dungeon_id'])
            k = int(run['keystone_level'])
            key = (region, season, period, d, k)

            rec = stats.setdefault(key, {
                'total_runs': 0,
                'spec_counts': defaultdict(int),
                'talent_counts': defaultdict(lambda: defaultdict(int)),
                'item_bonus': defaultdict(set)
            })
            rec['total_runs'] += 1

            for m in run['members']:
                spec = load_specialization(spec_dir, m)
                if spec:
                    sid = spec['spec_id']
                    rec['spec_counts'][sid] += 1
                    for t in spec['talents']:
                        rec['talent_counts'][sid][t['id']] += 1

                for eq in load_equipment(equip_dir, m):
                    rec['item_bonus'][eq['item_id']].update(eq['bonus_list'])

    # 3) write out *all* stats
    for (region, season, period, dungeon, k), rec in stats.items():
        out = {
            'region': region,
            'season': season,
            'period': period,
            'dungeon_id': dungeon,
            'keystone_level': k,
            'total_runs': rec['total_runs'],
            'specializations': [
              {
                'spec_id': sid,
                'picked': rec['spec_counts'][sid],
                'talents': [
                  {'talent_id': tid, 'count': rec['talent_counts'][sid][tid]}
                  for tid in sorted(rec['talent_counts'][sid])
                ]
              }
              for sid in sorted(rec['spec_counts'])
            ],
            'items': [
              {'item_id': iid, 'bonus_list': sorted(bl)}
              for iid, bl in rec['item_bonus'].items()
            ]
        }
        dst = Path(output_dir) / region / season / str(dungeon) / period
        dst.mkdir(parents=True, exist_ok=True)
        (dst / f"{k}.json").write_text(json.dumps(out, indent=2))

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir',     required=True,
                   help='root data/ folder')
    p.add_argument('--output-dir',   required=True,
                   help='where to write aggregated JSONs')
    p.add_argument('--incremental',  action='store_true',
                   help='load & merge existing aggregates before processing')
    args = p.parse_args()
    main(args.data_dir, args.output_dir, args.incremental)
