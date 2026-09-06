import os
import sys
import json
import argparse
import math
from contextlib import closing
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from compArchetypes import build_dungeon_archetypes
from pageGeneration import generateSpecNav, generateDungeonNav, build_trends, trend_feeds_for_comps
from generateSpecPages import LOOKUP_DIR, load_json, load_season_info
from image_generation.comp_overview import createCompOverviewImg

def avg_top_n_keys(keylevel_timed, n=5):
    """Average key level of a comp's N highest *timed* runs.

    Used as a tie-breaker between gems that share the same highest key: it
    rewards the comp that more consistently reaches near that ceiling, rather
    than one that hit the high key a single time.
    """
    collected = []
    for lvl in sorted(keylevel_timed.keys(), reverse=True):
        for _ in range(keylevel_timed[lvl]):
            collected.append(lvl)
            if len(collected) >= n:
                break
        if len(collected) >= n:
            break
    return (sum(collected) / len(collected)) if collected else 0


# Best Spec Combinations tuning. A Bayesian prior on the high-key timed rate keeps a
# pair seen only a handful of times near a neutral ~50% instead of a noisy 0/100%, and
# washes out once a pair clears the run gate. PAIR_MIN_HK_RUNS is how many runs at the
# top key levels a pair needs before it can rank, so the list is stable rather than led
# by one lucky high key from a tiny sample.
PAIR_PRIOR_A = 5
PAIR_PRIOR_B = 5
PAIR_MIN_HK_RUNS = 20


def rank_best_spec_pairs(comps, synergy_matrix, top_level_set, context):
    """Rank 2-spec pairings for one dungeon context by a blend of high-key performance
    (measured per context) and global synergy lift (a pairing trait, so the same matrix
    feeds every context).

    context is 'all' (whole-comp keylevel stats) or an int dungeon_id (that dungeon's
    per-comp keylevel stats). Returns the top 18 row dicts, each carrying spec_a, spec_b,
    hk_success (%), total_runs and max_key measured in the context. Synergy and the raw
    blend drive the ranking only and are not emitted.
    """
    pair_agg = {}
    for data in comps:
        specs = data['specs']
        if context == 'all':
            kl_timed = data.get('keylevel_timed', {})
            kl_runs = data.get('keylevel_runs', {})
            comp_runs = data['timed'] + data['depleted']
            comp_mk = data['max_key']
        else:
            ds = data['dungeons'].get(context)
            if not ds:
                continue
            comp_runs = ds.get('t', 0) + ds.get('d', 0)
            if comp_runs <= 0:
                continue
            kl_timed = ds.get('keylevel_timed', {})
            kl_runs = ds.get('keylevel_runs', {})
            comp_mk = ds.get('mk', 0)
        hk_timed = sum(kl_timed.get(lvl, 0) for lvl in top_level_set)
        hk_runs = sum(kl_runs.get(lvl, 0) for lvl in top_level_set)
        for i in range(len(specs)):
            for j in range(i + 1, len(specs)):
                a, b = specs[i], specs[j]
                if a > b:
                    a, b = b, a
                agg = pair_agg.get((a, b))
                if agg is None:
                    agg = {'hk_timed': 0, 'hk_runs': 0, 'total_runs': 0, 'max_key': 0}
                    pair_agg[(a, b)] = agg
                agg['hk_timed'] += hk_timed
                agg['hk_runs'] += hk_runs
                agg['total_runs'] += comp_runs
                if comp_mk > agg['max_key']:
                    agg['max_key'] = comp_mk

    pair_rows = []
    for (a, b), agg in pair_agg.items():
        if agg['hk_runs'] < PAIR_MIN_HK_RUNS:
            continue
        # Bayesian-smoothed timed rate at the hottest key levels: quality of the pair.
        hk_success = (agg['hk_timed'] + PAIR_PRIOR_A) / (agg['hk_runs'] + PAIR_PRIOR_A + PAIR_PRIOR_B)
        # perf rewards both quality and high-key volume (log so a busy pair does not
        # simply dominate on raw counts).
        perf = hk_success * math.log1p(agg['hk_timed'])
        # synergy lift: how much more often the pair is played together than chance
        # predicts (centered ~1.0). Every co-occurring pair has an entry, guard anyway.
        synergy = synergy_matrix.get(a, {}).get(b, 0)
        blend = perf * synergy
        pair_rows.append({
            'spec_a': a,
            'spec_b': b,
            'blend': blend,
            'hk_success': round(hk_success * 100),
            'total_runs': agg['total_runs'],
            'max_key': agg['max_key'],
        })

    pair_rows.sort(key=lambda r: r['blend'], reverse=True)
    top = pair_rows[:18]
    for r in top:
        r.pop('blend', None)
    return top


def calculate_comp_stats(connection, cursor, season, spec_lookup):
    # Fetch all comp aggregation data
    print("Fetching comps from database...")
    raw_comps = databaseConnector.fetch_all_comps(connection, cursor, season)
    print(f"Fetched {len(raw_comps)} comp rows")
    
    # comp_hash string -> { 'specs': [], 'weight': 0, 'timed': 0, 'depleted': 0, 'keys': [] }
    compiled_comps = {}
    spec_weights = {}
    total_runs = 0
    # keystone level -> runs (used to determine top keylevels dynamically)
    keylevel_counts = {}
    # per-dungeon keystone counts: dungeon_id -> { keylevel: runs }
    keylevel_counts_by_dungeon = {}
    # Exponent used to emphasize higher keys when ranking high-key comps
    HIGHKEY_EXP = 3

    for row in raw_comps:
        # row: dungeon_id, keystone_level, comp (csv string), timed_runs, depleted_runs
        dungeon_id = int(row[0])
        keystone_level = int(row[1])
        comp_str = row[2]
        timed = int(row[3])
        depleted = int(row[4])
        
        runs = timed + depleted
        total_runs += runs
        
        # Exponential curve for weights: e.g. a level 10 is weight 1, level 20 is weight 121
        # Timed runs give full weight, depleted gives 10%
        key_factor = max(1, keystone_level - 9)
        weight_per_timed = math.pow(key_factor, 2)
        weight_per_depleted = weight_per_timed * 0.1
        
        row_weight = (timed * weight_per_timed) + (depleted * weight_per_depleted)
        
        specs = [int(s) for s in comp_str.split(',') if s.strip()]
        if len(specs) != 5:
            continue
            
        specs.sort(key=lambda s: (int(spec_lookup.get(str(s), {}).get('role', 2)), s))
        
        comp_hash = ",".join(str(s) for s in specs)
        
        if comp_hash not in compiled_comps:
            compiled_comps[comp_hash] = {
                'specs': specs,
                'weight': 0,
                'timed': 0,
                'depleted': 0,
                'max_key': 0,
                'avg_key_acc': 0,
                # per-keylevel accumulators for dynamic top-keylevel metrics
                'keylevel_runs': {},
                'keylevel_timed': {},
                'keylevel_weight': {},
                'dungeons': {}
            }
            
        c = compiled_comps[comp_hash]
        c['weight'] += row_weight
        c['timed'] += timed
        c['depleted'] += depleted

        if dungeon_id not in c['dungeons']:
            c['dungeons'][dungeon_id] = {
                'w': 0,
                't': 0,
                'd': 0,
                'mk': 0,
                'avg_key_acc': 0,
                'keylevel_runs': {},
                'keylevel_timed': {},
                'keylevel_weight': {}
            }
        
        c['dungeons'][dungeon_id]['w'] += row_weight
        c['dungeons'][dungeon_id]['t'] += timed
        c['dungeons'][dungeon_id]['d'] += depleted
        if keystone_level > c['dungeons'][dungeon_id]['mk']:
            c['dungeons'][dungeon_id]['mk'] = keystone_level

        if keystone_level > c['max_key']:
            c['max_key'] = keystone_level
        c['avg_key_acc'] += (keystone_level * runs)
        # accumulate per-keylevel stats for comp and per-dungeon
        c['keylevel_runs'][keystone_level] = c['keylevel_runs'].get(keystone_level, 0) + runs
        c['keylevel_timed'][keystone_level] = c['keylevel_timed'].get(keystone_level, 0) + timed
        c['keylevel_weight'][keystone_level] = c['keylevel_weight'].get(keystone_level, 0) + row_weight
        c['dungeons'][dungeon_id]['avg_key_acc'] += (keystone_level * runs)
        c['dungeons'][dungeon_id]['keylevel_runs'][keystone_level] = c['dungeons'][dungeon_id]['keylevel_runs'].get(keystone_level, 0) + runs
        c['dungeons'][dungeon_id]['keylevel_timed'][keystone_level] = c['dungeons'][dungeon_id]['keylevel_timed'].get(keystone_level, 0) + timed
        c['dungeons'][dungeon_id]['keylevel_weight'][keystone_level] = c['dungeons'][dungeon_id]['keylevel_weight'].get(keystone_level, 0) + row_weight

        # global keystone counts (for top N keylevels selection)
        keylevel_counts[keystone_level] = keylevel_counts.get(keystone_level, 0) + runs
        # per-dungeon keystone counts
        if dungeon_id not in keylevel_counts_by_dungeon:
            keylevel_counts_by_dungeon[dungeon_id] = {}
        keylevel_counts_by_dungeon[dungeon_id][keystone_level] = keylevel_counts_by_dungeon[dungeon_id].get(keystone_level, 0) + runs
        
        for s in specs:
            spec_weights[s] = spec_weights.get(s, 0) + row_weight

    # Finalize compilations
    unique_comps_list = []
    total_weight = sum(spec_weights.values()) / 5.0 # Since each run has 5 specs

    for comp_hash, data in compiled_comps.items():
        runs = data['timed'] + data['depleted']
        if runs > 0:
            data['avg_key'] = data['avg_key_acc'] / runs
        else:
            data['avg_key'] = 0
            
        unique_comps_list.append(data)

    # Determine global top-2 keylevels by raw runs (fallback)
    top_key_levels = [k for k, _ in sorted(keylevel_counts.items(), key=lambda x: x[1], reverse=True)][:2]

    # Determine per-dungeon top-2 keylevels
    top_key_levels_by_dungeon = {}
    for did, counts in keylevel_counts_by_dungeon.items():
        top_levels = [k for k, _ in sorted(counts.items(), key=lambda x: x[1], reverse=True)][:2]
        top_key_levels_by_dungeon[did] = top_levels

    # Calculate Synergy
    print("Calculating synergy heatmap...")
    synergy_matrix = {} # [specA][specB] = lift
    # To compute lift: P(A inter B) / (P(A) * P(B)) based on weights
    pair_weights = {}
    for data in unique_comps_list:
        w = data['weight']
        specs = data['specs']
        for i in range(len(specs)):
            for j in range(i+1, len(specs)):
                sA, sB = specs[i], specs[j]
                if sA > sB: sA, sB = sB, sA
                pair_key = f"{sA}-{sB}"
                pair_weights[pair_key] = pair_weights.get(pair_key, 0) + w

    for pair_key, wAB in pair_weights.items():
        sA, sB = map(int, pair_key.split('-'))
        if sA not in synergy_matrix: synergy_matrix[sA] = {}
        if sB not in synergy_matrix: synergy_matrix[sB] = {}
        
        wA = spec_weights.get(sA, 1)
        wB = spec_weights.get(sB, 1)
        
        # Lift = (wAB / total_weight) / ((wA / total_weight) * (wB / total_weight))
        # Wait, there are 5 specs per comp, meaning each spec pairs with 4 others. 
        # So sum(wAB for B) = 4 * wA. We should adjust expectations accordingly.
        # Expected pair weight = (wA * wB) / total_weight * scale_factor
        # An easy approximation of synergy is just standardizing the lift to be centered around 1.0
        expected = (wA * wB) / (total_weight)
        if expected > 0:
            lift = wAB / expected
        else:
            lift = 0
            
        synergy_matrix[sA][sB] = lift
        synergy_matrix[sB][sA] = lift

    # Hidden Gems
    # A hidden gem is a comp that is played far less than the established meta
    # comps but still performs well at high keys. Popularity has to be measured
    # relative to the most-played comps -- NOT the raw total key count. Keys are
    # split across thousands of distinct 5-spec comps, so even the #1/#2 comp is
    # a tiny fraction of all runs and would wrongly slip under a "% of total
    # keys" gate (which is why the 2nd most-played comp used to show up here).
    print("Finding hidden gems...")
    POPULARITY_FRACTION = 0.02  # a gem is played at most 2% as often as the #1 comp
    max_comp_runs = max(
        (d['timed'] + d['depleted'] for d in unique_comps_list),
        default=0,
    )
    popularity_cutoff = max_comp_runs * POPULARITY_FRACTION
    hidden_gems = []
    for data in unique_comps_list:
        runs = data['timed'] + data['depleted']
        if 20 < runs < popularity_cutoff:  # niche, but with enough of a sample
            success_rate = data['timed'] / runs
            if success_rate >= 0.75 and data['avg_key'] >= 10:
                # Rank by the comp's actual highest key (the displayed column),
                # tie-broken by the avg of its top-5 timed keys, then success and
                # runs.
                data['success_pct'] = round(success_rate * 100)
                top5_avg = avg_top_n_keys(data.get('keylevel_timed', {}), 5)
                hidden_gems.append((data['max_key'], top5_avg, success_rate, runs, data))

    hidden_gems.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=True)
    hidden_gems_out = [x[-1] for x in hidden_gems[:10]]

    # Best Spec Combinations: rank every 2-spec pairing by a blend of high-key
    # performance and global synergy lift, so we surface the strongest pairs any two
    # players could bring together. Emitted per dungeon context so the card reacts to the
    # dungeon dropdown: 'all' uses whole-comp keylevel stats and the global top key
    # levels (the original behaviour), each dungeon uses that dungeon's per-comp keylevel
    # stats and its own top key levels. Reuses unique_comps_list and synergy_matrix, so
    # no extra DB work.
    print("Ranking best spec combinations...")
    best_spec_pairs_by_dungeon = {
        'all': rank_best_spec_pairs(unique_comps_list, synergy_matrix, set(top_key_levels), 'all')
    }
    for did in keylevel_counts_by_dungeon.keys():
        ctx_levels = set(top_key_levels_by_dungeon.get(did, top_key_levels))
        best_spec_pairs_by_dungeon[str(did)] = rank_best_spec_pairs(
            unique_comps_list, synergy_matrix, ctx_levels, did)
    # Keep the 'all' list for server-side first paint.
    best_spec_pairs = best_spec_pairs_by_dungeon['all']

    # Pre-calculate simple UI "Perfect Fit" data payload
    # We only need to send the top 2000 comps by weight to the frontend to keep json tiny
    unique_comps_list.sort(key=lambda x: x['weight'], reverse=True)
    top_comps = unique_comps_list[:2000]

    # Rich per-comp input for team-comp clustering: retains the per-key-level breakdown
    # (stripped from frontend_json below) so the high-key / hidden-gem cards can restrict
    # to each context's highest key levels. Dungeon keys stay int here, matching frontend.
    def _kl(runs_map, timed_map):
        return {int(lvl): {'r': int(runs_map.get(lvl, 0)), 't': int(timed_map.get(lvl, 0))}
                for lvl in runs_map}

    archetype_input = []
    for tc in top_comps:
        dj = {}
        for did, ds in tc['dungeons'].items():
            d_runs = ds.get('t', 0) + ds.get('d', 0)
            dj[did] = {
                't': ds.get('t', 0), 'd': ds.get('d', 0), 'runs': d_runs,
                'mk': ds.get('mk', 0), 'w': ds.get('w', 0),
                'avg_key': (ds.get('avg_key_acc', 0) / d_runs) if d_runs else 0,
                'kl': _kl(ds.get('keylevel_runs', {}), ds.get('keylevel_timed', {})),
            }
        archetype_input.append({
            'c': tc['specs'], 'w': tc['weight'], 't': tc['timed'], 'd': tc['depleted'],
            'runs': tc['timed'] + tc['depleted'], 'mk': tc['max_key'],
            'avg_key': tc.get('avg_key', 0),
            'kl': _kl(tc.get('keylevel_runs', {}), tc.get('keylevel_timed', {})),
            'dungeons': dj,
        })

    frontend_json = []
    for tc in top_comps:
        # compute runs and best dungeon
        best_dungeon_id = max(tc['dungeons'].items(), key=lambda x: x[1]['t'] + x[1]['d'])[0] if tc['dungeons'] else 0
        best_dungeon_runs = sum(x for x in [tc['dungeons'].get(best_dungeon_id, {}).get('t', 0), tc['dungeons'].get(best_dungeon_id, {}).get('d', 0)])
        tc_runs = tc.get('timed', 0) + tc.get('depleted', 0)
        
        # Round the weights inside the dungeons dictionary
        for did, d_stats in tc['dungeons'].items():
            d_stats['w'] = round(d_stats['w'], 2)
            # compute per-dungeon runs and avg_key
            d_runs = d_stats.get('t', 0) + d_stats.get('d', 0)
            d_stats['runs'] = d_runs
            if d_runs > 0:
                d_stats['avg_key'] = round(d_stats.get('avg_key_acc', 0) / d_runs, 2)
            else:
                d_stats['avg_key'] = 0
            # compute top-keylevel metrics for this dungeon using dungeon-specific top-2 levels
            d_stats['top_key_runs'] = 0
            d_stats['top_key_weight'] = 0
            dungeon_top_levels = top_key_levels_by_dungeon.get(did, top_key_levels)
            # compute per-dungeon high-key aggregates and identify highest top-key level this comp hit
            d_stats['highkey_score'] = 0
            d_stats['top_key_max'] = 0
            for lvl in dungeon_top_levels:
                lvl_runs = d_stats.get('keylevel_runs', {}).get(lvl, 0)
                lvl_timed = d_stats.get('keylevel_timed', {}).get(lvl, 0)
                lvl_weight = d_stats.get('keylevel_weight', {}).get(lvl, 0)
                d_stats['top_key_runs'] += lvl_runs
                d_stats['top_key_weight'] += round(lvl_weight, 2)
                d_stats['top_key_timed'] = d_stats.get('top_key_timed', 0) + lvl_timed
                # exponential score (timed weighted more, depleted as 10%)
                key_factor = max(1, lvl - 9)
                d_stats['highkey_score'] += lvl_timed * (key_factor ** HIGHKEY_EXP)
                depleted = max(0, lvl_runs - lvl_timed)
                d_stats['highkey_score'] += depleted * 0.1 * (key_factor ** HIGHKEY_EXP)
                if lvl_runs > 0 and lvl > d_stats['top_key_max']:
                    d_stats['top_key_max'] = lvl
            # clean up heavy internals
            d_stats.pop('avg_key_acc', None)
            d_stats.pop('keylevel_runs', None)
            d_stats.pop('keylevel_timed', None)
            d_stats.pop('keylevel_weight', None)
            
        # compute comp-level aggregated fields for frontend
        # compute comp-level aggregated fields for frontend using global top-2 keylevels
        top_key_runs = 0
        top_key_weight = 0
        top_key_timed = 0
        highkey_score = 0
        top_key_max = 0
        for lvl in top_key_levels:
            lvl_runs = tc.get('keylevel_runs', {}).get(lvl, 0)
            lvl_timed = tc.get('keylevel_timed', {}).get(lvl, 0)
            lvl_weight = tc.get('keylevel_weight', {}).get(lvl, 0)
            top_key_runs += lvl_runs
            top_key_weight += lvl_weight
            top_key_timed += lvl_timed
            # exponential high-key score (timed weighted more, depleted as 10%)
            key_factor = max(1, lvl - 9)
            highkey_score += lvl_timed * (key_factor ** HIGHKEY_EXP)
            depleted_lvl = max(0, lvl_runs - lvl_timed)
            highkey_score += depleted_lvl * 0.1 * (key_factor ** HIGHKEY_EXP)
            if lvl_runs > 0 and lvl > top_key_max:
                top_key_max = lvl

        frontend_json.append({
            'c': tc['specs'],
            'w': round(tc['weight'], 2),
            't': tc['timed'],
            'd': tc['depleted'],
            'runs': tc_runs,
            'avg_key': round(tc.get('avg_key', 0), 2),
            'mk': tc['max_key'],
            'bd': best_dungeon_id,
            'bdr': best_dungeon_runs,
            'top_key_levels': top_key_levels,
            'top_key_runs': top_key_runs,
            'top_key_timed': top_key_timed,
            'top_key_weight': round(top_key_weight, 2),
            'highkey_score': round(highkey_score, 2),
            'top_key_max': top_key_max,
            'dungeons': tc['dungeons']
        })

    # also keep per-dungeon top keylevels for debugging or advanced UIs (not required client-side)
    return frontend_json, synergy_matrix, hidden_gems_out, best_spec_pairs, top_key_levels, archetype_input, best_spec_pairs_by_dungeon


def compute_top_comps(frontend_json, n=5):
    """Top comps by raw runs, reduced to the row dicts createCompOverviewImg
    renders (specs, runs, timed, max_key) — same ordering as the page's
    "Most Popular" list."""
    top = sorted(frontend_json, key=lambda x: x.get('runs', 0), reverse=True)[:n]
    return [
        {
            "specs": c["c"],
            "runs": c.get("runs", 0),
            "timed": c.get("t", 0),
            "max_key": c.get("mk", 0),
        }
        for c in top
    ]


def compute_meta_comp(frontend_json, min_runs=20):
    """The page's "meta" comp — rank 1 of the Best-for-High-Keys ordering
    (runs >= min_runs, sorted by max key, high-key score, runs) — reduced to the
    dict createCompOverviewImg expects. popularity_rank is the comp's 1-based
    position in the by-runs ordering compute_top_comps uses. Returns None when
    no comp qualifies."""
    best = sorted(
        (c for c in frontend_json if c.get('runs', 0) >= min_runs),
        key=lambda x: (x.get('mk', 0), x.get('highkey_score', 0), x.get('runs', 0)),
        reverse=True,
    )
    if not best:
        return None
    c = best[0]
    runs_order = sorted(frontend_json, key=lambda x: x.get('runs', 0), reverse=True)
    return {
        "specs": c["c"],
        "runs": c.get("runs", 0),
        "timed": c.get("t", 0),
        "max_key": c.get("mk", 0),
        "popularity_rank": runs_order.index(c) + 1,
    }


def main(template_path, output_dir):
    season_info = load_season_info(LOOKUP_DIR)
    season = season_info.get('blizzard_season_id')
    if not season:
        print("ERROR: Current season ID not found in seasonInfo.json", file=sys.stderr)
        sys.exit(2)
    
    conn = None
    try:
        conn = databaseConnector.get_connection()
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
    except Exception as e:
        print(f"ERROR: Failed to obtain DB connection: {e}", file=sys.stderr)
        sys.exit(2)
        
    try:
        # Fetch lookup info
        dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
        spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
        class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
        notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
        # Raid-buff / utility coverage map (buff -> providing specIDs) for the
        # Perfect Fit widget. Small static file (~a dozen entries); passed to the
        # template as-is so the client can compute covered/missing buffs.
        group_buffs = load_json(os.path.join(LOOKUP_DIR, "groupbuffs.json")) or []
        
        # Map specs easily for UI
        specs_ui = []
        if spec_lookup is not None and isinstance(spec_lookup, dict):
            for sid, sdata in spec_lookup.items():
                c_id = str(sdata.get('classID'))
                c_data = class_lookup.get(c_id, {})
                role_int = int(sdata.get('role', 2))
                role_name = 'Tank' if role_int == 0 else ('Healer' if role_int == 1 else 'Dps')
                class_name = c_data.get('name', 'Unknown')
                specs_ui.append({
                    'id': int(sid),
                    'name': sdata.get('name', 'Unknown'),
                    'specName': sdata.get('name', 'Unknown'),
                    'role': role_int,
                    'roleName': role_name,
                    'className': class_name,
                    'cleanClass': class_name.replace(' ', ''),
                    'classId': int(c_id) if c_id else 0,
                    'icon': sdata.get('SpellIconFileId')
                })
        
        frontend_json, synergy_matrix, hidden_gems, best_spec_pairs, top_key_levels, archetype_input, best_spec_pairs_by_dungeon = calculate_comp_stats(conn, cursor, season, spec_lookup)

        # Save Perfect Fit JSON
        json_out_dir = os.path.join("assets", "json")
        os.makedirs(json_out_dir, exist_ok=True)
        with open(os.path.join(json_out_dir, "comps_index.json"), "w", encoding="utf-8") as f:
            json.dump(frontend_json, f, separators=(',', ':'))

        # Archetypes drive the Most Popular / Best High Keys / Hidden Gems cards:
        # popular comps grouped with their per-slot flexible alternates (leader
        # radius-1 clustering). Precomputed per dungeon so the dungeon dropdown can
        # re-rank client-side, and in three flavours per context. The 'all' context is
        # server-rendered for first paint; the full map is shipped as JSON for the
        # client to swap on dungeon change.
        dungeon_ids = [str(k) for k in dungeon_lookup.keys()]
        archetypes_by_dungeon = build_dungeon_archetypes(
            archetype_input, spec_lookup, class_lookup, dungeon_ids, top_n=6)
        with open(os.path.join(json_out_dir, "comp_archetypes.json"), "w", encoding="utf-8") as f:
            json.dump(archetypes_by_dungeon, f, separators=(',', ':'))
        archetypes_all = archetypes_by_dungeon.get(
            "all", {"popular": [], "highkey": [], "gems": []})

        # The "meta" comp is the rank-1 family of Best High Keys. It is highlighted
        # everywhere it appears; key is the canonical comma-joined spec list.
        meta_comp_key = (",".join(str(s) for s in archetypes_all["highkey"][0]["c"])
                         if archetypes_all["highkey"] else "")

        # Prepare JS-friendly lookups to safely serialize into template
        dungeon_lookup_js = {}
        for k, v in dungeon_lookup.items():
            try:
                dk = int(k)
            except Exception:
                dk = k
            name = v.get('name') if isinstance(v, dict) else v
            if isinstance(name, dict):
                name = name.get('en_US') or next(iter(name.values()), '')
            dungeon_lookup_js[dk] = name

        specs_ui_map = {s['id']: s for s in specs_ui}
            
        print("Rendering template...")
        env = Environment(
            loader=FileSystemLoader(os.path.dirname(template_path)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        
        template = env.get_template(os.path.basename(template_path))
        output_html = template.render(
            # Contextual archetype trends (the comps page's own bar), diffed off the
            # already-open build connection; build_trends opens its own tuple cursor.
            trends=build_trends(conn, cursor, trend_feeds_for_comps(),
                                {"specs": spec_lookup, "classes": class_lookup}),
            specs_ui=specs_ui,
            synergy_matrix=json.dumps(synergy_matrix),
            best_spec_pairs=best_spec_pairs,
            best_spec_pairs_by_dungeon=best_spec_pairs_by_dungeon,
            spec_lookup=spec_lookup,
            class_lookup=class_lookup,
            dungeon_lookup=dungeon_lookup,
            dungeon_nav=generateDungeonNav(dungeon_lookup),
            spec_nav=generateSpecNav(spec_lookup, class_lookup),
            season_info=season_info,
            active_page="comps",
            breadcrumbs=[
                {"title": "Pages", "href": "/pages"},
                {"title": "Comp Analysis", "href": "/pages/comps"},
            ],
            notifications=notifications,
            cur_page="comps",
            top_key_levels=top_key_levels,
            # server-side precomputed archetypes for the 'all' context (first paint);
            # the client swaps per-dungeon lists from comp_archetypes.json
            archetypes_all=archetypes_all,
            meta_comp_key=meta_comp_key,
            dungeon_lookup_js=dungeon_lookup_js,
            specs_ui_map=specs_ui_map,
            group_buffs=group_buffs,
        )
        
        out_path = os.path.join(output_dir, "comps.html")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(output_html)
        print(f"Generated {out_path}")
        
        # Create Preview Image
        preview_dir = os.path.join("assets", "img", "previews")
        os.makedirs(preview_dir, exist_ok=True)
        preview_path = os.path.join(preview_dir, "comps.png")
        try:
            createCompOverviewImg(
                tmpdir=os.path.join("tmp", "img"),
                out_path=preview_path,
                season=season,
                conn=conn,
                cursor=cursor,
                meta_comp=compute_meta_comp(frontend_json),
                top_comps=compute_top_comps(frontend_json),
            )
            print(f"Generated preview image at {preview_path}")
        except Exception as e:
            print(f"Failed to generate preview for comps: {e}", file=sys.stderr)
            
    finally:
        conn.close()
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate comp analysis page")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument("--output_dir", required=True, help="Directory to write generated HTML pages")
    args = parser.parse_args()

    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME", "Mythistone"),
        os.environ.get("DATABASE_PORT", "3306"),
        # main() holds one pooled connection for the whole build; the trends bar now
        # reuses it (build_trends opens its own cursor), so a small pool is plenty.
        4,
    )

    main(args.template, args.output_dir)
