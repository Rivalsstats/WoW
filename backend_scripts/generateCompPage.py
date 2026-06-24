import os
import sys
import json
import argparse
import math
from contextlib import closing
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from pageGeneration import generateSpecNav, generateDungeonNav
from generateSpecPages import LOOKUP_DIR, load_json
from generateSocialsPost import createCompOverviewImg

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

    # Glue Specs (Flexibility Index): in how many distinct viable high-key comps
    # (timed > 5, avg key > 12) a spec appears. The raw count is biased by play
    # rate -- a more-played spec shows up in more distinct comps simply because
    # there is more data -- so we debias it (see below) before ranking. Grouped
    # by role and shown as a percentage relative to the most flexible spec *in
    # that role* (a tank and a dps can't be compared directly since dps fill 3 of
    # the 5 slots). Raw counts are kept for the tooltip.
    print("Calculating glue specs...")
    flex_stats = {}
    for data in unique_comps_list:
        if data['timed'] > 5 and data['avg_key'] > 12:
            for s in data['specs']:
                fs = flex_stats.setdefault(s, {'comps': 0, 'runs': 0, 'max_key': 0})
                fs['comps'] += 1
                fs['runs'] += data['timed'] + data['depleted']
                fs['max_key'] = max(fs['max_key'], data['max_key'])

    # Debias flexibility for play volume, fitted separately within each role.
    # Distinct-comp count grows with runs (more data -> more distinct comps), so
    # within a role we fit that role's runs -> comps trend (log-log least
    # squares) and score each spec by how far it sits ABOVE its role's trend:
    # genuine versatility, not popularity. Low-sample specs are shrunk toward the
    # trend (score ~1) so an underplayed spec is neither punished nor randomly
    # crowned -- it just sits near the middle until there's evidence.
    def score_flexibility(specs):
        points = [(math.log(g['runs']), math.log(g['comps']))
                  for g in specs if g['runs'] > 0 and g['comps'] > 0]
        n = len(points)
        if n >= 2:
            mx = sum(p[0] for p in points) / n
            my = sum(p[1] for p in points) / n
            var = sum((p[0] - mx) ** 2 for p in points)
            cov = sum((p[0] - mx) * (p[1] - my) for p in points)
            slope = cov / var if var else 0.0
            intercept = my - slope * mx
        else:
            slope, intercept = 0.0, 0.0
        runs_sorted = sorted(g['runs'] for g in specs)
        shrink_k = runs_sorted[len(runs_sorted) // 2] if runs_sorted else 1  # role median runs
        for g in specs:
            if g['runs'] > 0 and g['comps'] > 0:
                residual = math.log(g['comps']) - (intercept + slope * math.log(g['runs']))
                reliability = g['runs'] / (g['runs'] + shrink_k)  # 0..1, shrink low data
                g['flex_score'] = math.exp(residual * reliability)
            else:
                g['flex_score'] = 0.0

    # bucket by role (0=tank, 1=healer, 2=dps)
    glue_specs_by_role = {'0': [], '1': [], '2': []}
    for spec_id, fs in flex_stats.items():
        role = str(spec_lookup.get(str(spec_id), {}).get('role', 2))
        glue_specs_by_role.setdefault(role, [])
        glue_specs_by_role[role].append({'spec_id': spec_id, **fs})

    # fit + score within each role, then percentage relative to the role's best
    for role, specs in glue_specs_by_role.items():
        score_flexibility(specs)
        max_score = max((g['flex_score'] for g in specs), default=0) or 1
        for g in specs:
            g['flex_pct'] = round(g['flex_score'] / max_score * 100)
        specs.sort(key=lambda g: g['flex_score'], reverse=True)

    # flat top-10 list kept for the social preview image (createCompOverviewImg)
    glue_specs_list = sorted(
        ({'spec_id': k, **v} for k, v in flex_stats.items()),
        key=lambda x: x['comps'], reverse=True,
    )[:10]

    # Pre-calculate simple UI "Perfect Fit" data payload
    # We only need to send the top 2000 comps by weight to the frontend to keep json tiny
    unique_comps_list.sort(key=lambda x: x['weight'], reverse=True)
    top_comps = unique_comps_list[:2000]
    
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
    return frontend_json, synergy_matrix, hidden_gems_out, glue_specs_list, glue_specs_by_role, top_key_levels


def main(template_path, output_dir):
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    season = season_info.get('blizzard_season_id')
    if not season:
        print("ERROR: Current season ID not found in seasonInfo.json", file=sys.stderr)
        sys.exit(2)
    
    conn = None
    try:
        conn = databaseConnector.get_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR: Failed to obtain DB connection: {e}", file=sys.stderr)
        sys.exit(2)
        
    try:
        # Fetch lookup info
        dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
        spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
        class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
        notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
        
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
        
        frontend_json, synergy_matrix, hidden_gems, glue_specs, glue_specs_by_role, top_key_levels = calculate_comp_stats(conn, cursor, season, spec_lookup)

        # Save Perfect Fit JSON
        json_out_dir = os.path.join("assets", "json")
        os.makedirs(json_out_dir, exist_ok=True)
        with open(os.path.join(json_out_dir, "comps_index.json"), "w", encoding="utf-8") as f:
            json.dump(frontend_json, f, separators=(',', ':'))

        # Compute server-side top lists for initial page render
        # Most popular by raw runs
        most_popular = sorted(frontend_json, key=lambda x: x.get('runs', 0), reverse=True)[:6]

        # Best for high keys. This mirrors the client-side renderTopLists ranking
        # exactly (global view): comps with at least MIN_RUNS runs, sorted by
        # highest key reached, then high-key score, then runs. Keeping it in sync
        # means the server-rendered list matches the client re-render, and the
        # meta comp is precisely this list's rank 1.
        MIN_COMP_RUNS = 20
        best_highkey = sorted(
            (c for c in frontend_json if c.get('runs', 0) >= MIN_COMP_RUNS),
            key=lambda x: (x.get('mk', 0), x.get('highkey_score', 0), x.get('runs', 0)),
            reverse=True,
        )[:6]

        # The "meta" comp is the rank 1 comp in "Best for High Keys". It is
        # highlighted everywhere it appears across the page. Key is the canonical
        # comma-joined spec list, matching the 'c' arrays used client-side.
        meta_comp_key = ",".join(str(s) for s in best_highkey[0]['c']) if best_highkey else ""

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
            specs_ui=specs_ui,
            synergy_matrix=json.dumps(synergy_matrix),
            hidden_gems=hidden_gems,
            glue_specs_by_role=glue_specs_by_role,
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
            # server-side precomputed lists for initial render
            most_popular=most_popular,
            best_highkey=best_highkey,
            meta_comp_key=meta_comp_key,
            dungeon_lookup_js=dungeon_lookup_js,
            specs_ui_map=specs_ui_map,
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
                glue_specs=glue_specs
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
        1,
    )

    main(args.template, args.output_dir)
