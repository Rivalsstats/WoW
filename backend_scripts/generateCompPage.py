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

def calculate_comp_stats(connection, cursor, season, spec_lookup):
    # Fetch all comp aggregation data
    print("Fetching comps from database...")
    raw_comps = databaseConnector.fetch_all_comps(connection, cursor, season)
    print(f"Fetched {len(raw_comps)} comp rows")
    
    # comp_hash string -> { 'specs': [], 'weight': 0, 'timed': 0, 'depleted': 0, 'keys': [] }
    compiled_comps = {}
    spec_weights = {}
    total_runs = 0

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
                'dungeons': {}
            }
            
        c = compiled_comps[comp_hash]
        c['weight'] += row_weight
        c['timed'] += timed
        c['depleted'] += depleted

        if dungeon_id not in c['dungeons']:
            c['dungeons'][dungeon_id] = {'w': 0, 't': 0, 'd': 0, 'mk': 0}
        
        c['dungeons'][dungeon_id]['w'] += row_weight
        c['dungeons'][dungeon_id]['t'] += timed
        c['dungeons'][dungeon_id]['d'] += depleted
        if keystone_level > c['dungeons'][dungeon_id]['mk']:
            c['dungeons'][dungeon_id]['mk'] = keystone_level

        if keystone_level > c['max_key']:
            c['max_key'] = keystone_level
        c['avg_key_acc'] += (keystone_level * runs)
        
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
    # Comp < 1% popularity but high avg key and > 90% timed in high keys or just very strong weight relative to raw runs
    print("Finding hidden gems...")
    hidden_gems = []
    for data in unique_comps_list:
        runs = data['timed'] + data['depleted']
        if runs < (total_runs * 0.005) and runs > 20: # Between 20 runs and 0.5% popularity
            success_rate = data['timed'] / runs
            if success_rate >= 0.75 and data['avg_key'] >= 10:
                score = success_rate * data['avg_key']
                hidden_gems.append((score, data))
                
    hidden_gems.sort(key=lambda x: x[0], reverse=True)
    hidden_gems_out = [x[1] for x in hidden_gems[:10]]

    # Glue Specs (Flexibility Index)
    # Number of distinctly timed high-key comps (e.g. timed > 5, avg key > 12) it appears in
    print("Calculating glue specs...")
    flex_stats = {}
    for data in unique_comps_list:
        if data['timed'] > 5 and data['avg_key'] > 12:
            for s in data['specs']:
                if s not in flex_stats:
                    flex_stats[s] = {'comps': 0, 'runs': 0, 'max_key': 0}
                flex_stats[s]['comps'] += 1
                flex_stats[s]['runs'] += data['timed'] + data['depleted']
                if data['max_key'] > flex_stats[s]['max_key']:
                    flex_stats[s]['max_key'] = data['max_key']
                
    glue_specs_raw = sorted(flex_stats.items(), key=lambda x: x[1]['comps'], reverse=True)[:10]
    glue_specs_list = [{'spec_id': k, **v} for k, v in glue_specs_raw]

    # Pre-calculate simple UI "Perfect Fit" data payload
    # We only need to send the top 2000 comps by weight to the frontend to keep json tiny
    unique_comps_list.sort(key=lambda x: x['weight'], reverse=True)
    top_comps = unique_comps_list[:2000]
    
    frontend_json = []
    for tc in top_comps:
        best_dungeon_id = max(tc['dungeons'].items(), key=lambda x: x[1]['t'] + x[1]['d'])[0] if tc['dungeons'] else 0
        best_dungeon_runs = sum(x for x in [tc['dungeons'].get(best_dungeon_id, {}).get('t', 0), tc['dungeons'].get(best_dungeon_id, {}).get('d', 0)])
        
        # Round the weights inside the dungeons dictionary
        for did, d_stats in tc['dungeons'].items():
            d_stats['w'] = round(d_stats['w'], 2)
            
        frontend_json.append({
            'c': tc['specs'],
            'w': round(tc['weight'], 2),
            't': tc['timed'],
            'd': tc['depleted'],
            'mk': tc['max_key'],
            'bd': best_dungeon_id,
            'bdr': best_dungeon_runs,
            'dungeons': tc['dungeons']
        })

    return frontend_json, synergy_matrix, hidden_gems_out, glue_specs_list


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
                specs_ui.append({
                    'id': int(sid),
                    'name': sdata.get('name', 'Unknown'),
                    'role': int(sdata.get('role', 2)),
                    'className': c_data.get('name', 'Unknown'),
                    'classId': int(c_id) if c_id else 0,
                    'icon': sdata.get('SpellIconFileId')
                })
        
        frontend_json, synergy_matrix, hidden_gems, glue_specs = calculate_comp_stats(conn, cursor, season, spec_lookup)
        
        # Save Perfect Fit JSON
        json_out_dir = os.path.join("assets", "json")
        os.makedirs(json_out_dir, exist_ok=True)
        with open(os.path.join(json_out_dir, "comps_index.json"), "w", encoding="utf-8") as f:
            json.dump(frontend_json, f, separators=(',', ':'))
            
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
            glue_specs=glue_specs,
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
