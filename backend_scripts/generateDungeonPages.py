import os
import json
import argparse
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from pageGeneration import generateSpecNav, generateDungeonNav, ROLE_FOLDERS
from generateSpecPages import format_duration, format_utc_timestamp, load_json, upgrade_info
from generateSocialsPost import createDungeonOverviewImg

LOOKUP_DIR = "data/static"

def parse_run_rows(rows):
    if not rows:
        return None
    rows = list(rows)
    if not rows:
        return None

    first = rows[0]
    is_dict = isinstance(first, dict)
    
    seen = set()
    members = []
    for r in rows:
        mid = r['member'] if is_dict else r[8]
        mspec = r['spec_id'] if is_dict else r[9]
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        members.append({
            "member_id": int(mid),
            "spec_id": int(mspec) if mspec is not None else None,
        })

    if is_dict:
        return {
            "run_id": int(first['run_id']) if first.get('run_id') is not None else None,
            "dungeon_id": first.get('dungeon_id'),
            "keystone_level": int(first['keystone_level']) if first.get('keystone_level') is not None else None,
            "duration": int(first['duration']) if first.get('duration') is not None else None,
            "timestamp": int(first['timestamp']) if first.get('timestamp') is not None else None,
            "faction": first.get('faction'),
            "region": first.get('region'),
            "season": int(first['season']) if first.get('season') is not None else None,
            "members": members,
        }
    else:
        return {
            "run_id": int(first[5]) if len(first) > 5 and first[5] is not None else None,
            "dungeon_id": first[0] if len(first) > 0 else None,
            "keystone_level": int(first[1]) if len(first) > 1 and first[1] is not None else None,
            "duration": int(first[2]) if len(first) > 2 and first[2] is not None else None,
            "timestamp": int(first[3]) if len(first) > 3 and first[3] is not None else None,
            "faction": first[4] if len(first) > 4 else None,
            "region": first[6] if len(first) > 6 else None,
            "season": int(first[7]) if len(first) > 7 and first[7] is not None else None,
            "members": members,
        }

def main(template_path, output_dir, debug=False, target_dungeon=None):
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    npcs_lookup = load_json(os.path.join(LOOKUP_DIR, "npcs.json"))
    
    try:
        with open(os.path.join('data', 'boss_npcs.json'), 'r') as f:
            bosses_lookup = json.load(f)
    except FileNotFoundError:
        bosses_lookup = {}
        
    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
    template = env.get_template(os.path.basename(template_path))

    os.makedirs(output_dir, exist_ok=True)

    conn = databaseConnector.get_connection()
    try:
        current_season = season_info.get('blizzard_season_id', None)
        if not current_season:
            raise ValueError("Current season ID not found in seasonInfo.json")

        with conn.cursor(dictionary=True) as cursor:
            print("Pre-fetching global spec populations for relative comparison...")
            global_total = databaseConnector.fetch_global_totals(conn, cursor, current_season)
            global_total_count = global_total[0]['total'] if global_total and global_total[0]['total'] else 1

            for dungeon_id, dungeon_data in dungeon_lookup.items():
                if target_dungeon and str(dungeon_id) != str(target_dungeon):
                    continue
                
                print(f"Generating dungeon page for {dungeon_data['name']['en_US']} ({dungeon_id})")

                # Over-represented specs
                local_total_res = databaseConnector.fetch_dungeon_totals(conn, cursor, dungeon_id, current_season)
                local_total = local_total_res[0]['total'] if local_total_res and local_total_res[0]['total'] else 1
                
                ratios = databaseConnector.fetch_dungeon_specs_ratio(conn, cursor, dungeon_id, current_season)
                over_represented = []
                for r in ratios:
                    s_id = str(r['spec_id'])
                    local_runs = r['local_runs']
                    global_runs = r['global_runs']
                    if local_runs < 50: continue
                    
                    local_pct = local_runs / local_total
                    global_pct = global_runs / global_total_count
                    if global_pct == 0: continue

                    diff_pct = local_pct - global_pct
                    relative_diff_pct = diff_pct / global_pct
                    
                    if diff_pct > 0 and s_id in spec_lookup:
                        s_data = spec_lookup[s_id]
                        c_id = str(s_data.get('classID', ''))
                        over_represented.append({
                            'spec_id': s_id,
                            'spec_name': s_data.get('name', 'Unknown'),
                            'class_name': class_lookup.get(c_id, {}).get('name', 'Unknown'),
                            'role': str(s_data.get('role', 2)),
                            'icon': s_data.get('SpellIconFileId', ''),
                            'diff_pct': diff_pct * 100,
                            'relative_diff_pct': relative_diff_pct * 100,
                            'ratio': local_pct / global_pct
                        })
                
                over_represented.sort(key=lambda x: x['relative_diff_pct'], reverse=True)
                top_over_represented = over_represented[:5]
                
                # Fetch top comps for this dungeon
                print(f"Fetching top comps for {dungeon_id}...")
                comps_rows = databaseConnector.fetch_dungeon_top_comps(conn, cursor, dungeon_id, current_season)
                
                print(f"Fetched {len(comps_rows)} comps for dungeon {dungeon_id}")
                print(comps_rows)
                top_comps = []
                for r in comps_rows:
                    if r['comp']:
                        top_comps.append({
                            'specs': r['comp'].split(','),
                            'count': r['comp_count']
                        })

                # Fetch top routes for this dungeon
                top_routes = databaseConnector.fetch_dungeon_top_routes(conn, cursor, dungeon_id)

                shortest_run = parse_run_rows(databaseConnector.fetch_dungeon_shortest_run(conn, cursor, dungeon_id, current_season))
                longest_run = parse_run_rows(databaseConnector.fetch_dungeon_longest_run(conn, cursor, dungeon_id, current_season))
                highest_run = parse_run_rows(databaseConnector.fetch_dungeon_max_key_run(conn, cursor, dungeon_id, current_season))

                lust_timeline = databaseConnector.fetch_dungeon_lust_timeline(conn, cursor, dungeon_id)
                skip_rates = databaseConnector.fetch_dungeon_skip_rates(conn, cursor, dungeon_id, current_season)
                
                for skip in skip_rates[:15]:
                    example_route = databaseConnector.fetch_example_skip_route(conn, cursor, dungeon_id, skip['npc_id'])
                    if example_route:
                        skip['example_route'] = example_route[0]
                        
                for pull in lust_timeline:
                    top_npcs_str = pull.get('top_npcs', '')
                    if top_npcs_str:
                        example_lust_route = databaseConnector.fetch_example_lust_route(conn, cursor, dungeon_id, top_npcs_str)
                        if example_lust_route:
                            pull['example_route'] = example_lust_route[0]

                # Validate lust_timeline contains at least one boss pull
                dungeon_bosses = bosses_lookup.get(str(dungeon_id), [])
                has_boss_lust = False
                for pull in lust_timeline:
                    top_npcs_str = pull.get('top_npcs', '')
                    if top_npcs_str:
                        for n in str(top_npcs_str).split(','):
                            if n.strip() and int(n.strip()) in dungeon_bosses:
                                has_boss_lust = True
                                break
                    if has_boss_lust:
                        break
                
                # Only throw a validation error if there is actually lust data available
                if lust_timeline and len(lust_timeline) > 0 and not has_boss_lust:
                    raise RuntimeError(f"Dungeon {dungeon_data['name']['en_US']} ({dungeon_id}) has no lust pull marked as a boss. This indicates missing boss NPC data in data/boss_npcs.json.")

                runs_cards = [
                    {"name": "Shortest", "data": shortest_run, "icon": "sprint"},
                    {"name": "Longest", "data": longest_run, "icon": "hourglass_bottom"},
                    {"name": "Highest", "data": highest_run, "icon": "leaderboard"},
                ]
                
                output_html = template.render(dungeon=dungeon_data,
                    runs=runs_cards,
                    lust_timeline=lust_timeline,
                    skip_rates=skip_rates,
                    npcs=npcs_lookup,
                    bosses=bosses_lookup.get(dungeon_id, []),
                    top_routes=top_routes,
                    top_comps=top_comps,
                    top_over_represented=top_over_represented,
                    generated_at=datetime.now(timezone.utc).timestamp(),
                    specs=spec_lookup,
                    spec_nav=spec_nav,
                    role_lookup=ROLE_FOLDERS,
                    dungeon_nav=dungeon_nav,
                    current_dungeon=dungeon_data['name']['en_US'],
                    dungeon_id=dungeon_id,
                    page_title=dungeon_data['name']['en_US'],
                    season_info=season_info,
                    notifications=notifications,
                    breadcrumbs=[
                        {"title": "Pages", "href": "/pages"},
                        {"title": "Dungeons", "href": "/dungeons"},
                    ],)
                
                
                slug = dungeon_data['slug']
                out_path = os.path.join(output_dir, f"{slug}.html")
                with open(out_path, "w", encoding="utf-8") as outf:
                    outf.write(output_html)
                    
                # Create Preview Image
                preview_dir = os.path.join("assets", "img", "previews")
                os.makedirs(preview_dir, exist_ok=True)
                preview_path = os.path.join(preview_dir, f"{slug}.png")
                try:
                    createDungeonOverviewImg(
                        tmpdir=os.path.join("tmp", "img"),
                        out_path=preview_path,
                        dungeon_id=dungeon_id,
                        season=current_season,
                        conn=conn,
                        cursor=cursor
                    )
                except Exception as e:
                    print(f"Failed to generate preview for {slug}: {e}")
                
                if debug:
                    break
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW M+ dungeon pages")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument("--output_dir", required=True, help="Directory to write generated HTML pages")
    parser.add_argument("--debug", required=False, action="store_true")
    parser.add_argument("--dungeon", required=False)

    args = parser.parse_args()

    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST", "127.0.0.1"),
        os.environ.get("DATABASE_USER", "root"),
        os.environ.get("DATABASE_PASSWORD", ""),
        os.environ.get("DATABASE_NAME", "Mythistone"),
        os.environ.get("DATABASE_PORT", "3306"),
        1,
    )
    main(args.template, args.output_dir, args.debug, args.dungeon)
