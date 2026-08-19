import os
import glob
import json
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
import compArchetypes
from pageGeneration import (
    generateSpecNav,
    generateDungeonNav,
    build_item_slug_map,
    ROLE_FOLDERS,
    build_trends,
    trend_feeds_for_dungeon,
)
from generateSpecPages import format_duration, format_utc_timestamp, format_iso_timestamp, load_json, load_season_info, upgrade_info
from generateItemPages import slot_for_item
from image_generation.dungeon_overview import createDungeonOverviewImg, fetch_route_thumbnail

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
    season_info = load_season_info(LOOKUP_DIR)
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    npcs_lookup = load_json(os.path.join(LOOKUP_DIR, "npcs.json"))
    # NPC model thumbnails downloaded by fetchNpcIcons.py -> data/icons/npc_<id>.png.
    # Set of npc_id strings that actually have an icon file, so the template only
    # renders <img> for npcs we have art for; the rest fall back to text.
    npc_icons = {
        os.path.basename(p)[len("npc_"):-len(".png")]
        for p in glob.glob(os.path.join("data", "icons", "npc_*.png"))
    }

    # Item metadata (from Raidbots) drives the "Best Loot" card: name/icon/ilvl/slot
    # plus the "sources" array that says which dungeon each item drops in.
    equippable_items = load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
    item_lookup = {it["id"]: it for it in equippable_items}
    item_slug_map = build_item_slug_map(item_lookup)

    # Reverse map: Blizzard journal instance id -> our dungeon key (challenge_mode_id).
    # journal_instance_id is written per dungeon by fetchDungeonData.py and equals the
    # instanceId Raidbots ships in each item's "sources". Without it a dungeon simply
    # has no loot to show (older data snapshots), so the card degrades to empty.
    instance_to_dungeon = {}
    for d_id, d_data in dungeon_lookup.items():
        jii = d_data.get("journal_instance_id")
        if jii is not None:
            instance_to_dungeon[int(jii)] = str(d_id)

    # dungeon key (str) -> set of item ids that drop there
    source_items_by_dungeon = defaultdict(set)
    for it in equippable_items:
        for src in it.get("sources", []) or []:
            d_id = instance_to_dungeon.get(src.get("instanceId"))
            if d_id:
                source_items_by_dungeon[d_id].add(it["id"])

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
    env.filters["iso_ts"] = format_iso_timestamp
    env.filters["upgrade_info"] = upgrade_info
    template = env.get_template(os.path.basename(template_path))

    os.makedirs(output_dir, exist_ok=True)

    conn = databaseConnector.get_connection()
    thumbnail_executor = ThreadPoolExecutor(max_workers=8)
    try:
        current_season = season_info.get('blizzard_season_id', None)
        if not current_season:
            raise ValueError("Current season ID not found in seasonInfo.json")

        with conn.cursor(dictionary=True) as cursor:
            databaseConnector.configure_read_session(conn, cursor)

            # Same ids the lust queries filter on, so the "More Details" heatmap
            # link shows exactly the spells the Most Lusted Pulls table counted.
            bloodlust_spell_ids = databaseConnector.fetch_bloodlust_spell_ids(conn, cursor)
            if not bloodlust_spell_ids:
                raise RuntimeError(
                    "bloodlust_spells is empty — the lust timeline and the heatmap link both depend on it."
                )

            print("Pre-fetching global dungeon success rates...")
            all_dungeon_runs = databaseConnector.fetch_runs_per_dungeon(conn, cursor, current_season)
            dungeon_runs_lookup = {str(d['dungeon_id']): d for d in all_dungeon_runs}
            
            all_dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, current_season)
            dungeon_runs_per_level_lookup = {}
            for d in all_dungeon_runs_per_level:
                d_id = str(d['dungeon_id'])
                if d_id not in dungeon_runs_per_level_lookup:
                    dungeon_runs_per_level_lookup[d_id] = []
                dungeon_runs_per_level_lookup[d_id].append(d)

            # Fetch every dungeon's top routes first and kick off its
            # keystone.guru thumbnail job in a background thread (pure HTTP):
            # the remote render job's queue/poll wait then overlaps all the
            # per-dungeon DB work below instead of blocking each page for up
            # to 2.5 minutes.
            top_routes_by_dungeon = {}
            thumbnail_futures = {}
            for dungeon_id, dungeon_data in dungeon_lookup.items():
                if target_dungeon and str(dungeon_id) != str(target_dungeon):
                    continue
                print(f"Fetching top routes for {dungeon_data['name']['en_US']} ({dungeon_id})")
                top_routes = databaseConnector.fetch_dungeon_top_routes(conn, cursor, dungeon_id)
                top_routes_by_dungeon[dungeon_id] = top_routes
                if top_routes and top_routes[0].get('route_key'):
                    print(f"Requesting thumbnail for top route: {top_routes[0]['route_key']}")
                    thumbnail_futures[dungeon_id] = thumbnail_executor.submit(
                        fetch_route_thumbnail, dungeon_id, top_routes[0]['route_key']
                    )

            # Team-comp families per dungeon (same clustering as the comps page), so each
            # dungeon's popular comps are grouped with their flexible alternates. One scan
            # + one clustering pass, indexed by dungeon.
            print("Clustering team comps for dungeon pages...")
            team_families_by_dungeon = compArchetypes.build_dungeon_archetypes(
                compArchetypes.collapse_comps(
                    databaseConnector.fetch_all_comps(conn, cursor, current_season),
                    spec_lookup),
                spec_lookup, class_lookup,
                [str(k) for k in dungeon_lookup.keys()], top_n=6)

            # Global per-item usage across the meta, so each dungeon's loot can be
            # ranked by how much the current playerbase actually equips each drop.
            # Reuses the per-spec indexed query the item pages use (fast path);
            # summed across specs = total equipping runs, and the spec with the most
            # runs on an item is the "most used by" credit shown on the card.
            print("Pre-fetching item usage for dungeon loot ranking...")
            item_total_runs = defaultdict(int)
            item_top_spec = {}  # item_id -> (spec_id str, runs)
            for spec_id in spec_lookup.keys():
                try:
                    spec_id_int = int(spec_id)
                except (TypeError, ValueError):
                    continue
                rows = databaseConnector.fetch_item_spec_usage(conn, cursor, current_season, spec_id_int)
                for row in rows or []:
                    # item_id is VARCHAR in global_aggregated_items, so normalise to
                    # int to match the equippable-items.json ids the loot join uses.
                    raw_iid = row['item_id']
                    if not str(raw_iid).isdigit():
                        continue
                    iid = int(raw_iid)
                    runs = row['run_count'] or 0
                    if runs <= 0:
                        continue
                    item_total_runs[iid] += runs
                    prev = item_top_spec.get(iid)
                    if prev is None or runs > prev[1]:
                        item_top_spec[iid] = (str(spec_id), runs)

            for dungeon_id, dungeon_data in dungeon_lookup.items():
                if target_dungeon and str(dungeon_id) != str(target_dungeon):
                    continue

                print(f"Generating dungeon page for {dungeon_data['name']['en_US']} ({dungeon_id})")

                # Overall run totals for this dungeon, consumed by the social overview
                # image (createDungeonOverviewImg dungeon_totals=...).
                local_total_res = databaseConnector.fetch_dungeon_totals(conn, cursor, dungeon_id, current_season)

                # Best loot: the items that drop in this dungeon, ranked by how much
                # the current meta actually equips them. Drop source comes from the
                # Raidbots "sources" field, joined to this dungeon via journal_instance_id;
                # usage comes from the global per-item sweep above.
                loot = []
                for iid in source_items_by_dungeon.get(str(dungeon_id), ()):
                    runs = item_total_runs.get(iid, 0)
                    if runs <= 0:
                        continue
                    item = item_lookup.get(iid)
                    if not item:
                        continue
                    slot_label, _slot_key = slot_for_item(item)
                    top_spec_id, _top_runs = item_top_spec.get(iid, (None, 0))
                    top_spec = spec_lookup.get(top_spec_id) if top_spec_id else None
                    top_class = class_lookup.get(str(top_spec.get('classID', '')), {}) if top_spec else {}
                    loot.append({
                        'id': iid,
                        'name': item.get('name', 'Unknown'),
                        'icon': item.get('icon', ''),
                        'quality': item.get('quality'),
                        'ilvl': item.get('itemLevel'),
                        'slot': slot_label,
                        'slug': item_slug_map.get(iid),
                        'runs': runs,
                        'top_spec_name': top_spec.get('name') if top_spec else None,
                        'top_class_name': top_class.get('name') if top_spec else None,
                        'top_spec_role': str(top_spec.get('role', 2)) if top_spec else None,
                        'top_spec_icon': top_spec.get('SpellIconFileId', '') if top_spec else None,
                    })

                loot.sort(key=lambda x: x['runs'], reverse=True)
                top_loot = loot[:8]

                # Clustered team-comp families for this dungeon (popular ranking).
                team_comp_families = team_families_by_dungeon.get(
                    str(dungeon_id), {}).get('popular', [])

                # Top routes were fetched in the thumbnail pre-pass above
                top_routes = top_routes_by_dungeon.get(dungeon_id, [])

                closest_call_run = parse_run_rows(databaseConnector.fetch_dungeon_closest_call_run(conn, cursor, dungeon_id, current_season))
                shortest_run = parse_run_rows(databaseConnector.fetch_dungeon_shortest_run(conn, cursor, dungeon_id, current_season))
                longest_run = parse_run_rows(databaseConnector.fetch_dungeon_longest_run(conn, cursor, dungeon_id, current_season))
                highest_run = parse_run_rows(databaseConnector.fetch_dungeon_max_key_run(conn, cursor, dungeon_id, current_season))
                fastest_top_run = parse_run_rows(databaseConnector.fetch_dungeon_fastest_top_levels_run(conn, cursor, dungeon_id, current_season))

                lust_timeline = databaseConnector.fetch_dungeon_lust_timeline(conn, cursor, dungeon_id)
                skip_rates = databaseConnector.fetch_dungeon_skip_rates(conn, cursor, dungeon_id, current_season)
                
                # one batched round trip each instead of one query per skip/pull
                skip_examples = databaseConnector.fetch_example_skip_routes(
                    conn, cursor, dungeon_id, [skip['npc_id'] for skip in skip_rates[:15]]
                )
                for skip in skip_rates[:15]:
                    example_route = skip_examples.get(skip['npc_id'])
                    if example_route:
                        skip['example_route'] = example_route

                lust_examples = databaseConnector.fetch_example_lust_routes(
                    conn, cursor, dungeon_id,
                    [pull['top_npcs'] for pull in lust_timeline if pull.get('top_npcs')],
                )
                for pull in lust_timeline:
                    top_npcs_str = pull.get('top_npcs', '')
                    if top_npcs_str:
                        example_lust_route = lust_examples.get(top_npcs_str)
                        if example_lust_route:
                            pull['example_route'] = example_lust_route

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

                # Every NPC in a "Most Lusted Pulls" composition must resolve to a name via
                # npcs.json, otherwise the template silently renders the raw id (e.g. 261552).
                # A miss means npcs.json is stale relative to the seeded/collected pull_enemies
                # ids: fail loudly here rather than shipping a page full of bare numbers.
                npc_names = npcs_lookup.get('en_US', {})
                missing_npc_ids = set()
                for pull in lust_timeline:
                    top_npcs_str = pull.get('top_npcs', '')
                    if top_npcs_str:
                        for n in str(top_npcs_str).split(','):
                            n = n.strip()
                            if n and n not in npc_names:
                                missing_npc_ids.add(n)
                if missing_npc_ids:
                    raise ValueError(
                        f"NPC name lookup is out of date: dungeon '{dungeon_data['name']['en_US']}' "
                        f"({dungeon_id}) has 'Most Lusted Pulls' NPC(s) with no entry in "
                        f"data/static/npcs.json, so the page would render bare ids instead of names: "
                        f"{sorted(int(n) for n in missing_npc_ids)}. "
                        f"Fix: run 'python backend_scripts/fetchNpcInfo.py' first before running this generator."
                    )

                # Fetch Overall Stats
                d_id_str = str(dungeon_id)
                overall_stats = dungeon_runs_lookup.get(d_id_str, {})
                level_stats = dungeon_runs_per_level_lookup.get(d_id_str, [])

                # Cards the user can toggle between inside a single card
                toggle_runs = [
                    {"name": "closest-call", "label": "Closest Call", "tab": "Closest", "data": closest_call_run, "icon": "timer", "show_margin": True},
                    {"name": "shortest", "label": "Shortest Run", "tab": "Shortest", "data": shortest_run, "icon": "sprint"},
                    {"name": "longest", "label": "Longest Run", "tab": "Longest", "data": longest_run, "icon": "hourglass_bottom"},
                ]
                # Stand-alone cards
                single_runs = [
                    {"name": "highest", "label": "Highest Run", "data": highest_run, "icon": "leaderboard"},
                    {"name": "fastest-top", "label": "Fastest (Top 3 Keys)", "data": fastest_top_run, "icon": "bolt",
                     "subtitle": "Fastest clear among the 3 highest key levels"},
                ]
                
                trends = build_trends(
                    conn,
                    cursor,
                    trend_feeds_for_dungeon(dungeon_id),
                    {"specs": spec_lookup},
                )

                output_html = template.render(dungeon=dungeon_data,
                    toggle_runs=toggle_runs,
                    trends=trends,
                    single_runs=single_runs,
                    lust_timeline=lust_timeline,
                    bloodlust_spell_ids=bloodlust_spell_ids,
                    skip_rates=skip_rates,
                    npcs=npcs_lookup,
                    npc_icons=npc_icons,
                    bosses=bosses_lookup.get(dungeon_id, []),
                    top_routes=top_routes,
                    team_comp_families=team_comp_families,
                    top_loot=top_loot,
                    overall_stats=overall_stats,
                    level_stats=level_stats,
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
                    # resolve the background thumbnail job started in the
                    # pre-pass (None = no top route / job failed → no map)
                    thumb_future = thumbnail_futures.get(dungeon_id)
                    route_thumbnail = thumb_future.result() if thumb_future else None
                    # pass the already-fetched data so the image step doesn't
                    # re-run the same queries (incl. the season-wide per-level
                    # rollup, previously re-scanned once per dungeon)
                    createDungeonOverviewImg(
                        tmpdir=os.path.join("tmp", "img"),
                        out_path=preview_path,
                        dungeon_id=dungeon_id,
                        season=current_season,
                        conn=conn,
                        cursor=cursor,
                        dungeon_totals=local_total_res,
                        per_level=level_stats,
                        top_routes_data=top_routes,
                        route_thumbnail=route_thumbnail,
                    )
                except Exception as e:
                    print(f"Failed to generate preview for {slug}: {e}")
                
                if debug:
                    break
    finally:
        thumbnail_executor.shutdown(wait=False)
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
