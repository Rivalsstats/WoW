import os
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from datetime import datetime, timezone
import argparse
from tierMath import build_buff_tiers, build_ckmeans_tiers, build_spec_tiers
from contextlib import closing
from pageGeneration import generateSpecNav, ROLE_FOLDERS, generateDungeonNav
from aggregateData import get_current_season_id, get_access_token
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
)

# config
CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)


def main(template_path, output_dir):
    # local import: keeps matplotlib/PIL out of the import path until actually rendering
    from image_generation.spec_popularity_performance import create_spec_popularity_vs_performance_img
    print("Generating index page...")
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    group_buffs = load_json(os.path.join(LOOKUP_DIR, "groupbuffs.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    buff_lookup = {b.get("id"): b for b in group_buffs}

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)
    template = env.get_template(os.path.basename(template_path))

    token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    current_season = get_current_season_id(token)
    print(f"Fetching database data {current_season}...")
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, current_season
        )
        spec_data = databaseConnector.fetch_spec_upgrades(conn, cursor)
        groupbuffs_stats = databaseConnector.fetch_groupbuffs_stats(
            conn, cursor, group_buffs, current_season, 12, 14
        )
    print(groupbuffs_stats)
    print("Building tiers...")
    dungeon_tiers = build_ckmeans_tiers(
        dungeon_lookup, dungeon_data, weight_base=1.6, k=6
    )
    spec_tiers = build_spec_tiers(
        spec_lookup, class_lookup, spec_data, weight_base=1.6, k=6
    )
    buff_tiers = build_buff_tiers(buff_lookup, groupbuffs_stats)

    print("Rendering template...")
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
        dungeon_nav=dungeon_nav,
        dungeon_lookup=dungeon_lookup,
        specs=spec_lookup,
        class_lookup=class_lookup,
        active_page="home",
        notifications=notifications,
        breadcrumbs=[
            {"title": "Home", "href": "/"},
        ],
        dungeon_tiers=dungeon_tiers,
        dungeon_scores_available=bool(dungeon_data),
        spec_tiers=spec_tiers,
        spec_scores_available=bool(spec_data),
        season=current_season,
        role_lookup=ROLE_FOLDERS,
        buff_tiers=buff_tiers,
        buff_lookup=buff_lookup,
        buff_scores_available=bool(groupbuffs_stats),
        season_info=season_info,
    )

    # Write output
    out_path = os.path.join(
        output_dir,
        "index.html",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path}")
    print("Generating spec popularity vs performance image...")
    preview_path = os.path.join("assets", "img", "previews", "spec_popularity_vs_performance.png")
    os.makedirs(os.path.dirname(preview_path), exist_ok=True)
    create_spec_popularity_vs_performance_img(preview_path, current_season)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ndex page")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    args = parser.parse_args()
    main(args.template, args.output_dir)
