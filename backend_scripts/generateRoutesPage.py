import os
import sys
import json
import argparse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import defaultdict
from datetime import datetime, timezone

# project imports (adjust paths if necessary)
from pageGeneration import generateSpecNav
from generateSpecPages import (
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
    LOOKUP_DIR,
)

import databaseConnector


def fail(msg):
    print("ERROR:", msg, file=sys.stderr)
    sys.exit(2)


def main(template_path, output_dir, limit):
    # ensure env vars exist

    if (
        not os.environ.get("DATABASE_HOST")
        or not os.environ.get("DATABASE_USER")
        or not os.environ.get("DATABASE_PASSWORD")
    ):
        fail(
            "Missing DB credentials. Ensure DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD are set in the environment."
        )

    # jinja env
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
    spell_lookup = load_json(os.path.join(LOOKUP_DIR, "spells.json"))
    npc_lookup = load_json(os.path.join(LOOKUP_DIR, "npcs.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))

    # init DB pool (this will raise on error)
    try:
        databaseConnector.init_connection_pool(
            os.environ.get("DATABASE_HOST"),
            os.environ.get("DATABASE_USER"),
            os.environ.get("DATABASE_PASSWORD"),
            os.environ.get("DATABASE_NAME"),
            os.environ.get("DATABASE_PORT"),
            1,
        )
    except Exception as e:
        fail(f"init_connection_pool failed: {e}")

    conn = None
    try:
        conn = databaseConnector.get_connection()
        cursor = conn.cursor()
    except Exception as e:
        fail(f"Failed to obtain DB connection: {e}")

    try:
        comp_routes = databaseConnector.fetch_comp_routes(
            conn, cursor, limit=limit if limit and limit > 0 else None
        )
        if not isinstance(comp_routes, dict):
            fail("fetch_comp_routes returned unexpected type (expected dict).")
        npc_map = {}
        for dungeon in dungeon_lookup:
            npc_ids = databaseConnector.fetch_distinct_npc_ids_for_dungeon(
                conn, cursor, dungeon
            )
            npc_map[dungeon] = npc_ids
    except Exception as e:
        fail(f"Error fetching data from DB: {e}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    # deterministic JSON embed
    comp_routes_json = json.dumps(
        comp_routes, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )

    # Build comp_routes_by_dungeon expected by template
    comp_routes_by_dungeon = defaultdict(list)
    for key, info in comp_routes.items():
        info_copy = dict(info)
        info_copy["specs"] = info_copy.get(
            "specs", key.split(",") if key != "unknown" else []
        )
        comp_routes_by_dungeon[str(info_copy.get("dungeon"))].append(info_copy)

    for runs in comp_routes_by_dungeon.values():
        runs.sort(key=lambda r: r.get("level", 0), reverse=True)

    # slug_lookup (template expects slug_lookup[slug] = {..., _id: ...})
    slug_lookup = {}
    for slug, d in dungeon_lookup.items():
        slug_lookup[slug] = {**d, "_id": slug}

    # render
    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=generateSpecNav(
            spec_lookup, class_lookup
        ),  # minimal nav; replace by real spec table if available
        comp_routes=comp_routes_json,
        comp_routes_by_dungeon=comp_routes_by_dungeon,
        slug_lookup=slug_lookup,
        dungeon_lookup=dungeon_lookup,
        specs=spec_lookup,
        class_lookup=class_lookup,
        spell_lookup=spell_lookup,
        npc_lookup=npc_lookup,
        npc_map=npc_map,
        season_info=season_info,
        active_page="routes",
        notifications=notifications,
        breadcrumbs=[
            {"title": "Pages", "href": "/pages"},
            {"title": "Routes", "href": "/routes"},
        ],
    )
    comp_routes_path = os.path.join("assets", "json", "compRoutes.json")
    with open(comp_routes_path, "w", encoding="utf-8") as fh:
        # pretty or compact — compact reduces transfer time
        json.dump(comp_routes, fh, separators=(",", ":"), ensure_ascii=False)
    print(f"Wrote compRoutes JSON to {comp_routes_path}")

    out_path = os.path.join(output_dir, "routes.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(output_html)

    print(f"Generated {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate routes.html using DB-only data"
    )
    parser.add_argument("--template", required=True, help="Path to Jinja template file")
    parser.add_argument(
        "--output_dir", required=True, help="Output directory to write generated HTML"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit to number of routes pulled (0 = no limit)",
    )
    args = parser.parse_args()
    main(args.template, args.output_dir, args.limit)
