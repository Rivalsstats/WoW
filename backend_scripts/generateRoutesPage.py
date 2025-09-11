import os
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
import argparse
from pageGeneration import generateSpecNav
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
)

def main(template_path, output_dir):
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
    comp_routes = load_json(os.path.join(LOOKUP_DIR,  'compRoutes.json'))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))

    spec_nav = generateSpecNav(spec_lookup, class_lookup)

    comp_routes_by_dungeon = defaultdict(list)
    for key, info in comp_routes.items():
        # split the spec‑IDs into a list for later icon loops
        info = dict(info)                # shallow copy
        info['specs'] = key.split(',')
        comp_routes_by_dungeon[info['dungeon']].append(info)

    for runs in comp_routes_by_dungeon.values():
        runs.sort(key=lambda r: r['level'], reverse=True)

    slug_lookup = {
        d["slug"]: { **d, "_id": did }
        for did, d in dungeon_lookup.items()
    }

    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
        comp_routes=json.dumps(comp_routes),
        comp_routes_by_dungeon=comp_routes_by_dungeon,
        slug_lookup=slug_lookup,
        dungeon_lookup=dungeon_lookup,
        specs = spec_lookup,
        class_lookup=class_lookup,
        season_info=season_info,
        active_page="routes",
        breadcrumbs=[
            {"title": "Pages", "href": "/Pages"},
            {"title": "Routes", "href": "/Routes"}
        ]
    )

    # Write output
    out_path = os.path.join(
        output_dir,
        "routes.html",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW Dashboard page")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    args = parser.parse_args()
    main(args.template, args.output_dir)
