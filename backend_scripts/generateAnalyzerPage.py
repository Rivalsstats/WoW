"""Generate the client-side "Am I meta?" analyzer page.

The page is fully static: analyzer.js parses a pasted SimulationCraft addon
export in the browser, resolves the spec, fetches the small per-spec meta JSON
that generateSpecPages.py bakes to /assets/json/spec_meta/<spec_id>.json, and
renders a slot-by-slot gap report. This generator only bakes the lookup tables
the JS needs to turn a SimC export's class/spec tokens into a spec_id — no DB
access required.
"""
import os
import json
import argparse
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pageGeneration import generateSpecNav, generateDungeonNav
from generateSpecPages import LOOKUP_DIR, load_json

TEMPLATE_PATH = "templates"

# SimulationCraft class tokens (the lowercase word before '="charname"' in an
# export) mapped to the WoW class id used in specs.json/classes.json. Stable.
SIMC_CLASS_TOKENS = {
    "deathknight": 6,
    "demonhunter": 12,
    "druid": 11,
    "evoker": 13,
    "hunter": 3,
    "mage": 8,
    "monk": 10,
    "paladin": 2,
    "priest": 5,
    "rogue": 4,
    "shaman": 7,
    "warlock": 9,
    "warrior": 1,
}


def build_spec_index(spec_lookup, class_lookup):
    """`{"<classID>|<specname_lower>": spec_id}` plus a display map, so the JS can
    resolve a SimC (class token, spec token) pair to a spec_id and label it."""
    index = {}
    display = {}
    for sid, sdata in spec_lookup.items():
        class_id = str(sdata.get("classID"))
        name = sdata.get("name") or ""
        key = f"{class_id}|{name.lower()}"
        index[key] = int(sid)
        class_name = (class_lookup.get(class_id) or {}).get("name", "")
        display[int(sid)] = {
            "name": name,
            "class": class_name,
            "icon": sdata.get("SpellIconFileId"),
            "role": int(sdata.get("role", 2)),
        }
    return index, display


def main(template_path, output_dir):
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))

    spec_index, spec_display = build_spec_index(spec_lookup, class_lookup)

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path) or TEMPLATE_PATH),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=generateSpecNav(spec_lookup, class_lookup),
        dungeon_nav=generateDungeonNav(dungeon_lookup),
        notifications=notifications,
        season_info=season_info,
        active_page="analyzer",
        cur_page="analyzer",
        breadcrumbs=[
            {"title": "Pages", "href": "/pages"},
            {"title": "Am I Meta?"},
        ],
        simc_class_tokens=SIMC_CLASS_TOKENS,
        spec_index=spec_index,
        spec_display=spec_display,
    )

    out_path = os.path.join(output_dir, "analyzer.html")
    if os.path.dirname(out_path):
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default=os.path.join("templates", "analyzer.html"))
    parser.add_argument("--output_dir", default="pages")
    args = parser.parse_args()
    main(args.template, args.output_dir)
