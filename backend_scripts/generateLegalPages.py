import os
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
import argparse
from generateSpecPages import (
    LOOKUP_DIR,
    ROLE_FOLDERS,
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

    spec_nav = {role_name: [] for role_name in ROLE_FOLDERS.values()}

    for sid, sdata in spec_lookup.items():
        role_key = str(sdata.get("role", 2))
        role_name = ROLE_FOLDERS.get(role_key, "Other")
        class_data = class_lookup.get(str(sdata.get("classID", "")), {})
        filename = f"{sdata['name']}_{class_data.get('name')}"
        spec_nav[role_name].append(
            {
                "name": f"{sdata['name']} {class_data.get('name')}",
                "url": f"/classes/{role_name}/{filename}",
                "icon": sdata.get("SpellIconFileId"),
                "color": {
                    "r": class_data.get("color", {}).get("r", 0),
                    "g": class_data.get("color", {}).get("g", 0),
                    "b": class_data.get("color", {}).get("b", 0),
                },
            }
        )

    # Optionally sort each list by name:
    for lst in spec_nav.values():
        lst.sort(key=lambda x: x["name"])


    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
    )

    # Write output
    out_path = os.path.join(
        output_dir,
        "privacy.html",
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
