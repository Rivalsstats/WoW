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
TEMPLATE_PATH = "templates"
LEGAL_PAGES = {
    "privacy": {
        "template": "privacy.html",
        "breadcrumbs": [
            {"title": "Pages", "href": "/Pages"},
            {"title": "Privacy"}
        ]
    },
    "impressum": {
        "template": "impressum.html",
        "breadcrumbs": [
            {"title": "Pages", "href": "/Pages"},
            {"title": "Impressum"}
        ]
    }
}

def main(output_dir):
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_PATH),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))

    spec_nav = generateSpecNav(spec_lookup, class_lookup)

    for page, value in LEGAL_PAGES.items():
        template_name = value["template"]
        template = env.get_template(os.path.basename(template_name))
        output_html = template.render(
            generated_at=datetime.now(timezone.utc).timestamp(),
            spec_nav=spec_nav,
            breadcrumbs=value.get("breadcrumbs", [])
        )
        # Write output
        out_path = os.path.join(
            output_dir,
            template_name,
        )
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(output_html)
        print(f"Generated {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW Dashboard page")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    args = parser.parse_args()
    main(args.output_dir)
