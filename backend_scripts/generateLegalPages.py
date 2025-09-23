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
        "output": os.path.join("pages", "privacy.html"),
        "breadcrumbs": [
            {"title": "Pages", "href": "/Pages"},
            {"title": "Privacy"}
        ]
    },
    "impressum": {
        "template": "impressum.html",
        "output": os.path.join("pages", "impressum.html"),
        "breadcrumbs": [
            {"title": "Pages", "href": "/Pages"},
            {"title": "Impressum"}
        ]
    },
    "404": {
        "template": "404Page.html",
        "output": "404.html",
        "breadcrumbs": [
            {"title": "Not Found"}
        ]
    },
    "AboutUs": {
        "template": "AboutUs.html",
        "output": os.path.join("pages", "about.html"),
        "breadcrumbs": [
            {"title": "About Us"}
        ]
    },
    "Search": {
        "template": "Search.html",
        "output": os.path.join("pages", "search.html"),
        "breadcrumbs": [
            {"title": "Search"}
        ]
    }
}

def main():
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
        if os.path.dirname(value["output"]):
            os.makedirs(os.path.dirname(value["output"]), exist_ok=True)
        with open(value["output"], "w", encoding="utf-8") as f:
            f.write(output_html)
        print(f"Generated {value['output']}")


if __name__ == "__main__":
    main()
