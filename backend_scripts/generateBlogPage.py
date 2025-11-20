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
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    posts = load_json(os.path.join("data", "socials.json"))

    posts = OrderedDict(
        sorted(posts.items(), key=lambda t: t[1]["timestamp"], reverse=True)
    )

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    template = env.get_template(os.path.basename("blog.html"))
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
        breadcrumbs=[{"title": "Blog"}],
        active_page="blog",
        notifications=notifications,
        posts=posts,
    )
    # Write output
    if os.path.dirname(os.path.join("pages", "blog.html")):
        os.makedirs(os.path.dirname(os.path.join("pages", "blog.html")), exist_ok=True)
    with open(os.path.join("pages", "blog.html"), "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {os.path.join('pages', 'blog.html')}")


if __name__ == "__main__":
    main()
