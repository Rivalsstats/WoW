import os
import re
import argparse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime, timezone
from pageGeneration import generateSpecNav, generateDungeonNav, ROLE_FOLDERS
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
)

TEMPLATE_PATH = "templates"

# post_type -> (badge label, badge css class)
POST_TYPE_META = {
    "spec_overview": ("Spec Overview", "bg-gradient-primary"),
    "dungeon_overview": ("Dungeon Overview", "bg-gradient-success"),
    "comp_overview": ("Comp Analysis", "bg-gradient-info"),
    "dungeon_tierlist": ("Tier List", "bg-gradient-warning"),
    "spec_popularity_tierlist": ("Tier List", "bg-gradient-warning"),
    "spec_distribution_by_level": ("Meta Analysis", "bg-gradient-info"),
    "dungeon_popularity_by_level": ("Meta Analysis", "bg-gradient-info"),
    "spec_popularity_vs_performance": ("Meta Analysis", "bg-gradient-info"),
    "highest_run": ("Run Highlight", "bg-gradient-danger"),
    "longest_run": ("Run Highlight", "bg-gradient-danger"),
    "shortest_run": ("Run Highlight", "bg-gradient-danger"),
    "season_countdown": ("Season Countdown", "bg-gradient-primary"),
}
DEFAULT_TYPE_META = ("Data Spotlight", "bg-gradient-secondary")

# Filename prefixes of images created by social_posts/posts.py, used to
# classify entries that predate the post_type field in socials.json.
FILENAME_TYPE_PATTERNS = [
    ("spec_overview_", "spec_overview"),
    ("dungeon_overview_", "dungeon_overview"),
    ("comp_overview_", "comp_overview"),
    ("dungeon_tierlist_", "dungeon_tierlist"),
    ("spec_popularity_tierlist_", "spec_popularity_tierlist"),
    ("spec_distribution_by_level_", "spec_distribution_by_level"),
    ("dungeon_popularity_across_keylevels_", "dungeon_popularity_by_level"),
    ("spec_popularity_vs_performance_", "spec_popularity_vs_performance"),
    ("highest_run_mplus_", "highest_run"),
    ("longest_run_mplus_", "longest_run"),
    ("shortest_run_mplus_", "shortest_run"),
    ("season_countdown_", "season_countdown"),
]

STATIC_TITLES = {
    "comp_overview": "Global Top Comps",
    "dungeon_tierlist": "Dungeon Tier List",
    "spec_popularity_tierlist": "Spec Popularity Tier List",
    "spec_distribution_by_level": "Spec Distribution Across Key Levels",
    "dungeon_popularity_by_level": "Dungeon Popularity Across Key Levels",
    "spec_popularity_vs_performance": "Spec Popularity vs Performance",
}


def derive_post_type(filename):
    for prefix, post_type in FILENAME_TYPE_PATTERNS:
        if filename.startswith(prefix):
            return post_type
    return ""


def spec_display_name(spec_id, spec_lookup, class_lookup):
    spec_meta = spec_lookup.get(str(spec_id), {})
    class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
    if not spec_meta:
        return None
    return f"{spec_meta.get('name', '')} {class_meta.get('name', '')}".strip()


def find_dungeon_meta(dungeon_id, dungeon_lookup):
    if str(dungeon_id) in dungeon_lookup:
        return dungeon_lookup[str(dungeon_id)]
    for v in dungeon_lookup.values():
        if str(v.get("id")) == str(dungeon_id):
            return v
    return None


def derive_title(filename, post_type, spec_lookup, class_lookup, dungeon_lookup):
    """Fallback title for socials.json entries that predate the title field."""
    if post_type == "spec_overview":
        m = re.match(r"spec_overview_(\d+)_", filename)
        name = m and spec_display_name(m.group(1), spec_lookup, class_lookup)
        if name:
            return f"{name} Mythic+ Overview"
    elif post_type == "dungeon_overview":
        m = re.match(r"dungeon_overview_(\d+)_", filename)
        meta = m and find_dungeon_meta(m.group(1), dungeon_lookup)
        if meta:
            return f"{meta['name']['en_US']} Dungeon Overview"
    elif post_type in ("highest_run", "longest_run", "shortest_run"):
        m = re.match(r"(?:highest|longest|shortest)_run_mplus_(\d+)_(\d+)_", filename)
        meta = m and find_dungeon_meta(m.group(1), dungeon_lookup)
        if m and meta:
            kind = post_type.split("_")[0].capitalize()
            return f"{kind} Run: +{m.group(2)} {meta['name']['en_US']}"
    if post_type in STATIC_TITLES:
        return STATIC_TITLES[post_type]
    return "Mythic+ Data Spotlight"


def derive_link(filename, post_type, spec_lookup, class_lookup, dungeon_lookup):
    """Fallback deep link for entries that predate the link field."""
    if post_type == "spec_overview":
        m = re.match(r"spec_overview_(\d+)_", filename)
        spec_meta = m and spec_lookup.get(m.group(1))
        if spec_meta:
            class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
            role = ROLE_FOLDERS.get(str(spec_meta.get("role", 2)), "Dps")
            return f"/classes/{role}/{spec_meta['name']}_{class_meta.get('name', '')}"
    elif post_type == "dungeon_overview":
        m = re.match(r"dungeon_overview_(\d+)_", filename)
        meta = m and find_dungeon_meta(m.group(1), dungeon_lookup)
        if meta and meta.get("slug"):
            return f"/dungeons/{meta['slug']}"
    elif post_type == "comp_overview":
        return "/pages/comps"
    return "/pages/dashboard"


def build_posts(raw_posts, images_dir, spec_lookup, class_lookup, dungeon_lookup):
    posts = []
    # accept a single dir or a list of dirs (first hit wins)
    images_dirs = [images_dir] if isinstance(images_dir, str) else list(images_dir)
    for img_path, entry in raw_posts.items():
        # normalize so keys written on Windows (backslashes) resolve everywhere
        filename = os.path.basename(str(img_path).replace("\\", "/"))
        post_type = entry.get("post_type") or derive_post_type(filename)
        label, badge = POST_TYPE_META.get(post_type, DEFAULT_TYPE_META)

        title = entry.get("title") or derive_title(
            filename, post_type, spec_lookup, class_lookup, dungeon_lookup
        )
        body = entry.get("blog") or entry.get("post") or ""
        # legacy posts end with the bare site URL; drop it for display
        body = re.sub(r"\s*https?://\S+\s*$", "", body).strip()
        paragraphs = [p.strip() for p in re.split(r"\n+", body) if p.strip()]

        image = None
        for d in images_dirs:
            if d and os.path.exists(os.path.join(d, filename)):
                image = filename
                break

        posts.append(
            {
                "title": title,
                "type_label": label,
                "badge_class": badge,
                "paragraphs": paragraphs,
                "image": image,
                "link": entry.get("link")
                or derive_link(
                    filename, post_type, spec_lookup, class_lookup, dungeon_lookup
                ),
                "timestamp": int(entry.get("timestamp", 0)),
            }
        )
    posts.sort(key=lambda p: p["timestamp"], reverse=True)
    return posts


def build_page_links(current, total, window=2):
    """Page numbers to render: first/last plus a window around the current
    page, with None marking an ellipsis gap."""
    wanted = {1, total}
    wanted.update(p for p in range(current - window, current + window + 1) if 1 <= p <= total)
    links = []
    prev = 0
    for p in sorted(wanted):
        if p - prev > 1:
            links.append(None)
        links.append(p)
        prev = p
    return links


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--images_dir",
        default=os.path.join("data", "social"),
        help="Directory holding the persisted social post images",
    )
    parser.add_argument("--output_dir", default="pages")
    parser.add_argument("--per_page", type=int, default=12)
    args = parser.parse_args()

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
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    raw_posts = load_json(os.path.join("data", "socials.json"))

    # look in the requested dir first, then fall back to the local default so
    # running with the CI flag (--images_dir social-images) still finds
    # locally generated images
    default_dir = os.path.join("data", "social")
    images_dirs = [args.images_dir]
    if args.images_dir != default_dir:
        images_dirs.append(default_dir)
    posts = build_posts(
        raw_posts, images_dirs, spec_lookup, class_lookup, dungeon_lookup
    )

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)
    template = env.get_template("blog.html")

    chunks = [
        posts[i : i + args.per_page] for i in range(0, len(posts), args.per_page)
    ] or [[]]
    total_pages = len(chunks)
    os.makedirs(args.output_dir, exist_ok=True)

    for page_num, chunk in enumerate(chunks, start=1):
        out_name = "blog.html" if page_num == 1 else f"blog-{page_num}.html"
        prev_url = None
        if page_num == 2:
            prev_url = "/pages/blog"
        elif page_num > 2:
            prev_url = f"/pages/blog-{page_num - 1}"
        next_url = f"/pages/blog-{page_num + 1}" if page_num < total_pages else None

        output_html = template.render(
            generated_at=datetime.now(timezone.utc).timestamp(),
            spec_nav=spec_nav,
            dungeon_nav=dungeon_nav,
            breadcrumbs=[{"title": "Blog"}],
            active_page="blog",
            notifications=notifications,
            posts=chunk,
            page_num=page_num,
            total_pages=total_pages,
            page_links=build_page_links(page_num, total_pages),
            prev_url=prev_url,
            next_url=next_url,
        )
        out_path = os.path.join(args.output_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(output_html)
        print(f"Generated {out_path}")


if __name__ == "__main__":
    main()
