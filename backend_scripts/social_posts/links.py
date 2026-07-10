"""Site-link builders and small time helpers for social posts."""

from datetime import datetime, timezone
from urllib.parse import quote

from commonUtils import find_dungeon_meta, get_class_lookup, get_spec_lookup
from pageGeneration import ROLE_FOLDERS


def build_site_link(base_url, path=""):
    """Join the site base URL with a repo-relative page path, URL-escaping it."""
    base = (base_url or "https://mythistone.com/").rstrip("/")
    if not path:
        return base + "/"
    return f"{base}/{quote(path)}"


def spec_page_link(base_url, spec_id):
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    spec_meta = spec_lookup.get(str(spec_id), {})
    class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
    if not spec_meta or not class_meta:
        return build_site_link(base_url, "pages/dashboard")
    role = ROLE_FOLDERS.get(str(spec_meta.get("role", 2)), "Dps")
    return build_site_link(
        base_url, f"classes/{role}/{spec_meta['name']}_{class_meta['name']}"
    )


def dungeon_page_link(base_url, dungeon_id):
    meta = find_dungeon_meta(dungeon_id)
    slug = meta.get("slug") if meta else None
    if slug:
        return build_site_link(base_url, f"dungeons/{slug}")
    return build_site_link(base_url, "pages/dashboard")


def time_ago(ms_timestamp: int) -> str:
    # Convert milliseconds timestamp to a datetime in UTC
    dt = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    delta = now - dt

    seconds = int(delta.total_seconds())
    periods = [
        ("year", 60 * 60 * 24 * 365),
        ("month", 60 * 60 * 24 * 30),
        ("day", 60 * 60 * 24),
        ("hour", 60 * 60),
        ("minute", 60),
        ("second", 1),
    ]

    for name, count in periods:
        value = seconds // count
        if value:
            return f"{value} {name}{'s' if value > 1 else ''} ago"
    return "just now"
