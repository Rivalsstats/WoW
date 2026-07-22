"""Shared, dependency-light helpers used across page generators, the
image_generation package and the social_posts package.

Only stdlib + databaseConnector may be imported here; keeping this module
free of jinja2/matplotlib/PIL/openai is what breaks the old circular-import
chains (generateSocialsPost <-> generateSpecPages/generateDashboardPage).
"""

import json
import os

import databaseConnector

LOOKUP_DIR = "data/static"  # Default lookup directory, can be overridden by command line argument

SECONDARY_STATS = ["haste", "versatility", "mastery", "crit"]
TERTIARY_STATS = [
    "avoidance",
    "lifesteal",
    "speed",
]
HEALTH_STATS = ["health", "stamina"]

# Friendly labels for the composite/adaptive stat tokens shown on stat badges
# across the site. The adaptive "mainstat" token (stragiint) and the
# multi-primary combos don't title-case cleanly, so they get explicit names;
# plain secondary stats (crit/haste/...) fall through to a title-cased token.
# Single source shared by the spec page (exposed as a Jinja global) and the
# item preview cards, so the mapping is maintained in exactly one place.
STAT_DISPLAY_NAMES = {
    "stragiint": "Mainstat",
    "stragi": "Str/Agi",
    "agiint": "Agi/Int",
    "strint": "Str/Int",
}


def stat_display_name(stat_type):
    """Display label for a stat token, e.g. 'stragiint' -> 'Mainstat',
    'crit' -> 'Crit'. This is the one conversion used everywhere stat badges are
    rendered (spec page + item preview cards)."""
    if not stat_type:
        return ""
    return STAT_DISPLAY_NAMES.get(stat_type, str(stat_type).title())


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Weapon itemSubClass values that occupy both hands the way inventoryType 17
# (two-hand) does: bows, guns and crossbows are ranged mainhands with no
# off-hand.
TWO_HAND_SUBCLASSES = {2, 3, 18}

# Specs that dual-wield two-handers (Titan's Grip Fury). For these the "2H main
# hand => no off-hand" rule is wrong: they equip a two-hander in BOTH hands, so
# the off-hand must be kept even though the main hand is a 2H. (Single-Minded
# Fury uses one-handers, so its main hand isn't a 2H and the rule never fires.)
DUAL_WIELD_TWOHAND_SPECS = {72}  # Fury Warrior


def occupies_both_hands(item, spec_id=None):
    """Does this main-hand item leave the given spec no off-hand slot?

    Single source of truth for the "2H main hand => drop the OFF_HAND slot" rule
    the spec page's gear overview applies, so the two-hand marks the analyzer
    reads (baked into both spec_meta picks and the item icon shards) can never
    disagree with the slot list they are rendered against.

    ``spec_id`` carries the Titan's Grip exception: a two-hander occupies both
    hands for every spec *except* the ones in DUAL_WIELD_TWOHAND_SPECS, which
    wield one in each hand. Omit it only where the answer is a property of the
    item alone and no spec is in play (the analyzer's spec-independent icon
    shards) — every spec-scoped call must pass it.
    """
    if spec_id is not None and int(spec_id) in DUAL_WIELD_TWOHAND_SPECS:
        return False
    item = item or {}
    if item.get("inventoryType") == 17:
        return True
    # itemSubClass is only a weapon type on itemClass 2 — on armor the same
    # numbers mean leather/mail, which must not be mistaken for a two-hander.
    return item.get("itemClass") == 2 and item.get("itemSubClass") in TWO_HAND_SUBCLASSES


# --- lazily loaded lookup tables --------------------------------------------
# These replace the old import-time loads in generateSocialsPost.py so that
# importing any module is side-effect free; the JSON is read on first use.

_lookup_cache = {}


def _get_lookup(name):
    if name not in _lookup_cache:
        _lookup_cache[name] = load_json(os.path.join(LOOKUP_DIR, f"{name}.json"))
    return _lookup_cache[name]


def get_spec_lookup():
    return _get_lookup("specs")


def get_class_lookup():
    return _get_lookup("classes")


def get_dungeon_lookup():
    return _get_lookup("dungeons")


def find_dungeon_meta(dungeon_id):
    dungeon_lookup = get_dungeon_lookup()
    if isinstance(dungeon_lookup, dict):
        if str(dungeon_id) in dungeon_lookup:
            return dungeon_lookup[str(dungeon_id)]
        for v in dungeon_lookup.values():
            if str(v.get("id")) == str(dungeon_id):
                return v
    elif isinstance(dungeon_lookup, list):
        for d in dungeon_lookup:
            if str(d.get("id")) == str(dungeon_id):
                return d
    return None


def sort_spec_ids_by_role(spec_ids, spec_lookup):
    """Sort spec-id strings by role (tank, healer, dps), then numerically.
    Unknown ids sort last."""
    return sorted(
        spec_ids,
        key=lambda sid: (
            int(spec_lookup[sid]["role"]) if sid in spec_lookup else 99,
            int(sid),
        ),
    )


def format_comp_names(comp_str):
    """Turn a comma-separated spec-id comp string into 'Spec Class, ...' ordered
    by role (tank, healer, dps). Unknown ids are skipped."""
    if not comp_str:
        return ""
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    ids = [s for s in str(comp_str).split(",") if s]
    ids = sorted(
        ids,
        key=lambda sid: (
            int(spec_lookup[sid]["role"]) if sid in spec_lookup else 99,
            int(sid) if sid.isdigit() else 0,
        ),
    )
    names = []
    for sid in ids:
        if sid in spec_lookup:
            sm = spec_lookup[sid]
            cm = class_lookup.get(str(sm.get("classID", "")), {})
            names.append(f"{sm.get('name', '')} {cm.get('name', '')}".strip())
    return ", ".join(names)


# formatters
def upgrade_info(duration, upgrade_map, keystone_level):
    """
    Given:
      - duration: an integer (ms) or something castable to int
      - upgrade_map: a dict whose values are dicts with
          { 'upgrade_level': int, 'qualifying_duration': int }
      - keystone_level: int or str (or None)
    Returns:
      A dict with:
        - text: the '+…' or '-' prefix joined to keystone_level
        - css:  the bootstrap class to use ('text-success' or 'text-danger')
    """
    try:
        dur = int(duration)
    except (TypeError, ValueError):
        # fallback to no upgrade
        return {"text": f"-{keystone_level or ''}", "css": "text-danger"}

    # sort descending by upgrade_level
    levels = sorted(
        upgrade_map.values(), key=lambda e: e["upgrade_level"], reverse=True
    )

    achieved = 0
    for lvl in levels:
        if dur <= lvl["qualifying_duration"]:
            achieved = lvl["upgrade_level"]
            break

    if achieved > 0:
        prefix, css = "+" * achieved, "text-success"
    else:
        prefix, css = "-", "text-danger"

    return {"text": f"{prefix}{keystone_level or ''}", "css": css}


def humanize_number(value):
    """
    Turn 123 → '123', 1500 → '1.5k', 500000 → '500k', 3000000 → '3m', etc.
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        return value

    if n >= 1_000_000:
        x = n / 1_000_000.0
        # one decimal, strip trailing .0
        s = f"{x:.1f}".rstrip("0").rstrip(".")
        return f"{s} M"
    if n >= 1_000:
        x = n / 1_000.0
        s = f"{x:.1f}".rstrip("0").rstrip(".")
        return f"{s} K"
    return str(n)


def format_duration(ms):
    """
    Turn a millisecond count into:
      - "MM:SS.mmm" if under an hour
      - "HH:MM:SS.mmm" if one hour or more

    Examples:
      34567    → "00:34.567"
      1234567  → "20:34.567"
      3661000  → "01:01:01.000"
    """
    try:
        total_ms = int(ms)
    except (TypeError, ValueError):
        return ms

    # Break into components
    total_seconds = total_ms // 1000
    milliseconds = total_ms % 1000

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # Zero‑pad each piece
    hh = f"{hours:02d}"
    mm = f"{minutes:02d}"
    ss = f"{seconds:02d}"
    mmm = f"{milliseconds:03d}"

    # Build the string
    base = f"{mm}:{ss}.{mmm}"
    if hours > 0:
        return f"{hh}:{base}"
    return base


def fetch_stat_info(conn, cursor, spec_id, current_season_id, spec_lookup):
    stats = databaseConnector.fetch_stats(conn, cursor, spec_id, current_season_id)
    stat_priority = []
    tertiary_priority = []
    health_priority = []
    for stat, value in stats.items():
        if stat == "mainstat":
            value["name"] = spec_lookup[spec_id].get("primary_stat")
            stat_priority.append(value)
        elif stat in SECONDARY_STATS:
            value["name"] = stat
            stat_priority.append(value)
        elif stat in TERTIARY_STATS:
            value["name"] = stat
            tertiary_priority.append(value)
        elif stat in HEALTH_STATS:
            value["name"] = stat
            health_priority.append(value)
    return stat_priority, tertiary_priority, health_priority
