"""Generate the client-side "Am I meta?" analyzer page.

The page is fully static: analyzer.js parses a pasted SimulationCraft addon
export in the browser, resolves the spec, fetches the small per-spec meta JSON
that generateSpecPages.py bakes to /assets/json/spec_meta/<spec_id>.json, and
renders a slot-by-slot gap report. This generator bakes the lookup tables the JS needs to turn a SimC export's
class/spec tokens into a spec_id, plus the sharded item icon index it falls back
to for gear outside the site's popular-items manifest — no DB access required.
"""
import os
import glob
import json
import shutil
import argparse
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from pageGeneration import generateSpecNav, generateDungeonNav, build_global_trends
from commonUtils import LOOKUP_DIR, load_json, load_season_info, occupies_both_hands

TEMPLATE_PATH = "templates"

# Per-spec talent-tree geometry (fullNodeOrder + positioned nodes) the analyzer
# decodes and draws its build tree from. Baked here, credential-free, straight
# from processTalents' output so the tree stays in step with the game data
# without waiting on the DB-driven spec_meta rebuild (which only carries the
# per-hero-tree meta loadout strings). See write_talent_trees.
TALENT_SRC_DIR = os.path.join("data", "static", "talents")
TALENT_TREE_DIR = os.path.join("assets", "json", "talent_trees")

# Item ids per icon-index shard. analyzer.js only ever needs the handful of
# buckets its equipped ids land in, so a small bucket keeps the fetch tiny; 1000
# gives ~276 files of ~17 KB each for the current catalog.
ICON_SHARD_SIZE = 1000
ICON_SHARD_DIR = os.path.join("assets", "json", "item_icons")

# Client copy of the bonus id -> item quality table (see write_bonus_quality_map).
BONUS_QUALITY_OUT = os.path.join("assets", "json", "bonus_quality.json")

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


def write_item_icon_shards():
    """Bake the id -> [icon, quality(, 1)] fallback lookup analyzer.js draws the
    player's own gear with, sharded by ``id // ICON_SHARD_SIZE``. The optional
    third element marks a weapon that occupies both hands, which is how the
    client knows a suggested two-hander replaces a one-hand + off-hand pair.

    No item name here on purpose: the report's rows are icons only, so a name
    would be ~3 MB of catalog nobody reads. Hover gets it from the Wowhead
    tooltip instead.

    ``items_index.json`` only covers the ~500 items that have an /items page, so
    anything else a player wears (PvP gear, raid/leveling drops) used to render a
    questionmark. The full catalog is 109k items — far too big to ship as one
    blob — but the client knows its equipped ids before it needs an icon, so it
    fetches only the buckets those ids fall in.

    Returns the sorted list of bucket keys written, which gets baked into the
    page so the client never has to guess whether a 404 means "empty id range"
    or "broken deploy".
    """
    items = load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
    buckets = {}
    for item in items:
        icon = item.get("icon")
        if not icon:
            continue  # a handful of catalog rows carry no icon at all
        item_id = item["id"]
        entry = [icon, item.get("quality")]
        # Shards are keyed by item id alone, so this mark is the item-level
        # answer with no spec_id: "this weapon is a two-hander". The Titan's Grip
        # exception is applied on the client instead — a dual-wielding spec keeps
        # its OFF_HAND meta slot, and analyzer.js only consults the mark for a
        # spec that has none.
        if occupies_both_hands(item):
            entry.append(1)
        buckets.setdefault(item_id // ICON_SHARD_SIZE, {})[str(item_id)] = entry

    # Rebuild from scratch so a shrinking catalog can't leave stale shards behind.
    shutil.rmtree(ICON_SHARD_DIR, ignore_errors=True)
    os.makedirs(ICON_SHARD_DIR, exist_ok=True)
    for key, entries in buckets.items():
        with open(os.path.join(ICON_SHARD_DIR, f"{key}.json"), "w", encoding="utf-8") as f:
            json.dump(entries, f, separators=(",", ":"), ensure_ascii=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"wrote {len(buckets)} item icon shards ({sum(len(v) for v in buckets.values())} items)"
    )
    return sorted(buckets)


def write_bonus_quality_map():
    """Bake the bonus id -> quality table analyzer.js needs to rim the player's
    own gear the way the spec pages rim the meta picks.

    A Mythic+ drop is a *rare* item in the catalog that a quality bonus id
    promotes to epic. generateSpecPages.py already applies this table to the meta
    picks (convert_slots -> quality_override), so without the client half the
    analyzer draws a blue rim on the very item it draws purple one tile right.

    data/static/bonus_quality_map.json is committed (processBonusIds.py builds it
    from bonuses.json) but data/static is never deployed — buildPages.yml ships
    only data/icons and data/creature_img — so re-emit it minified under
    assets/json, which the build artifact does carry.
    """
    quality_map = load_json(os.path.join(LOOKUP_DIR, "bonus_quality_map.json"))
    os.makedirs(os.path.dirname(BONUS_QUALITY_OUT), exist_ok=True)
    with open(BONUS_QUALITY_OUT, "w", encoding="utf-8") as f:
        json.dump(quality_map, f, separators=(",", ":"), ensure_ascii=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"wrote bonus quality map ({len(quality_map)} bonus ids)"
    )


def write_talent_trees():
    """Bake the per-spec talent-tree geometry analyzer.js positions its build on,
    from processTalents' ``data/static/talents/<spec>.json`` files.

    Only ``fullNodeOrder`` + ``nodes`` (id -> {name, icon/spellId per entry, x, y,
    next, type, maxRanks, subTreeId, g group, free}) + the hero-tree ``subTrees``
    name map go out — the fields the client needs to lay out and label the tree.
    The meta *loadout strings* to compare against stay in spec_meta (they need the
    DB). data/static isn't deployed, so re-emit under assets/json, which the build
    artifact carries. Rebuilt from scratch so a dropped spec can't leave a stale
    tree behind."""
    files = sorted(glob.glob(os.path.join(TALENT_SRC_DIR, "*.json")))
    if not files:
        raise FileNotFoundError(
            f"No per-spec talent files in {TALENT_SRC_DIR}; run processTalents.py first."
        )
    shutil.rmtree(TALENT_TREE_DIR, ignore_errors=True)
    os.makedirs(TALENT_TREE_DIR, exist_ok=True)
    written = 0
    for path in files:
        spec_id = os.path.splitext(os.path.basename(path))[0]
        data = load_json(path)
        if not data.get("fullNodeOrder") or not data.get("nodes"):
            # A spec whose talent file predates the tree fields — skip rather than
            # ship an un-layoutable tree (the client falls back to a flat list).
            continue
        tree = {
            "fullNodeOrder": data["fullNodeOrder"],
            "nodes": data["nodes"],
            "subTrees": data.get("subTrees", {}),
        }
        with open(os.path.join(TALENT_TREE_DIR, f"{spec_id}.json"), "w", encoding="utf-8") as f:
            json.dump(tree, f, separators=(",", ":"), ensure_ascii=False)
        written += 1
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] wrote {written} talent tree files"
    )


def maybe_init_db_pool():
    """The analyzer page itself needs no DB, but the shared trends bar does
    (build_global_trends opens a pooled connection). The other page generators
    init the pool from DATABASE_* env vars; do the same best-effort here so the
    bar shows in the real build, and hide it (as before) when creds are absent."""
    if not (
        os.environ.get("DATABASE_HOST")
        and os.environ.get("DATABASE_USER")
        and os.environ.get("DATABASE_PASSWORD")
    ):
        return
    try:
        databaseConnector.init_connection_pool(
            os.environ.get("DATABASE_HOST"),
            os.environ.get("DATABASE_USER"),
            os.environ.get("DATABASE_PASSWORD"),
            os.environ.get("DATABASE_NAME", "Mythistone"),
            os.environ.get("DATABASE_PORT", "3306"),
            1,
        )
    except Exception as exc:  # pragma: no cover - trends just stay hidden
        print(f"[analyzer] DB pool init failed ({exc}); trends bar will hide.")


def main(template_path, output_dir):
    maybe_init_db_pool()
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    season_info = load_season_info(LOOKUP_DIR)

    spec_index, spec_display = build_spec_index(spec_lookup, class_lookup)
    item_icon_buckets = write_item_icon_shards()
    write_bonus_quality_map()
    write_talent_trees()

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path) or TEMPLATE_PATH),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        trends=build_global_trends(),
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
        item_icon_buckets=item_icon_buckets,
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
