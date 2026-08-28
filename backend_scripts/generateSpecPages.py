import os
import json
import argparse
import traceback
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
import compArchetypes
import aggregateData
import commonUtils
from collections import defaultdict
from datetime import datetime, timezone
from contextlib import closing
import re
from urllib.parse import quote_plus
from pageGeneration import (
    ROLE_FOLDERS,
    generateSpecNav,
    generateDungeonNav,
    build_item_slug_map,
    build_item_source_map,
    build_trends,
    trend_feeds_for_spec,
)
# Re-exported for the many modules that import these from generateSpecPages;
# the implementations live in commonUtils so image_generation/social_posts can
# use them without importing this (jinja2-heavy) module.
from commonUtils import (
    LOOKUP_DIR,
    SECONDARY_STATS,
    TERTIARY_STATS,
    HEALTH_STATS,
    load_json,
    load_season_info,
    occupies_both_hands,
    upgrade_info,
    humanize_number,
    format_duration,
    fetch_stat_info,
    stat_display_name,
    # Enchant slot resolution now lives in commonUtils so the spec page and the
    # item page share one implementation. Re-exported here for the modules that
    # import these names from generateSpecPages.
    ENCHANT_CLASS_WEAPON,
    ENCHANT_CLASS_ARMOR,
    ENCHANT_CLASS_PROFESSION_TOOL,
    INVTYPE_DISPLAY_ORDER,
    NON_GEAR_DISPLAY_ORDER,
    enchant_slot_pos,
)

LEFT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST"]
RIGHT_ORDER = ["HANDS", "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2"]

WEAPON_SLOTS = ["MAIN_HAND", "OFF_HAND"]

TRINKET_SLOTS = ["TRINKET_1", "TRINKET_2"]

MULTI_SLOT_GROUPS = {
    "TRINKET_1": "TRINKET",
    "TRINKET_2": "TRINKET",
    "FINGER_1": "FINGER",
    "FINGER_2": "FINGER",
}

# Gear-list noise filter: an entry must hold at least this share of the spec's
# tracked runs for the slot (summed over every item in the slot, not just the
# shown ones) to be rendered; the first GEAR_LIST_MIN_KEEP entries always stay
# so sparse early-season slots still show a list. The threshold scales with the
# data volume, so a 1-run old legendary is dropped from a 10k-run slot while an
# early-season 50-run slot keeps its handful of picks. generateItemPages passes
# these same values to fetch_spec_page_linked_items so an item page exists for
# exactly the entries that survive this filter.
GEAR_LIST_MIN_SLOT_SHARE = 1.0  # % of the spec's slot runs
GEAR_LIST_MIN_KEEP = 3

# Talent "TOP" highlight thresholds (elite-vs-popular divergence). A talent node
# is flagged as an elite pick when the top-50 verified players take it far more
# often than the general Mythic+ population. Tuned by eye on a real spec; kept
# here so the badge density is a one-line change.
TALENT_ELITE_MIN_PCT = 50.0      # top-50 usage must be at least this high
TALENT_DIVERGENCE_DELTA = 20.0   # ...and exceed general popularity by this many points

# Minimum top-50 usage share (%) for an item/gem/enchant/missive/embellishment or
# a combo to earn the gold "TOP" badge. Shared by the gear-slot BIS annotations
# and the combo/detail sections so every TOP badge on the page means the same
# thing: the top-50 players run it in more than this share of their loadouts.
BIS_PCT_THRESHOLD = 80.0

# Keystone window used by the collector when deciding which runs contribute gear
# and talent data: for each dungeon it takes the current rank-1 key level and
# every run down to KEY_LEVEL_WINDOW levels below it (so KEY_LEVEL_WINDOW + 1 key
# levels in total). Mirrors collectLeaderboardData.KEYLEVELS_DOWN — keep the two
# in sync; it is duplicated here rather than imported so this generator doesn't
# pull in the async collector module. Surfaced to the spec page so the gear
# overview copy states the real cutoff instead of a hard-coded example.
KEY_LEVEL_WINDOW = 5

# Talent Differences modal (per-dungeon talent swaps). It reads ONLY the top-50
# verified loadouts: each of those is a complete build tied to one dungeon, so a
# talent's per-dungeon share is a true adoption rate. The general-population
# aggregation is sampled per run and its per-dungeon slices are too noisy to
# tell a real dungeon swap from collection noise, which is why it is not mixed
# in here.
#
# Everything below is a noise gate. The modal shows a dungeon's swaps only when
# they clear these, and renders nothing at all rather than an empty shell: a
# heading over "no data" is worse than no heading.
TALENT_DIFF_MIN_DUNGEON_LOADOUTS = 5   # loadouts before a dungeon is listed
TALENT_DIFF_MIN_PCT_POINTS = 10.0      # a talent must move this much to show
TALENT_DIFF_TOP_N = 4                  # rows per side (take / drop)
# Called out as an outright recommendation only where the adoption rate decides
# it on its own: most of the dungeon's top loadouts take it, or almost none do.
TALENT_DIFF_RECOMMEND_MIN_PCT = 50.0
TALENT_DIFF_DROP_MAX_PCT = 20.0
# Hero-tree preference shifts per dungeon: a couple of tenths of a percent is
# not a preference, so only shifts of this many points are worth a row.
HERO_TREE_DIFF_MIN_PCT_POINTS = 5.0

# Enchant slot groups in gear-overview order (LEFT_ORDER + RIGHT_ORDER, then
# weapons and trinkets), which is the order the Enchantment Details accordion
# renders: fetch_enchant_info builds `enchant_slots` by iterating this list and
# spec_page.html walks that dict's insertion order.
SLOT_GROUPS = [
    "HEAD",
    "NECK",
    "SHOULDER",
    "BACK",
    "CHEST",
    "WRIST",
    "HANDS",
    "WAIST",
    "LEGS",
    "FEET",
    "FINGER",
    "WEAPON",
    "TRINKET",
]

BLIZZARD_STAT_MAP = {
    32: "crit",
    36: "haste",
    40: "versatility",
    49: "mastery",
    61: "speed",
    62: "leech",
    63: "avoidance",
}

def format_utc_timestamp(ms):
    """
    Convert a UTC timestamp in milliseconds (e.g. 1750986462000)
    into a string like "DD/MM/YYYY, HH:MM:SS".
    """

    dt = datetime.fromtimestamp(int(ms), timezone.utc)
    return dt.strftime("%d/%m/%Y, %H:%M:%S")


def format_iso_timestamp(ts):
    """
    Convert a UTC timestamp in seconds into an ISO 8601 string
    (e.g. "2026-06-28T11:16:37Z") for use in JSON-LD date fields.
    """

    dt = datetime.fromtimestamp(int(ts), timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_buyout(buyout):
    if buyout is None:
        return "N/A"
    total = int(buyout)
    gold = total // 10_000
    silver = (total % 10_000) // 100
    copper = total % 100

    # Big abbreviated display for ≥ 1 000 gold
    if gold >= 1_000:
        if gold < 10_000:
            abbrev = f"{gold:.0f}"
        elif gold < 1_000_000:
            abbrev = f"{gold / 1_000:.0f}k"
        else:
            abbrev = f"{gold / 1_000_000:.2f}M"
        return (
            f'<span class="buyout-abbrev">{abbrev} '
            '<img src="/data/icons/gold_coin.png" '
            'alt="Gold" style="width:16px;vertical-align:middle;"></span>'
        )

    parts = []
    if gold > 0:
        parts.append(
            f'<span class="buyout-gold">{gold} '
            '<img src="/data/icons/gold_coin.png" '
            'alt="Gold" style="width:16px;vertical-align:middle;"></span>'
        )
    if silver > 0 and gold < 100:
        parts.append(
            f'<span class="buyout-silver">{silver} '
            '<img src="/data/icons/silver_coin.png" '
            'alt="Silver" style="width:16px;vertical-align:middle;"></span>'
        )
    if copper > 0 and silver < 100 and gold < 1:
        parts.append(
            f'<span class="buyout-copper">{copper} '
            '<img src="/data/icons/copper_coin.png" '
            'alt="Copper" style="width:16px;vertical-align:middle;"></span>'
        )

    return " ".join(parts) or "0 <small>c</small>"


# helpers

def node_has_valid_spellid(node):
    entries = node.get("entries", [])
    # For choice/tiered nodes, at least one entry must have a nonzero spellId
    for e in entries:
        if e.get("spellId", 0):
            return True
    return False


def strip_empty_talent_entries(talents_tree_data):
    """Drop empty/identity-less entry objects from every talent node.

    The vendored raidbots talents.json pads some single nodes with a stray
    `{}` entry (node name ends in " / "). Left in place, len(entries) > 1
    makes build_ui_tree misdetect the node as a choice node. Strip them so
    the node keeps its true type. Warns per dropped entry so upstream data
    changes stay visible (fail-loudly).
    """
    NODE_KEYS = ("classNodes", "specNodes", "heroNodes")
    dropped = 0
    for spec in talents_tree_data:
        spec_id = spec.get("specId")
        for key in NODE_KEYS:
            for node in spec.get(key, []):
                entries = node.get("entries")
                if not entries:
                    continue
                # keep only entries carrying an identity; drops `{}`
                clean = [e for e in entries
                         if e.get("id") or e.get("definitionId") or e.get("spellId")]
                if len(clean) != len(entries):
                    n_dropped = len(entries) - len(clean)
                    dropped += n_dropped
                    print(f"[talents] WARN spec {spec_id} node {node.get('id')} "
                          f"'{node.get('name','')}' dropped {n_dropped} empty "
                          f"entry object(s)")
                    node["entries"] = clean
    if dropped:
        print(f"[talents] WARN stripped {dropped} empty entry object(s) total "
              f"from talents.json")
    return talents_tree_data

def build_ui_tree(nodes, pop_data, is_hero=False, pop_hero_tree_id=None, top_pct_map=None, top_entry_pct_map=None):

    if not nodes:
        return {"nodes": [], "edges": []}

    if is_hero and pop_hero_tree_id is not None:
        nodes = [n for n in nodes if n.get("subTreeId") == pop_hero_tree_id]

    # Filter out nodes with no valid spellId in any entry
    nodes = [n for n in nodes if node_has_valid_spellid(n)]

    pop_map = {}
    pop_avg_ranks = {}
    pop_count_map = {}
    total_data_count = 0
    if isinstance(pop_data, dict):
        total_data_count = pop_data.get("data_count", 0)
        for t in pop_data.get("overall_dungeon_talents", []):
            pop_map[int(t["id"])] = float(t.get("pct", 0.0))
            pop_avg_ranks[int(t["id"])] = float(t.get("avg_rank", 1.0))
            pop_count_map[int(t["id"])] = int(t.get("count", 0))

    # Top-50 verified-player usage per node (node_id -> pct). Used to flag nodes
    # where the elite build diverges from the general population.
    top_pct_lookup = {}
    if isinstance(top_pct_map, dict):
        for nid, info in top_pct_map.items():
            try:
                top_pct_lookup[int(nid)] = float(info.get("pct", 0.0))
            except (TypeError, ValueError, AttributeError):
                continue

    # Per-choice top-50 usage (node_id -> list of {entry_id, spell_id, pct}).
    # Empty for rows collected before entry ids were stored; every consumer
    # below must degrade to node-level behaviour when a node is missing here.
    top_entry_lookup = {}
    if isinstance(top_entry_pct_map, dict):
        for nid, entries_info in top_entry_pct_map.items():
            try:
                top_entry_lookup[int(nid)] = list(entries_info or [])
            except (TypeError, ValueError):
                continue

    if not nodes:
        return {"nodes": [], "edges": []}

    min_x = min((n.get("posX", 0) for n in nodes), default=0)
    max_x = max((n.get("posX", 0) for n in nodes), default=0)
    min_y = min((n.get("posY", 0) for n in nodes), default=0)
    max_y = max((n.get("posY", 0) for n in nodes), default=0)
    
    # Padding
    min_x -= 150
    max_x += 150
    min_y -= 150
    max_y += 150
    
    w = max_x - min_x
    h = max_y - min_y
    if w <= 0: w = 1
    if h <= 0: h = 1

    node_map = {n["id"]: n for n in nodes if "id" in n}
    ui_nodes = []
    
    for n in nodes:
        if "id" not in n: continue
        
        pct = pop_map.get(n["id"], 0.0)
        count = pop_count_map.get(n["id"], 0)
        
        entries = n.get("entries", [])
        node_choices = []
        total_entry_pct = 0.0
        node_top_entries = top_entry_lookup.get(n["id"], [])

        for e in entries:
            e_pct = pop_map.get(e.get("definitionId"), 0.0)
            e_count = pop_count_map.get(e.get("definitionId"), 0)
            if e_pct == 0.0 and e.get("id"):
                e_pct = pop_map.get(e["id"], 0.0)
                e_count = pop_count_map.get(e["id"], 0)
            if e_pct == 0.0 and e.get("spellId"):
                e_pct = pop_map.get(e["spellId"], 0.0)
                e_count = pop_count_map.get(e["spellId"], 0)

            # Top-50 usage of this specific choice, matched by entry id or
            # spell id (whichever the collector managed to capture).
            e_top_pct = 0.0
            for te in node_top_entries:
                if (te.get("entry_id") is not None and te.get("entry_id") == e.get("id")) or (
                    te.get("spell_id") is not None and te.get("spell_id") == e.get("spellId")
                ):
                    e_top_pct = float(te.get("pct", 0.0))
                    break

            e_pct_capped = min(e_pct, 100.0)
            e_top_pct = min(e_top_pct, 100.0)

            total_entry_pct += e_pct
            node_choices.append({
                "name": e.get("name", ""),
                "icon": e.get("icon", ""),
                # Raw overall usage; for choice nodes these get normalized below
                # to the node-conditional split (see the normalization block).
                "pct": e_pct_capped,
                "count": e_count,
                "spellId": e.get("spellId", 0),
                "maxRanks": e.get("maxRanks", 1),
                "top_pct": e_top_pct,
                "is_top": False,
            })
            
        if entries and pct == 0.0:
            pct = total_entry_pct
            count = sum(c["count"] for c in node_choices)
            
        pct = min(pct, 100.0)

        if n.get("freeNode") is True:
            pct = 100.0
            count = total_data_count

        n_type = n.get("type", "passive")
        max_ranks = n.get("maxRanks", 1)
        
        # if n_type == "tiered":
        #     n_type = "passive"

        if n_type != "tiered" and len(entries) > 1:
            n_type = "choice"
        elif n_type != "tiered" and entries:
            n_type = entries[0].get("type", n_type)

        if is_hero and n_type != "choice":
            continue

        # Choice-node percentages are shown *conditional on the node being taken*.
        # Raw usage is measured against the whole population, so a node taken 50%
        # of the time with picks split 40%/10% understates the actual preference;
        # normalized against the node total it reads 80%/20%. Do this for both the
        # general population and the top-50 split so the tooltip and the divergence
        # flag compare like with like. Passive/tiered nodes keep raw values.
        if n_type == "choice" and node_choices:
            total_gen_pct = sum(c["pct"] for c in node_choices)
            total_top_pct = sum(c["top_pct"] for c in node_choices)
            for c in node_choices:
                c["pct"] = (c["pct"] / total_gen_pct * 100.0) if total_gen_pct > 0 else 0.0
                c["top_pct"] = (c["top_pct"] / total_top_pct * 100.0) if total_top_pct > 0 else 0.0
                c["is_top"] = (
                    c["top_pct"] >= TALENT_ELITE_MIN_PCT
                    and (c["top_pct"] - c["pct"]) >= TALENT_DIVERGENCE_DELTA
                )

        icon = "inv_misc_questionmark"
        spell_id = 0

        if n_type == "choice" and len(node_choices) > 0:
            best_choice = max(node_choices, key=lambda x: x["pct"])
            icon = best_choice["icon"] or "inv_misc_questionmark"
            spell_id = best_choice["spellId"]
        elif entries:
            icon = entries[0].get("icon", "inv_misc_questionmark")
            spell_id = entries[0].get("spellId", 0)

        if n_type == "choice" and count == 0:
            count = sum(c["count"] for c in node_choices)

        avg_rank = pop_avg_ranks.get(n["id"], 0.0)
        
        # If no avg_rank is explicitly mapped, look for it in the entries
        if avg_rank == 0.0 and entries:
            # Gather valid mapped entries
            valid_e_ranks = [
                pop_avg_ranks.get(e.get("definitionId"), 0.0) or 
                pop_avg_ranks.get(e.get("id"), 0.0) or 
                pop_avg_ranks.get(e.get("spellId"), 0.0) 
                for e in entries
            ]
            valid_e_ranks = [r for r in valid_e_ranks if r > 0.0]
            if valid_e_ranks:
                if n_type == "tiered":
                    avg_rank = max(valid_e_ranks)
                else:
                    avg_rank = sum(valid_e_ranks) / len(valid_e_ranks)

        if avg_rank == 0.0:
            avg_rank = float(max_ranks)

        # Elite-vs-popular divergence: flag nodes the top-50 verified players
        # take far more often than the general population. Free nodes (forced
        # to 100%) can never diverge, so they never light up.
        top_pct_val = top_pct_lookup.get(n["id"], 0.0)
        is_top = (
            not n.get("freeNode", False)
            and top_pct_val >= TALENT_ELITE_MIN_PCT
            and (top_pct_val - pct) >= TALENT_DIVERGENCE_DELTA
        )

        # Choice nodes: node-level usage is ~100% for both populations, so the rule
        # above (almost) never fires — leave whatever it decided as the node-level
        # TOP badge. A lone diverging INNER choice must NOT light the node-level badge
        # (that is what the per-choice tt-choice-badge is for); we only record which
        # choice the elite favour so the node tooltip can name it IF the node itself
        # legitimately earned the badge. Free choice nodes: the node is forced but the
        # pick isn't, and the per-choice badges still mark the divergent pick.
        top_choice = None
        if n_type == "choice":
            diverging = [c for c in node_choices if c.get("is_top")]
            if diverging:
                top_choice = max(diverging, key=lambda c: c["top_pct"])

        ui_nodes.append({
            "id": n["id"],
            "left": (n.get("posX", 0) - min_x) / w * 100,
            "top": (n.get("posY", 0) - min_y) / h * 100,
            "pct": "{:.1f}".format(pct),
            "pct_val": pct,
            "top_pct": "{:.1f}".format(top_pct_val),
            "top_pct_val": top_pct_val,
            "is_top": is_top,
            "top_choice_name": top_choice["name"] if top_choice else None,
            "top_choice_pct_val": top_choice["pct"] if top_choice else None,
            "count": count,
            "total_count": total_data_count,
            "icon": icon,
            "spellId": spell_id,
            "type": n_type,
            "maxRanks": max_ranks,
            "avgRank": avg_rank,
            "isFreeNode": n.get("freeNode", False),
            "choices": sorted(node_choices, key=lambda x: x["pct"], reverse=True) if n_type == "choice" else (node_choices if n_type == "tiered" else [])
        })

    ui_edges = []
    if not is_hero:
        for n in nodes:
            if "id" not in n: continue
            
            start_x = (n.get("posX", 0) - min_x) / w * 100
            start_y = (n.get("posY", 0) - min_y) / h * 100
            
            start_pct = pop_map.get(n["id"], 0.0)
            if start_pct == 0.0 and n.get("entries"):
                start_pct = sum([pop_map.get(e.get("definitionId"), 0.0) or pop_map.get(e.get("id"), 0.0) or pop_map.get(e.get("spellId"), 0.0) for e in n["entries"]])
            
            for child_id in n.get("next", []):
                child = node_map.get(child_id)
                if not child: continue
                
                child_pct = pop_map.get(child_id, 0.0)
                if child_pct == 0.0 and child.get("entries"):
                    child_pct = sum([pop_map.get(e.get("definitionId"), 0.0) or pop_map.get(e.get("id"), 0.0) or pop_map.get(e.get("spellId"), 0.0) for e in child["entries"]])
                    
                end_x = (child.get("posX", 0) - min_x) / w * 100
                end_y = (child.get("posY", 0) - min_y) / h * 100
                
                is_active = (start_pct >= 1.0 and child_pct >= 1.0)
                
                ui_edges.append({
                    "x1": start_x, "y1": start_y,
                    "x2": end_x, "y2": end_y,
                    "active": is_active
                })

    return {"nodes": ui_nodes, "edges": ui_edges}


def escape_raidbot_code(code):
    """ """
    loadout = {}
    if not code:
        return
    loadout["original"] = code
    loadout["code"] = quote_plus(code, safe="")
    return loadout


def normalize_slot_collections(list_of_lists, slot_names):
    """
    Convert list-of-lists (raw items) into template-friendly slot dicts:
      [ { "slot": slot_names[i], "slug": slot_slug, "entries": [ {id, count, bonus:{list, count}, slot_slug, ...}, ... ] }, ... ]
    - slot_names must be the same order/length or longer than list_of_lists (we allow shorter; missing names get fallback "<idx>").
    - We try to convert item IDs to int for item_lookup compatibility.
    - bonus.ids (comma string) -> bonus.list (list of strings).
    """
    normalized = []
    for i, raw_entries in enumerate(list_of_lists):
        # preserve original slot name when available (keeps HEAD/NECK/... exactly as in LEFT_ORDER)
        slot_name = slot_names[i] if i < len(slot_names) else f"slot {i}"
        slot_slug = slot_name.replace(" ", "")

        entries = []
        total_count = 0
        for e in raw_entries:
            raw_item = e.get("item")
            # try to convert to int for item_lookup; fall back to original
            try:
                entry_id = int(raw_item) if raw_item is not None else None
            except (TypeError, ValueError):
                entry_id = raw_item

            # normalize bonus.ids -> bonus.list (list of strings)
            bonus_raw = e.get("bonus") or {}
            ids = bonus_raw.get("ids", "")
            if isinstance(ids, str):
                bonus_list = [s.strip() for s in ids.split(",") if s.strip()]
            elif isinstance(ids, (list, tuple)):
                bonus_list = [str(x) for x in ids]
            else:
                bonus_list = []

            bonus_count = bonus_raw.get("count")

            entry = {
                "id": entry_id,
                "count": e.get("count", 0),
                "bonus": {"list": bonus_list, "count": bonus_count},
                "socket_count": e.get("socket_count", 0.0),
                # optional passthroughs (keep them if present)
                "enchantment": e.get("enchantment"),
                "socket": e.get("socket"),
                "pcs": e.get("pcs"),
                "embellishment": e.get("embellishment"),
                "missive": e.get("missive"),
                "max_timed_key": e.get("max_timed_key", 0),
                "max_depleted_key": e.get("max_depleted_key", 0),
                # BIS passthroughs
                "is_bis": e.get("is_bis", False),
                "bis_pct": e.get("bis_pct"),
                "bis_count": e.get("bis_count"),
                "bis_rank": e.get("bis_rank"),
                # SimulationCraft BiS passthroughs
                "is_simc_bis": e.get("is_simc_bis", False),
                "simc_dps_pct": e.get("simc_dps_pct"),
                "simc_rank": e.get("simc_rank"),
                "quality_override": e.get("quality_override"),
                "crafted_stats": e.get("crafted_stats"),
                "slot_slug": slot_slug,
            }
            total_count += e.get("count", 0)
            entries.append(entry)

        normalized.append(
            {
                "slot": slot_name,
                "slug": slot_slug,
                "entries": entries,
                "slot_count": total_count,
            }
        )
    return normalized




# Start-of-list sentinel for the set-run split below: itemSetId is legitimately
# None for setless items, so None can't double as "no previous group".
_UNSET = object()


def build_comps(
    raw_rows, threshold, item_lookup, comp_kind, spec_id, limit=10, slot_sorted=True
):
    """Filter rare comps and parse the DB comp strings into display id lists.

    Each comp carries ``ids`` (the flat display order) and ``groups`` (the same
    ids split into their item-set runs, in the same order). The template draws a
    divider between groups so a set combo that mixes, say, four tier pieces with
    a two-piece ring set reads as two clusters instead of one undifferentiated
    row of icons.

    ``slot_sorted`` off keeps the canonical ascending-id order and yields a
    single group: embellishment comps go through here too, and an embellishment
    is a reagent with no gear slot of its own (its ids resolve via
    ``reagent_lookup``, not ``item_lookup``), so there is nothing to sort or
    group them by.
    """
    # An item that can't be resolved to an inventoryType used to sort to the
    # end, which silently collapsed the whole sort back to the ascending-id
    # tiebreak -- and Blizzard hands out tier-set item ids in alphabetical
    # slot-name order, so the result looked like a deliberate name sort rather
    # than a broken slot sort. Fail loudly instead.
    def slot_pos(item_id):
        item = item_lookup.get(item_id)
        inv_type = (item or {}).get("inventoryType")
        pos = INVTYPE_DISPLAY_ORDER.get(inv_type)
        if pos is None:
            raise ValueError(
                f"spec {spec_id}: {comp_kind} comp item {item_id} has no display "
                f"slot ({'not in item_lookup' if item is None else f'inventoryType={inv_type!r}'})"
                " - refresh equippable-items.json"
            )
        return pos

    comps = []
    for row in raw_rows:
        count = int(row[1] or 0)
        if threshold > 0 and count < threshold:
            continue
        try:
            ids = [int(i) for i in str(row[0]).split(",") if i]
        except (ValueError, TypeError):
            continue
        # comp strings are id-sorted (canonical DB key); show the items grouped
        # by set (largest set first, tie broken by earliest slot) in
        # gear-overview slot order within each group. Setless items (crafted
        # comps) all land in one group, i.e. plain slot order.
        if slot_sorted:
            set_counts = {}
            set_first_slot = {}
            for i in ids:
                sid = item_lookup.get(i, {}).get("itemSetId")
                set_counts[sid] = set_counts.get(sid, 0) + 1
                set_first_slot[sid] = min(
                    set_first_slot.get(sid, NON_GEAR_DISPLAY_ORDER), slot_pos(i)
                )
            group_rank = {
                sid: rank
                for rank, sid in enumerate(
                    sorted(
                        set_counts,
                        key=lambda s: (-set_counts[s], set_first_slot[s]),
                    )
                )
            }
            ids.sort(
                key=lambda i: (
                    group_rank[item_lookup.get(i, {}).get("itemSetId")],
                    slot_pos(i),
                    i,
                )
            )
            # Split into the set runs the sort just produced. Setless items all
            # share the None key, so they stay one cluster rather than becoming
            # one cluster each.
            groups = []
            last_sid = _UNSET
            for i in ids:
                sid = item_lookup.get(i, {}).get("itemSetId")
                if sid != last_sid:
                    groups.append([])
                    last_sid = sid
                groups[-1].append(i)
        else:
            groups = [ids]
        comps.append(
            {
                "ids": ids,
                "groups": groups,
                "count": count,
                "max_timed": row[2],
                "max_depleted": row[3],
            }
        )
        if len(comps) >= limit:
            break
    return comps


def build_multiset_comps(raw_rows, lookup, threshold, limit=10, slot_rank=None):
    """Collapse gem/enchant comps (multisets) into ``{id, qty}`` display entries.

    The DB comp string keeps repeats (e.g. the same enchant on both rings).
    This collapses those into ``{id, qty}`` for a compact "x2" display, drops
    ids the render lookup doesn't know (cosmetic gems / filtered old enchants),
    and re-merges any rows that become identical once those ids are gone so
    their counts sum instead of showing as duplicate rows.

    ``slot_rank`` (used for enchants) sorts each surviving row's entries into
    gear-overview slot order. It is applied only at the very end: the dedupe key
    has to stay in canonical id order or equivalent multisets stop merging.
    """
    merged = {}
    for row in raw_rows:
        count = int(row[1] or 0)
        if threshold > 0 and count < threshold:
            continue
        try:
            ids = [int(i) for i in str(row[0]).split(",") if i]
        except (ValueError, TypeError):
            continue
        # keep only ids the template can render (has an icon/name)
        ids = [
            i for i in ids
            if lookup.get(i) is not None or lookup.get(str(i)) is not None
        ]
        if not ids:
            continue
        # ids arrive id-sorted (canonical DB key); collapse runs of
        # the same id into {id, qty} preserving that canonical order.
        entries = []
        for i in ids:
            if entries and entries[-1]["id"] == i:
                entries[-1]["qty"] += 1
            else:
                entries.append({"id": i, "qty": 1})
        key = tuple((e["id"], e["qty"]) for e in entries)
        existing = merged.get(key)
        if existing:
            existing["count"] += count
            existing["max_timed"] = max(
                existing["max_timed"] or 0, row[2] or 0
            )
            existing["max_depleted"] = max(
                existing["max_depleted"] or 0, row[3] or 0
            )
        else:
            merged[key] = {
                "entries": entries,
                "count": count,
                "max_timed": row[2],
                "max_depleted": row[3],
            }
    # merging can reorder, so re-sort by the summed count.
    ranked = sorted(
        merged.values(), key=lambda m: m["count"], reverse=True
    )[:limit]
    if slot_rank:
        for comp in ranked:
            comp["entries"].sort(
                key=lambda e: (
                    slot_rank(lookup.get(e["id"]) or lookup.get(str(e["id"])), e["id"]),
                    e["id"],
                )
            )
    return ranked


def build_talent_meta(talent_lookup, loadouts, node_pct=None):
    """Meta-loadout payload for the analyzer, folded into the spec meta JSON.

    ``meta_by_hero`` gives analyzer.js the most-run meta loadout string *per hero
    tree* (aggregateData.get_loadout), so it can compare a pasted build against
    the meta build for the player's own hero tree and offer a switch to the
    others; ``popular_hero`` is the tree it defaults its "you're on the off-meta
    tree" note against. ``node_pct`` maps node id -> meta pick-rate percent (int),
    so the analyzer draws the same per-node popularity badges the spec page does.

    The tree *geometry* it decodes and draws against (fullNodeOrder + positioned
    nodes) is NOT baked here — generateAnalyzerPage bakes it, credential-free,
    into assets/json/talent_trees/<spec>.json so it tracks the game data without
    waiting on this DB-driven rebuild. Returns ``None`` when the spec has no meta
    loadouts yet, or when its talent data predates the tree fields (nothing for
    the client to compare against/decode)."""
    if not talent_lookup.get("fullNodeOrder") or not talent_lookup.get("nodes"):
        return None

    meta_by_hero = {}
    popular_hero = None
    popular_count = -1
    for hero_id, info in (loadouts or {}).items():
        code = info.get("loadout")
        if not code:
            continue
        count = int(info.get("count") or 0)
        meta_by_hero[str(hero_id)] = {"loadout": code, "count": count}
        if count > popular_count:
            popular_count = count
            popular_hero = str(hero_id)

    if not meta_by_hero:
        return None
    payload = {"meta_by_hero": meta_by_hero, "popular_hero": popular_hero}
    if node_pct:
        payload["node_pct"] = {str(nid): int(pct) for nid, pct in node_pct.items()}
    return payload


def build_spec_meta_json(
    spec_id, spec_data, class_data,
    left_slots, right_slots, weapon_slots, trinket_slots,
    enchant_slots, enchant_lookup, item_lookup, item_slug_map,
    bis_summary, socket_lookup,
    talent_lookup=None, loadouts=None, node_pct=None,
):
    """Compact, machine-readable meta snapshot for one spec, consumed by the
    client-side "Am I meta?" analyzer (assets/js/analyzer.js). Built entirely
    from data the spec page already computed — no extra queries.

    Per slot it records the two meta *targets* the spec page badges:
    ``top`` (the Raider.io top-50 loadout picks — the page's ``is_bis``/TOP
    badge, up to two for the FINGER/TRINKET groups) and ``sim`` (the
    SimulationCraft rank-1 pick — the ``is_simc_bis``/SIM badge). The analyzer
    scores a slot as a match only when the equipped item is one of these. The
    single most-equipped item is kept as ``common`` for neutral display on slots
    that have neither target (sparse Raider.io/SimC data) — it never counts
    toward the score. Also carries the single most-popular top-50 gem combo and
    enchant combo (multisets, ``gem_combo``/``enchant_combo``) that the analyzer
    scores the player's sockets/enchants against as a per-id quantity budget, how
    many enchanted slots top players run per slot group
    (``enchant_group_expected``, so the client only flags a bare slot "missing"
    while under that count). Icons/quality are baked so the client can render
    item tiles.
    """
    def _name(item_id):
        it = item_lookup.get(item_id) or {}
        return it.get("name")

    def _icon(item_id):
        it = item_lookup.get(item_id) or {}
        return it.get("icon")

    def _quality(entry):
        it = item_lookup.get(entry.get("id")) or {}
        return entry.get("quality_override") or it.get("quality")

    def _pick(entry, **extra):
        item_id = entry.get("id")
        # Link/tooltip fields, mirroring the spec page's render_slot markup
        # (spec_page.html:44-58) so the analyzer can build the same Wowhead
        # tooltips and internal /items links.
        bonus = ((entry.get("bonus") or {}).get("list")) or []
        ench = (entry.get("enchantment") or {}).get("id")
        gems = [s.get("id") for s in (entry.get("socket") or []) if s.get("id")]
        pcs = entry.get("pcs") or None
        pick = {
            "id": item_id,
            "name": _name(item_id),
            "icon": _icon(item_id),
            "quality": _quality(entry),
            "slug": item_slug_map.get(item_id),
            "bonus": bonus,
            "ench": ench,
            "gems": gems,
            "pcs": pcs,
        }
        # Only set when true, so the flag stays absent on the 15 slots it can't
        # apply to. The analyzer uses it to tell a 1H+off-hand player that the
        # meta two-hander replaces BOTH of their weapons — which is never the
        # case for a Titan's Grip spec, hence the spec_id.
        if occupies_both_hands(item_lookup.get(item_id), spec_id):
            pick["two_handed"] = True
        pick.update(extra)
        return pick

    slots = {}
    for collection in (left_slots, right_slots, weapon_slots, trinket_slots):
        for slot_dict in collection:
            entries = slot_dict.get("entries") or []
            if not entries:
                continue
            slot_name = slot_dict.get("slot")
            slot_count = slot_dict.get("slot_count") or 0
            top = [_pick(e, pct=e.get("bis_pct")) for e in entries if e.get("is_bis")]
            simc = next((e for e in entries if e.get("is_simc_bis")), None)
            common = entries[0]
            common_pct = round(common.get("count", 0) / slot_count * 100, 1) if slot_count else None
            slots[slot_name] = {
                "top": top,
                "sim": _pick(simc, dps_pct=simc.get("simc_dps_pct")) if simc else None,
                "common": _pick(common, pct=common_pct),
            }

    # The single most-popular gem/enchant combo among the top-50 verified
    # loadouts (the same `best` combos the spec page's "TOP" badge is built from,
    # `compute_bis_from_top_loadouts`). The analyzer treats each as a per-id
    # quantity budget and flags sockets/enchants that fall outside it. The canonical
    # `key` is an ascending, comma-joined multiset (repeats kept); collapse it back
    # into `{id, qty}` display entries, dropping ids the lookup can't resolve
    # (cosmetic/filtered noise, mirroring the spec page's combo rendering).
    def _combo_from_best(best, lookup, is_enchant):
        if not best or not best.get("key"):
            return None
        counts = {}
        for tok in str(best["key"]).split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                cid = int(tok)
            except ValueError:
                continue
            counts[cid] = counts.get(cid, 0) + 1
        entries = []
        for cid, qty in counts.items():
            info = lookup.get(cid) or lookup.get(int(cid)) or {}
            if not info:
                continue  # unresolved id -> drop from the display combo
            entry = {
                "id": cid,
                "qty": qty,
                "name": info.get("itemName") or info.get("displayName") or info.get("name"),
                "icon": info.get("itemIcon") or info.get("icon") or info.get("spellIcon"),
                "quality": info.get("quality"),
            }
            if is_enchant:
                # Wowhead tooltip/link for the enchant: its scroll itemId, or its
                # spellId for runes/enchants with no scroll item (DK weapon runes).
                entry.update(_enchant_link_fields(info))
            entries.append((entry, info))
        if not entries:
            return None
        if is_enchant:
            # Gear-overview slot order, matching the spec page's Enchant Combos
            # section -- the analyzer renders these entries in array order.
            entries.sort(key=lambda t: (enchant_slot_pos(t[1], t[0]["id"]), t[0]["id"]))
        else:
            # Gems have no slot, so most-repeated ids first for a stable,
            # popularity-ish ordering.
            entries.sort(key=lambda t: (-t[0]["qty"], t[0]["id"]))
        entries = [entry for entry, _info in entries]
        return {
            "entries": entries,
            "pct": round(best.get("pct") or 0, 1),
            "count": int(best.get("count") or 0),
        }

    bis = bis_summary or {}
    gem_combo = _combo_from_best((bis.get("gem_comps") or {}).get("best"), socket_lookup, False)
    enchant_combo = _combo_from_best((bis.get("enchant_comps") or {}).get("best"), enchant_lookup, True)

    # enchant id -> its normalized slot group (FINGER/WEAPON/...), from the
    # popular valid enchants per group. enchant_slots is keyed by the same
    # SLOT_GROUPS names the client's ENCHANT_GROUP map targets.
    ench_id_group = {}
    for grp, lst in (enchant_slots or {}).items():
        for e in (lst or []):
            eid = e.get("id")
            if eid is not None:
                ench_id_group[int(eid)] = grp
    # How many enchanted slots top players run per group, read off the combo, so
    # the client flags a bare slot "missing" only while under that count: a
    # caster's WEAPON expected == 1 won't flag the un-enchantable off-hand, while
    # a dual-wielder's WEAPON == 2 still flags a bare second weapon. FINGER == 2
    # means both rings are expected to be enchanted.
    enchant_group_expected = {}
    for e in (enchant_combo or {}).get("entries", []):
        grp = ench_id_group.get(int(e["id"]))
        if grp:
            enchant_group_expected[grp] = enchant_group_expected.get(grp, 0) + e["qty"]

    meta = {
        "spec_id": int(spec_id),
        "spec": spec_data.get("name"),
        "class": class_data.get("name"),
        "slots": slots,
        "gem_combo": gem_combo,
        "enchant_combo": enchant_combo,
        "enchant_group_expected": enchant_group_expected,
    }
    talents = build_talent_meta(talent_lookup or {}, loadouts or {}, node_pct or {})
    if talents:
        meta["talents"] = talents
    return meta


def _enchant_link_fields(info):
    """Wowhead link fields baked onto an enchant: its scroll ``itemId`` and/or its
    ``spellId``. Normal enchants have a scroll item; DK weapon runes have only a
    spellId (no scroll item). Baking both lets the client link the enchant itself
    (item=<scroll> or spell=<rune>), never item=<enchant_id>, which collides with
    an unrelated item. Shared by both the combo entries and the gem/enchant index
    so the pair never diverges. None-valued keys are omitted.
    """
    out = {}
    if info.get("itemId") is not None:
        out["itemId"] = info["itemId"]
    if info.get("spellId") is not None:
        out["spellId"] = info["spellId"]
    return out


def write_analyzer_gem_enchant_index(enchant_lookup_all):
    """Bake a spec-independent gem/enchant icon+name index for analyzer.js.

    Written once per build to ``assets/json/gem_enchant_index.json`` as
    ``{"gems": {<gemItemId>: {...}}, "enchants": {<enchantId>: {...}}}``:

      - gems are keyed by their item id (what the SimC export's ``gem_id`` is),
      - enchants are keyed by their SimC ``enchant_id`` and carry ``itemId``
        (the enchanting scroll) so the client links/tooltips the *enchant*, not
        an unrelated item that happens to share the enchant's numeric id.

    Mirrors the ``enchantments.json`` catalog the spec pages already use, so the
    analyzer resolves anything a player actually has, not just the top combo.
    """
    gems = {}
    enchants = {}
    for e in enchant_lookup_all or []:
        name = e.get("itemName") or e.get("displayName")
        icon = e.get("itemIcon") or e.get("spellIcon")
        quality = e.get("quality")
        if e.get("slot") == "socket":
            gid = e.get("itemId")
            if gid is None:
                continue
            gems[str(gid)] = {"name": name, "icon": icon, "quality": quality}
        else:
            eid = e.get("id")
            if eid is None:
                continue
            entry = {"name": name, "icon": icon, "quality": quality}
            entry.update(_enchant_link_fields(e))
            enchants[str(eid)] = entry
    out_dir = os.path.join("assets", "json")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "gem_enchant_index.json"), "w", encoding="utf-8") as f:
        json.dump({"gems": gems, "enchants": enchants}, f, separators=(",", ":"), ensure_ascii=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"wrote gem_enchant_index.json ({len(gems)} gems, {len(enchants)} enchants)"
    )


def checkItemLimits(sockets, socket_lookup, socket_limits):
    for socket in sockets:
        if not socket_lookup.get(int(socket["id"])):
            continue
        limit = socket_lookup[int(socket["id"])].get("itemLimitCategory")
        if limit:
            if socket_limits.get(limit["id"]):
                if limit["quantity"] >= socket_limits.get(limit["id"]):
                    continue
                else:
                    socket_limits[limit["id"]] += 1
                    return socket
            else:
                socket_limits[limit["id"]] = limit["quantity"]
                return socket
        else:
            return socket
    return


def compute_bis_from_top_loadouts(
    top_loadouts,
    item_lookup=None,
    missive_lookup=None,
    embellishment_lookup=None,
    crafted_item_ids=None,
    hero_node_subtree=None,
):
    """Compute BIS summary from a list of top-player loadouts.

    Input: list of loadout dicts as returned by `databaseConnector.fetch_top50_loadouts`.
    Returns a dict with `items`, `enchants`, `gems`, `talents`, `full_loadout` summary.

    With `hero_node_subtree` (hero node_id -> subTreeId) each loadout is also
    assigned the hero tree most of its hero nodes belong to, which feeds
    `hero_tree_loadout_counts` and `talent_dungeon_stats` (per hero tree, the
    raw per-dungeon talent counts the Talent Differences modal is built from).

    When the optional lookups are supplied it additionally derives the top-50
    versions of the combo/detail sections (mirroring the DB aggregation logic so
    the canonical id keys line up with the general-population lists):
      - ``crafted_items`` / ``crafted_comps`` (item ids in ``crafted_item_ids``)
      - ``tier_set_comps`` (items sharing an ``itemSetId``, >=2 pieces)
      - ``gem_comps`` / ``enchant_comps`` (multisets of gem / enchant ids)
      - ``missives`` / ``embellishments`` / ``embellishment_comps`` (resolved from
        each item's ``bonus_ids`` via ``missive_lookup`` / ``embellishment_lookup``)
    Each of these is a dict ``{canonical_key: {"count", "pct"}}`` plus a ``best``
    entry, where ``canonical_key`` is the ascending-id, comma-joined string the DB
    uses (a single item id for the detail lists).
    """

    n = len(top_loadouts)
    if n == 0:
        return {}

    item_lookup = item_lookup or {}
    missive_lookup = missive_lookup or {}
    embellishment_lookup = embellishment_lookup or {}
    crafted_item_ids = {int(i) for i in (crafted_item_ids or [])}

    # Counts
    items_counts = defaultdict(lambda: defaultdict(int))  # slot -> item_id -> count
    item_ilvl_sum = defaultdict(lambda: defaultdict(int))
    enchant_counts = defaultdict(lambda: defaultdict(int))  # slot_group -> enchant_id -> count
    gem_counts = defaultdict(int)  # gem_item_id -> count (weighted by usage_count)
    talent_node_counts = defaultdict(int)  # node_id -> count
    # Which choice was picked per node: node_id -> (entry_id, spell_id) -> count.
    # entry/spell ids are NULL on rows collected before the schema gained them.
    talent_entry_counts = defaultdict(lambda: defaultdict(int))
    # Per-dungeon talent adoption. top_player_loadouts is keyed per dungeon
    # (map_challenge_mode_id), so each loadout belongs to one dungeon; this lets
    # the Talent Differences modal flag niche talents top players take in a
    # specific dungeon.
    talent_dungeon_counts = defaultdict(lambda: defaultdict(int))  # dungeon -> node_id -> count
    dungeon_loadout_totals = defaultdict(int)  # dungeon -> number of loadouts
    full_loadout_counts = defaultdict(int)

    # Same counts split by the hero tree each loadout runs. A build's whole
    # talent shape follows its hero tree, so per-dungeon deviations are only
    # meaningful within one tree (comparing across trees mostly measures which
    # tree happened to be sampled in that dungeon). `ranks` sums the points
    # spent per node so multi-rank nodes can show an average.
    def _new_dungeon_stats():
        return {"total": 0, "nodes": defaultdict(int), "ranks": defaultdict(int)}

    def _new_tree_stats():
        return {
            "total": 0,
            "nodes": defaultdict(int),
            "ranks": defaultdict(int),
            "dungeons": defaultdict(_new_dungeon_stats),
        }

    tree_talent_stats = defaultdict(_new_tree_stats)  # subTreeId -> stats
    hero_tree_loadout_counts = defaultdict(int)  # subTreeId -> loadouts

    for lo in top_loadouts:
        # meta loadout key
        meta = lo.get("meta") if isinstance(lo.get("meta"), dict) else lo
        loadout_key = meta.get("loadout_key") if isinstance(meta, dict) else None
        if loadout_key:
            full_loadout_counts[loadout_key] += 1

        # items
        for it in lo.get("items", []) or []:
            slot = it.get("slot")
            # normalize multi-slot names to their group (e.g., TRINKET_1 -> TRINKET)
            slot = MULTI_SLOT_GROUPS.get(slot, slot)
            item_id = it.get("item_id") or it.get("item")
            if not slot or not item_id:
                continue
            items_counts[slot][int(item_id)] += 1
            ilvl = it.get("item_level")
            if ilvl:
                item_ilvl_sum[slot][int(item_id)] += int(ilvl)

        # gems
        for g in lo.get("gems", []) or []:
            gid = g.get("gem_item_id") or g.get("id")
            if not gid:
                continue
            usage = int(g.get("usage_count", 1) or 1)
            gem_counts[int(gid)] += usage

        # enchants
        for e in lo.get("enchants", []) or []:
            sg = e.get("slot_group") or e.get("slot")
            eid = e.get("enchantment_id") or e.get("id")
            if not sg or not eid:
                continue
            enchant_counts[sg][int(eid)] += 1

        # talents
        dungeon = meta.get("map_challenge_mode_id") if isinstance(meta, dict) else None
        if dungeon is not None:
            dungeon_loadout_totals[int(dungeon)] += 1
        loadout_nodes = []
        loadout_ranks = {}
        for t in lo.get("talents", []) or []:
            node = t.get("node_id") or t.get("id")
            if not node:
                continue
            loadout_nodes.append(int(node))
            loadout_ranks[int(node)] = int(t.get("node_rank") or 1)
            talent_node_counts[int(node)] += 1
            if dungeon is not None:
                talent_dungeon_counts[int(dungeon)][int(node)] += 1
            entry_id = t.get("entry_id")
            spell_id = t.get("spell_id")
            if entry_id is not None or spell_id is not None:
                talent_entry_counts[int(node)][(entry_id, spell_id)] += 1

        # hero tree of this loadout: the subtree most of its hero nodes sit in
        loadout_tree = None
        if hero_node_subtree:
            subtree_hits = defaultdict(int)
            for node in loadout_nodes:
                subtree = hero_node_subtree.get(node)
                if subtree is not None:
                    subtree_hits[subtree] += 1
            if subtree_hits:
                loadout_tree = max(subtree_hits.items(), key=lambda x: x[1])[0]
        if loadout_tree is not None:
            hero_tree_loadout_counts[loadout_tree] += 1
            buckets = [tree_talent_stats[loadout_tree]]
            if dungeon is not None:
                buckets.append(tree_talent_stats[loadout_tree]["dungeons"][int(dungeon)])
            for bucket in buckets:
                bucket["total"] += 1
                for node in loadout_nodes:
                    bucket["nodes"][node] += 1
                    bucket["ranks"][node] += loadout_ranks.get(node, 1)

    # Build summary
    def _top_n_from_countmap(countmap, n_top=3, total=n):
        items = sorted(countmap.items(), key=lambda x: x[1], reverse=True)
        out = []
        for item_id, cnt in items[:n_top]:
            out.append({"id": int(item_id), "count": int(cnt), "pct": (int(cnt) / total) * 100.0})
        return out

    items_summary = {}
    for slot, cmap in items_counts.items():
        details = _top_n_from_countmap(cmap, 3, n)
        best = details[0] if details else None
        # average ilvl for each detail if available
        for d in details:
            iid = d["id"]
            ilvl_sum = item_ilvl_sum.get(slot, {}).get(iid)
            if ilvl_sum:
                # divide by number of occurrences of this item
                occurrences = cmap.get(iid, 1)
                try:
                    d["avg_item_level"] = int(ilvl_sum / occurrences)
                except Exception:
                    d["avg_item_level"] = None
        items_summary[slot] = {"best": best, "details": details, "total": n}

    enchants_summary = {}
    for sg, cmap in enchant_counts.items():
        details = _top_n_from_countmap(cmap, 3, n)
        enchants_summary[sg] = {"best": details[0] if details else None, "details": details, "total": n}

    gems_summary = []
    for gid, cnt in sorted(gem_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        raw_cnt = int(cnt)
        # cap displayed gem count at the number of loadouts (n) to avoid >100% values
        display_cnt = raw_cnt if raw_cnt <= n else n
        pct = (display_cnt / (n or 1)) * 100.0
        if pct > 100.0:
            pct = 100.0
        gems_summary.append({"id": int(gid), "count": int(display_cnt), "pct": pct, "raw_count": raw_cnt})

    talents_summary = []
    for nid, cnt in sorted(talent_node_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        talents_summary.append({"node_id": int(nid), "count": int(cnt), "pct": (int(cnt) / (n or 1)) * 100.0})

    # Full per-node top-50 usage map (every node any top player took), so the
    # talent tree can flag elite-vs-popular divergence for all nodes, not just
    # the top 20 shown in `talents_summary`.
    talent_node_pct = {
        int(nid): {"count": int(cnt), "pct": (int(cnt) / (n or 1)) * 100.0}
        for nid, cnt in talent_node_counts.items()
    }

    # Per-choice top-50 usage: which entry the elite players pick on choice
    # nodes. Same denominator as talent_node_pct so the pcts are comparable.
    talent_node_entry_pct = {
        int(nid): [
            {
                "entry_id": eid,
                "spell_id": sid,
                "count": int(cnt),
                "pct": (int(cnt) / (n or 1)) * 100.0,
            }
            for (eid, sid), cnt in sorted(emap.items(), key=lambda x: x[1], reverse=True)
        ]
        for nid, emap in talent_entry_counts.items()
    }

    # Raw per-dungeon talent counts per hero tree, with their loadout
    # denominators and points-spent sums. The Talent Differences modal derives
    # both its per-dungeon gains/losses (via
    # `aggregateData.dungeon_talent_deviations_from_top`) and its per-dungeon
    # tree view from these; keeping counts rather than pcts lets the consumer
    # apply a sample-size floor.
    def _counts(bucket):
        return {
            "total": int(bucket["total"]),
            "nodes": {int(nid): int(cnt) for nid, cnt in bucket["nodes"].items()},
            "ranks": {int(nid): int(cnt) for nid, cnt in bucket["ranks"].items()},
        }

    talent_dungeon_stats = {
        int(tid): {
            **_counts(s),
            "dungeons": {str(d): _counts(ds) for d, ds in s["dungeons"].items()},
        }
        for tid, s in tree_talent_stats.items()
    }

    # most common full loadout if present
    full_loadout_top = None
    if full_loadout_counts:
        fk, fc = max(full_loadout_counts.items(), key=lambda x: x[1])
        full_loadout_top = {"loadout_key": fk, "count": int(fc), "pct": (int(fc) / n) * 100.0}

    # ---- Combo / detail sections (top-50 versions) -------------------------
    # Second pass over the loadouts, mirroring the per-(run, member) DB
    # aggregation so the canonical keys match the general-population lists.
    # Counts are per-loadout presence (a loadout that runs a comp counts once),
    # denominator n = number of top-50 loadouts, so pct is comparable to the
    # gem/enchant TOP badges already on the page.
    crafted_item_counts = defaultdict(int)     # item_id -> loadouts
    crafted_comp_counts = defaultdict(int)      # "id,id" -> loadouts
    tier_set_comp_counts = defaultdict(int)
    gem_comp_counts = defaultdict(int)
    enchant_comp_counts = defaultdict(int)
    missive_counts = defaultdict(int)           # reagent item_id -> loadouts
    embellishment_counts = defaultdict(int)
    embellishment_comp_counts = defaultdict(int)

    def _comp_key(ids):
        # canonical DB key: ascending ids (repeats kept), comma-joined
        return ",".join(str(i) for i in sorted(ids))

    for lo in top_loadouts:
        items = lo.get("items", []) or []

        # crafted items + crafted comp
        crafted_ids = [
            int(it.get("item_id"))
            for it in items
            if it.get("item_id") and int(it.get("item_id")) in crafted_item_ids
        ]
        for iid in set(crafted_ids):
            crafted_item_counts[iid] += 1
        if crafted_ids:
            crafted_comp_counts[_comp_key(crafted_ids)] += 1

        # tier set comp: items sharing an itemSetId, only sets worn >=2 pieces
        set_members = defaultdict(list)
        for it in items:
            iid = it.get("item_id")
            if not iid:
                continue
            sid = item_lookup.get(int(iid), {}).get("itemSetId")
            if sid:
                set_members[sid].append(int(iid))
        set_ids = [iid for members in set_members.values() if len(members) >= 2 for iid in members]
        if set_ids:
            tier_set_comp_counts[_comp_key(set_ids)] += 1

        # gem comp: multiset of gem item ids (usage_count = repeats)
        gem_ids = []
        for g in lo.get("gems", []) or []:
            gid = g.get("gem_item_id") or g.get("id")
            if not gid:
                continue
            gem_ids.extend([int(gid)] * int(g.get("usage_count", 1) or 1))
        if gem_ids:
            gem_comp_counts[_comp_key(gem_ids)] += 1

        # enchant comp: multiset of enchantment ids
        ench_ids = []
        for e in lo.get("enchants", []) or []:
            eid = e.get("enchantment_id") or e.get("id")
            if eid:
                ench_ids.append(int(eid))
        if ench_ids:
            enchant_comp_counts[_comp_key(ench_ids)] += 1

        # missives + embellishments (+ embellishment comp) from bonus ids
        missive_ids = set()
        embellishment_ids = []
        for it in items:
            raw = it.get("bonus_ids")
            if not raw:
                continue
            for b in str(raw).split(","):
                b = b.strip()
                if not b:
                    continue
                m = missive_lookup.get(b)
                if m is not None:
                    missive_ids.add(int(m))
                em = embellishment_lookup.get(b)
                if em is not None:
                    embellishment_ids.append(int(em))
        for mid in missive_ids:
            missive_counts[mid] += 1
        for emid in set(embellishment_ids):
            embellishment_counts[emid] += 1
        if embellishment_ids:
            embellishment_comp_counts[_comp_key(embellishment_ids)] += 1

    def _summarize(countmap):
        """Turn a {key: count} map into {'by_key': {key: {count, pct}}, 'best': {...}}."""
        by_key = {
            k: {"count": int(c), "pct": (int(c) / n) * 100.0}
            for k, c in countmap.items()
        }
        best = None
        if countmap:
            bk, bc = max(countmap.items(), key=lambda x: x[1])
            best = {"key": bk, "count": int(bc), "pct": (int(bc) / n) * 100.0}
        return {"by_key": by_key, "best": best}

    return {
        "num_loadouts": n,
        "items": items_summary,
        "enchants": enchants_summary,
        "gems": gems_summary,
        "talents": talents_summary,
        "talent_node_pct": talent_node_pct,
        "talent_node_entry_pct": talent_node_entry_pct,
        "talent_dungeon_stats": talent_dungeon_stats,
        "hero_tree_loadout_counts": {
            int(tid): int(cnt) for tid, cnt in hero_tree_loadout_counts.items()
        },
        "full_loadout": full_loadout_top,
        "crafted_items": _summarize(crafted_item_counts),
        "crafted_comps": _summarize(crafted_comp_counts),
        "tier_set_comps": _summarize(tier_set_comp_counts),
        "gem_comps": _summarize(gem_comp_counts),
        "enchant_comps": _summarize(enchant_comp_counts),
        "missives": _summarize(missive_counts),
        "embellishments": _summarize(embellishment_counts),
        "embellishment_comps": _summarize(embellishment_comp_counts),
    }


def handleSocketsForItem(
    conn,
    cursor,
    spec_id,
    current_season_id,
    item_id,
    amount,
    sockets,
    socket_limits,
    socket_lookup,
    socket_map=None,
):
    sockets_data = []
    if amount > 0:
        # prefer socket_map if provided (no db roundtrip)
        if socket_map is not None:
            current_socket_items = socket_map.get(str(item_id), [])
            used_sockets = [pair[0] for pair in current_socket_items]
        else:
            current_socket_items = databaseConnector.fetch_top_sockets_for_item(
                conn, cursor, spec_id, current_season_id, item_id
            )
            used_sockets = [pair[0] for pair in current_socket_items if len(pair) > 0]

        for _ in range(0, amount):
            active_socket = None
            if len(sockets) > 0 and sockets[0] in used_sockets:
                active_socket = checkItemLimits(sockets, socket_lookup, socket_limits)
            elif used_sockets and len(used_sockets) > 0:
                used_sockets_converted = [{"id": socket} for socket in used_sockets]
                active_socket = checkItemLimits(
                    used_sockets_converted, socket_lookup, socket_limits
                )
            if active_socket:
                sockets_data.append(active_socket)
    return sockets_data


def filter_gear_entries(entries, slot_total):
    """Drop the noise tail of a top-10 gear list.

    ``entries`` are sorted by count desc; anything past the first
    GEAR_LIST_MIN_KEEP entries must hold at least GEAR_LIST_MIN_SLOT_SHARE% of
    the slot's total runs. Mirrored in SQL by
    databaseConnector.FETCH_SPEC_PAGE_LINKED_ITEMS_SQL — keep the two in sync.
    """
    if not slot_total:
        return entries
    min_count = slot_total * GEAR_LIST_MIN_SLOT_SHARE / 100.0
    return [e for i, e in enumerate(entries)
            if i < GEAR_LIST_MIN_KEEP or e["count"] >= min_count]


def filter_weapon_gear_entries(weapon_lists, slot_totals):
    """Noise-filter the MAIN_HAND/OFF_HAND lists jointly.

    Weapons share one denominator (the combined weapon-slot total) and one
    min-keep floor (over the combined ranking): if 99% of a spec runs
    two-handers, the handful of loadouts that equip an off-hand anyway are a
    fraction of the *weapon* runs, so no off-hand list is rendered — a per-slot
    floor would instead promote that junk to a top-3. ``weapon_lists`` is the
    fetched entry lists in WEAPON_SLOTS order. Mirrored in SQL by the
    weapon_ranked CTE of FETCH_SPEC_PAGE_LINKED_ITEMS_SQL — keep in sync.
    """
    weapon_total = sum(slot_totals.get(s, 0) for s in WEAPON_SLOTS)
    if not weapon_total:
        return weapon_lists
    # Combined ranking with the SQL mirror's exact tiebreak (count desc, then
    # item id as string, then slot) — a dual-wield one-hander can appear in
    # both lists, so entries are tracked by identity, not item id.
    tagged = [(e, s) for lst, s in zip(weapon_lists, WEAPON_SLOTS) for e in lst]
    tagged.sort(key=lambda t: (-t[0]["count"], str(t[0]["item"]), t[1]))
    floor = {id(t[0]) for t in tagged[:GEAR_LIST_MIN_KEEP]}
    min_count = weapon_total * GEAR_LIST_MIN_SLOT_SHARE / 100.0
    return [[e for e in lst if id(e) in floor or e["count"] >= min_count]
            for lst in weapon_lists]


def fetch_slot_info(conn, cursor, spec_id, current_season_id, slot, slot_totals):
    if MULTI_SLOT_GROUPS.get(slot):
        group = MULTI_SLOT_GROUPS[slot]
        num = re.search(r"\d+", slot)
        data = databaseConnector.fetch_top_items_for_slot_group_with_bonus(
            conn, cursor, spec_id, current_season_id, group
        )
        # Filter on the intact group list (before the positional removal below)
        # so FINGER_1/FINGER_2 both derive from the same filtered ranking — the
        # same list the SQL mirror models.
        group_total = sum(t for s, t in slot_totals.items()
                          if MULTI_SLOT_GROUPS.get(s) == group)
        data = filter_gear_entries(data, group_total)
        index_to_remove = int(num.group()) - 1
        if 0 <= index_to_remove < len(data):
            del data[index_to_remove]
        return data
    data = databaseConnector.fetch_top_items_for_slot_with_bonus(
        conn, cursor, spec_id, current_season_id, slot
    )
    if slot in WEAPON_SLOTS:
        # Returned unfiltered: MAIN_HAND/OFF_HAND are filtered together by
        # filter_weapon_gear_entries at the call site.
        return data
    return filter_gear_entries(data, slot_totals.get(slot))


def fetch_hero_tree_info(conn, cursor, spec_id, current_season_id, valid_subtrees=None):
    popular_hero_tree = 0
    popular_hero_tree_count = 0
    hero_trees = aggregateData.get_hero_trees(
        conn, cursor, spec_id, current_season_id, valid_subtrees
    )
    hero_tree_count = 0
    for tree in hero_trees:
        if tree.get("count"):
            count = tree["count"]
            hero_tree_count += count
            if count > popular_hero_tree_count:
                popular_hero_tree_count = count
                popular_hero_tree = tree.get("id")
    return hero_trees, popular_hero_tree, popular_hero_tree_count, hero_tree_count


def fetch_enchant_info(
    conn, cursor, spec_id, current_season_id, enchant_lookup, spec_sample_size,
    current_expansion,
):
    enchant_slots_raw = {
        slot_group: aggregateData.get_enchants_for_slot(
            conn, cursor, spec_id, current_season_id, slot_group
        )
        for slot_group in SLOT_GROUPS
    }
    total_enchant_counts = {slot_group: 0 for slot_group in SLOT_GROUPS}
    enchant_slots = {}
    for slot_group, enchants in enchant_slots_raw.items():
        if enchants and len(enchants) > 0:
            valid_enchants = []
            for enchant in enchants:
                enchant_id = enchant.get("id")
                record = enchant_lookup.get(enchant_id) if enchant_id else None
                if record is None:
                    print(
                        f"Warning: enchant {enchant_id} (slot {slot_group}, count {enchant.get('count')}) not in enchantments.json for spec {spec_id} - skipping"
                    )
                    continue
                if not commonUtils.is_enchant_relevant(record, current_expansion, slot_group):
                    continue  # old-expansion / slot-incompatible: silent drop (expected noise)
                valid_enchants.append(enchant)
                total_enchant_counts[slot_group] += enchant.get("count")
            enchant_slots[slot_group] = valid_enchants
    # Hide slot groups enchanted by <1% of sampled characters. The denominator
    # must be the ~14-day gear-retention sample, not season-wide run counts —
    # those keep growing while the sample stays fixed-size, which filtered out
    # every slot but FINGER for popular specs late in the season.
    threshold = spec_sample_size * 0.01
    enchant_slots = {
        sg: lst
        for sg, lst in enchant_slots.items()
        if total_enchant_counts.get(sg, 0) >= threshold
    }
    return enchant_slots, total_enchant_counts


def convert_slots(
    conn,
    cursor,
    spec_id,
    current_season_id,
    slots,
    item_lookup,
    bonus_lookup,
    missive_lookup,
    embellishment_lookup,
    bonus_quality_lookup,
    sockets,
    socket_lookup,
    enchant_slots,
    set_members,
    missives=None,
    embellishments=None,
    bis_summary=None,
    simc_bis=None,
):
    primary_ids = {int(items[0]["item"]) for items in slots if len(items) > 0}

    all_item_ids = set()
    for items in slots:
        for it in items:
            all_item_ids.add(int(it.get("item")))
    socket_map = databaseConnector.fetch_top_sockets_for_items(
        conn, cursor, spec_id, current_season_id, list(all_item_ids)
    )

    socket_limits = {}
    for items, slot in zip(
        slots, LEFT_ORDER + RIGHT_ORDER + WEAPON_SLOTS + TRINKET_SLOTS
    ):
        for item in items:
            sid = item_lookup[int(item["item"])].get("itemSetId")
            if sid:
                raw_peers = [pid for pid in set_members[sid]]
                peers = [pid for pid in raw_peers if pid in primary_ids]
                # set bonuses start at 2 pieces; a lone piece of a set (e.g. a
                # single set trinket) shouldn't render as an equipped set
                if len(peers) >= 2:
                    item["pcs"] = peers

            amount = 0
            if not item.get("bonus"):
                continue
            bonus = item.get("bonus", {}).get("ids", "")
            bonus_ids = bonus.split(",")
            # Colour the equipped item by the variant its bonus ids resolve to,
            # not the base item quality (shared with the item page).
            q = commonUtils.resolve_bonus_quality(bonus_ids, bonus_quality_lookup)
            if q is not None:
                item["quality_override"] = q
            for bonus in bonus_ids:
                b_data = bonus_lookup.get(str(bonus))
                if b_data:
                    if b_data.get("socket"):
                        amount += b_data.get("socket")
                    if missive_lookup.get(str(bonus)):
                        if missives and len(missives) > 0:
                            item["missive"] = missives[0][0]
                    if embellishment_lookup.get(str(bonus)):
                        if embellishments and len(embellishments) > 0:
                            item["embellishment"] = embellishments[0][0]
                    if "craftedStats" in b_data:
                        if "crafted_stats" not in item:
                            item["crafted_stats"] = []
                        for stat_id in b_data["craftedStats"]:
                            stat_type = BLIZZARD_STAT_MAP.get(stat_id)
                            if stat_type:
                                item["crafted_stats"].append({"type": stat_type, "alloc": 0})
            if amount < len(
                item_lookup.get(int(item["item"]), {})
                .get("socketInfo", {})
                .get("sockets", [])
            ):
                print(
                    f"Adjusting amount for item {item['item']}: {amount} {len(item_lookup.get(int(item['item']), {}).get('socketInfo', {}).get('sockets', []))}"
                )
                amount = len(
                    item_lookup.get(int(item["item"]), {})
                    .get("socketInfo", {})
                    .get("sockets", [])
                )

            sockets_data = handleSocketsForItem(
                conn,
                cursor,
                spec_id,
                current_season_id,
                item["item"],
                amount,
                sockets,
                socket_limits,
                socket_lookup,
                socket_map,
            )
            if sockets_data:
                item["socket"] = sockets_data

            enchantment_data = {}
            # Low-usage slot groups are already filtered out of enchant_slots
            # (fetch_enchant_info), so presence alone decides here.
            if slot in WEAPON_SLOTS:
                weapon_ok = (
                    enchant_slots.get("WEAPON")
                    and len(enchant_slots["WEAPON"]) > 0
                )
                if item_lookup[int(item["item"])].get("itemClass") == 2 and weapon_ok:
                    enchantment_data = enchant_slots["WEAPON"][0]
            # direct slot-specific enchants
            elif enchant_slots.get(slot) and len(enchant_slots[slot]) > 0:
                enchantment_data = enchant_slots[slot][0]
            # multi-slot groups (FINGER/TRINKET)
            elif (
                MULTI_SLOT_GROUPS.get(slot)
                and enchant_slots.get(MULTI_SLOT_GROUPS[slot])
                and len(enchant_slots[MULTI_SLOT_GROUPS[slot]]) > 0
            ):
                enchantment_data = enchant_slots[MULTI_SLOT_GROUPS[slot]][0]
            item["enchantment"] = enchantment_data

            # SimulationCraft BiS annotation: mark the item if it is the rank-1
            # (highest simulated DPS) candidate for this slot. Keyed by the same
            # Blizzard slot names used here (HEAD, FINGER_1, TRINKET_1, ...).
            if simc_bis:
                ranked = simc_bis.get(slot)
                if ranked:
                    best = ranked[0]
                    if int(best.get("item_id")) == int(item.get("item")):
                        item["is_simc_bis"] = True
                        item["simc_rank"] = best.get("rank", 1)
                        item["simc_dps_pct"] = best.get("dps_pct_gain")

            # BIS annotations: items, enchants, gems (respect multi-slot groups)
            if bis_summary and isinstance(bis_summary, dict):
                bis_threshold = BIS_PCT_THRESHOLD
                # Items: support group mapping for TRINKET/FINGER
                items_map = bis_summary.get("items", {})
                # prefer group summary (e.g., TRINKET, FINGER) for multi-slot
                slot_candidates = [slot]
                grp = MULTI_SLOT_GROUPS.get(slot)
                if grp:
                    slot_candidates.insert(0, grp)

                slot_summary = None
                for sc in slot_candidates:
                    if sc in items_map:
                        slot_summary = items_map.get(sc)
                        break

                if slot_summary:
                    details = slot_summary.get("details", [])
                    # For groups that allow two equipped items (e.g., FINGER, TRINKET), respect usage threshold
                    two_slot_groups = {"FINGER", "TRINKET"}
                    try:
                        grp_name = grp or None
                    except Exception:
                        grp_name = None

                    if grp_name in two_slot_groups:
                        # collect top entries (up to first two) that exceed threshold
                        top_candidates = [d for d in details[:2] if float(d.get("pct", 0.0)) > bis_threshold]
                        top_ids = [int(d.get("id")) for d in top_candidates if d.get("id")]
                        if int(item.get("item")) in top_ids:
                            # set rank/pct/count based on position in the first-two list
                            for idx, d in enumerate(details[:2]):
                                if int(d.get("id")) == int(item.get("item")) and float(d.get("pct", 0.0)) > bis_threshold:
                                    item["is_bis"] = True
                                    item["bis_rank"] = idx + 1
                                    item["bis_pct"] = float(d.get("pct", 0.0))
                                    item["bis_count"] = int(d.get("count", 0))
                                    break
                    else:
                        best = slot_summary.get("best")
                        if best and float(best.get("pct", 0.0)) > bis_threshold and int(best.get("id")) == int(item.get("item")):
                            item["is_bis"] = True
                            item["bis_pct"] = float(best.get("pct", 0.0))
                            item["bis_count"] = int(best.get("count", 0))
                        else:
                            for idx, d in enumerate(details):
                                if int(d.get("id")) == int(item.get("item")):
                                    item["bis_rank"] = idx + 1
                                    item["bis_pct"] = float(d.get("pct", 0.0))
                                    item["bis_count"] = int(d.get("count", 0))
                                    break

                # Enchants: per-slot-group
                # Be tolerant of plural/singular differences (e.g., SHOULDERS vs SHOULDER)
                ench_map = bis_summary.get("enchants", {})
                ench_id = None
                if item.get("enchantment"):
                    ench_id = item["enchantment"].get("id") or item["enchantment"].get("enchantment_id")
                if ench_id:
                    # build candidate keys to search in the bis enchants map
                    candidates = set()
                    candidates.add(slot)
                    candidates.add(f"{slot}S")
                    if slot.endswith("S"):
                        candidates.add(slot.rstrip("S"))
                    if grp:
                        candidates.add(grp)
                        candidates.add(f"{grp}S")
                        candidates.add(f"{grp}_1")
                        candidates.add(f"{grp}_2")
                    # try each candidate key to find a matching best enchant that exceeds threshold
                    found = None
                    for k in candidates:
                        s = ench_map.get(k)
                        if not s:
                            continue
                        best_e = s.get("best")
                        if best_e and float(best_e.get("pct", 0.0)) > bis_threshold and int(best_e.get("id")) == int(ench_id):
                            found = best_e
                            break
                    if found:
                        item["enchantment"]["is_bis"] = True
                        item["enchantment"]["bis_pct"] = float(found.get("pct", 0.0))
                # Gems: mark sockets only for gems exceeding the threshold
                gems_list = bis_summary.get("gems", []) or []
                top_gems = [g for g in gems_list if float(g.get("pct", 0.0)) > bis_threshold and g.get("id")]
                top_gem_ids = {int(g.get("id")) for g in top_gems}
                top_gem_map = {int(g.get("id")): g for g in top_gems}
                if item.get("socket") and isinstance(item.get("socket"), list):
                    for sock in item.get("socket"):
                        sid = sock.get("id") or sock.get("gem_item_id")
                        try:
                            sid_int = int(sid)
                        except Exception:
                            continue
                        if sid_int in top_gem_ids:
                            sock["is_bis"] = True
                            gem = top_gem_map.get(sid_int, {})
                            sock["bis_pct"] = float(gem.get("pct", 0.0))
                            sock["bis_count"] = int(gem.get("count", 0))
                            for idx, g in enumerate(top_gems):
                                if int(g.get("id")) == sid_int:
                                    sock["bis_rank"] = idx + 1
                                    break

                # Also annotate the global enchant_slots structure so the Enchantment Details
                # accordion can render BIS badges reliably (matches template checks).
                # Always only mark the single best enchant as BIS if it exceeds threshold.
                if enchant_slots and isinstance(enchant_slots, dict):
                    ench_map_all = bis_summary.get("enchants", {}) if bis_summary else {}
                    for e_slot_name, e_list in enchant_slots.items():
                        if not e_list:
                            continue
                        # group fallback (e.g., FINGER -> FINGER_1/FINGER_2)
                        group_name = e_slot_name.split("_")[0] if isinstance(e_slot_name, str) else e_slot_name
                        # try plural/singular variants and group-indexed keys
                        possible_keys = [e_slot_name, f"{e_slot_name}S", group_name, f"{group_name}S", f"{group_name}_1", f"{group_name}_2"]
                        for e in e_list:
                            eid = e.get("id")
                            if eid is None:
                                continue
                            marked = False
                            for k in possible_keys:
                                ssum = ench_map_all.get(k) if ench_map_all else None
                                if ssum and isinstance(ssum.get("details"), list) and len(ssum.get("details", [])) > 0:
                                    d = ssum.get("details", [])[0]
                                    if d and float(d.get("pct", 0.0)) > bis_threshold and int(d.get("id")) == int(eid):
                                        e["is_bis"] = True
                                        e["bis_pct"] = float(d.get("pct", 0.0))
                                        e["bis_count"] = int(d.get("count", 0))
                                        e["bis_rank"] = 1
                                        marked = True
                                        break
                                if marked:
                                    break


def main(template_path, output_dir, debug=False, spec=None):
    # local import: keeps PIL/matplotlib out of the import path for the many
    # modules that only need this file's helpers
    from image_generation.spec_overview import createSpecOverviewImg
    # Prepare Jinja2 environment
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
        extensions=["jinja2.ext.loopcontrols"],
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["iso_ts"] = format_iso_timestamp
    env.filters["upgrade_info"] = upgrade_info
    env.globals["stat_display_name"] = stat_display_name
    template = env.get_template(os.path.basename(template_path))

    # Load lookup tables
    enchant_lookup_all = load_json(os.path.join(LOOKUP_DIR, "enchantments.json"))
    talents_tree_data = load_json(os.path.join(LOOKUP_DIR, "talents.json"))
    talents_tree_data = strip_empty_talent_entries(talents_tree_data)
    tree_by_spec = {t.get("specId"): t for t in talents_tree_data if t.get("specId")}
    embellishment_lookup = load_json(os.path.join(LOOKUP_DIR, "embellishments.json"))
    missive_lookup = load_json(os.path.join(LOOKUP_DIR, "missives.json"))
    price_lookup = load_json(
        os.path.join(LOOKUP_DIR, "commodities", "eu.json")
    )  # this is temporarily just using eu prices
    bonus_lookup = load_json(os.path.join(LOOKUP_DIR, "bonuses.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    dungeon_lookup_slug = {}
    for id, value in dungeon_lookup.items():
        value["id"] = id
        dungeon_lookup_slug[value["slug"]] = value
    bonus_quality_lookup = load_json(os.path.join(LOOKUP_DIR, "bonus_quality_map.json"))
    formatted_price = {pid: format_buyout(price_lookup[pid]) for pid in price_lookup}
    enchant_lookup = {e["id"]: e for e in enchant_lookup_all}
    # Expansion the build renders against, so old-expansion enchants people still
    # have equipped are dropped from the gear lists (see is_enchant_relevant).
    current_expansion = commonUtils.current_expansion_id()
    socket_lookup = {
        e["itemId"]: e for e in enchant_lookup_all if e.get("slot") == "socket"
    }
    # Global gem/enchant catalog for the client-side analyzer. The per-spec
    # spec_meta only carries the single most-popular gem/enchant combo, so a
    # player running anything off that combo (or gear optimised for a different
    # spec) had no icon/name for their sockets and — worse — the analyzer linked
    # an enchant by its enchant_id as if it were an item id, resolving to an
    # unrelated item on Wowhead. This spec-independent index lets the analyzer
    # resolve any gem (by its item id) or enchant (by its SimC enchant_id) to the
    # real icon/name and, for enchants, the scroll item id used for the link.
    write_analyzer_gem_enchant_index(enchant_lookup_all)
    equippable_items = load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
    for item in equippable_items:
        if "stats" in item:
            processed_stats = []
            for s in sorted(item["stats"], key=lambda x: x.get("alloc", 0), reverse=True):
                stat_type = BLIZZARD_STAT_MAP.get(s["id"])
                if stat_type:
                    processed_stats.append({"type": stat_type, "alloc": s.get("alloc", 0)})
            item["stats"] = processed_stats

    item_lookup = {
        i["id"]: i for i in equippable_items
    }
    # item_id -> URL slug for linking to the dedicated item pages (/items/<slug>).
    # Derived from item names; matches the map generateItemPages.py builds.
    item_slug_map = build_item_slug_map(item_lookup)
    # item_id -> {source_dungeons, source_raids, crafted} for the loot-source
    # badges on each gear row. raids.json may be absent early season (mirrors the
    # item page's optional load); raid sources then simply won't appear.
    raids_path = os.path.join(LOOKUP_DIR, "raids.json")
    raids_json = load_json(raids_path) if os.path.exists(raids_path) else {}
    item_sources = build_item_source_map(item_lookup, dungeon_lookup, raids_json)
    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)
    crafting_all = load_json(os.path.join(LOOKUP_DIR, "crafting.json"))
    reagent_lookup = {r["id"]: r for r in crafting_all.get("reagents", [])}
    # Normalize reagent stats so templates can rely on `stat.type` and `stat.amount`
    for _rid, _r in reagent_lookup.items():
        stats = _r.get("stats")
        if not stats or not isinstance(stats, list):
            continue
        normalized = []
        for s in stats:
            # If already in desired shape, keep it
            if isinstance(s, dict) and s.get("type") and (s.get("amount") is not None):
                normalized.append({"type": s.get("type"), "amount": s.get("amount")})
                continue
            # Support legacy shapes coming from crafting.json: { "id": <stat_id>, "alloc": <value> }
            stat_id = s.get("id") if isinstance(s, dict) else None
            stat_amount = None
            if isinstance(s, dict):
                stat_amount = s.get("amount") if s.get("amount") is not None else s.get("alloc")
            # Map numeric blizzard stat id to short name when possible
            stat_type = BLIZZARD_STAT_MAP.get(stat_id) if stat_id is not None else None
            if stat_type and stat_amount is not None:
                normalized.append({"type": stat_type, "amount": stat_amount})
        # only replace if we actually found normalized entries
        if normalized:
            _r["stats"] = normalized
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    season_info = load_season_info(LOOKUP_DIR)
    os.makedirs(output_dir, exist_ok=True)

    # Merged id -> item metadata for the Top Trends bar so its gem / embellishment
    # / crafted / *_combo feeds resolve icons the same lookups the page uses:
    # gear + tier-set pieces (item_lookup), gems (socket_lookup) and embellishment/
    # crafted reagents (reagent_lookup). Keyed by str(id); gear wins on collision.
    trend_item_icons = {}
    for _src in (reagent_lookup, socket_lookup, item_lookup):
        for _iid, _meta in _src.items():
            trend_item_icons[str(_iid)] = _meta

    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)

    current_season_id = int(season_info["blizzard_season_id"])
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Current season ID: {current_season_id}"
    )

    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))

    # if only single page should be rendered set spec_keys to just that one spec
    if spec:
        spec_keys = [spec]
    else:
        spec_keys = list(spec_lookup.keys())

    # Season-constant data, identical for every spec: fetch once instead of
    # once per spec (the per-spec top-comps query alone is a full table scan
    # because FIND_IN_SET can't use an index).
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        print(f"[{datetime.now(timezone.utc).isoformat()}] fetching total season runs...")
        total_runs = databaseConnector.fetch_total_season_runs(
            conn, cursor, current_season_id
        )
        print(f"[{datetime.now(timezone.utc).isoformat()}] fetching per-spec upgrade distributions...")
        spec_upgrades_all = databaseConnector.fetch_spec_upgrades(conn, cursor)
        # Single highest key completed this season across every class/spec. Used
        # by the spec page's "how this data is collected" blurb as a real example
        # of the per-dungeon top-key window (season-constant, so fetched once).
        season_max_key_run = databaseConnector.fetch_max_key_run(
            conn, cursor, current_season_id
        )
        season_max_key = (
            season_max_key_run.get("keystone_level") if season_max_key_run else None
        )

        # Real dungeon names for the "how this data is collected" blurb: the
        # dungeon that holds the season's single hardest key, and the dungeon
        # with the LOWEST top key (the "quietest" one), so the blurb can point
        # at concrete examples instead of "that dungeon" / "quieter dungeons".
        # Season-constant, so computed once here.
        def _dungeon_name(dungeon_id):
            d = dungeon_lookup.get(str(dungeon_id))
            return (d.get("name") or {}).get("en_US") if d else None

        season_max_key_dungeon = None
        season_min_top_key = None
        season_min_top_key_dungeon = None
        if season_max_key_run and season_max_key_run.get("dungeon_id") is not None:
            season_max_key_dungeon = _dungeon_name(season_max_key_run["dungeon_id"])
        per_dungeon_top_key = {}
        for _r in databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, current_season_id
        ):
            if int(_r.get("total_runs", 0) or 0) <= 0:
                continue
            _did = str(_r["dungeon_id"])
            _lvl = int(_r["keystone_level"])
            if _lvl > per_dungeon_top_key.get(_did, 0):
                per_dungeon_top_key[_did] = _lvl
        if per_dungeon_top_key:
            _low_did, _low_lvl = min(
                per_dungeon_top_key.items(), key=lambda kv: kv[1]
            )
            # Only worth naming a "lower band" example when it really is lower.
            if season_max_key and _low_lvl < season_max_key:
                season_min_top_key = _low_lvl
                season_min_top_key_dungeon = _dungeon_name(_low_did)
        # Team-comp families (same clustering as the comps page) so each spec page can
        # show the popular team comps that spec belongs to, grouped with their flexible
        # alternates. One scan of aggregated_dungeon_comps, clustered once, indexed by spec.
        print(f"[{datetime.now(timezone.utc).isoformat()}] clustering team comps...")
        _collapsed_comps = compArchetypes.collapse_comps(
            databaseConnector.fetch_all_comps(conn, cursor, current_season_id),
            spec_lookup)
        _team_families = compArchetypes.build_archetypes(
            _collapsed_comps, spec_lookup, class_lookup, top_n=None)
        team_spec_comps = compArchetypes.spec_team_comps(
            _team_families, spec_lookup, class_lookup)

    # Iterate over each spec folder
    for spec_id in spec_keys:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Processing spec {spec_id}..."
        )
        if not os.path.exists(os.path.join(LOOKUP_DIR, "talents", f"{spec_id}.json")):
            print(f"No talent data for spec {spec_id}, skipping")
            return
        try:
            with closing(databaseConnector.get_connection()) as conn:
                cursor = conn.cursor()
                databaseConnector.configure_read_session(conn, cursor)

                spec_data = spec_lookup.get(spec_id, {})
                class_data = class_lookup.get(spec_data.get("classID", ""), {})

                talent_lookup = load_json(
                    os.path.join(LOOKUP_DIR, "talents", f"{spec_id}.json")
                )
                valid_talents = {int(tid) for tid in talent_lookup.get("talents", {})}
                valid_subtrees = {int(tid) for tid in talent_lookup.get("subTrees", {})}
                print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching talents...")
                spec_talent_rows = databaseConnector.fetch_spec_talents_differences(
                    conn, cursor, spec_id, current_season_id
                )
                # Per-hero-tree breakdowns so the talent overview can be shown
                # relative to a single hero tree (toggleable on the page).
                class_by_tree = aggregateData.get_class_talent_differences_by_hero_tree(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )
                spec_by_tree = aggregateData.get_spec_talent_differences_by_hero_tree(
                    conn, cursor, spec_id, current_season_id, valid_talents,
                    rows=spec_talent_rows,
                )
                hero_by_tree = aggregateData.get_hero_talent_differences_by_hero_tree(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )
                # Spec-wide (all hero trees pooled) class/spec pick rates, so the
                # analyzer's per-node badges show one spec-wide number for the
                # class + spec trees (hero nodes stay per-tree, from hero_by_tree).
                class_pop_all = aggregateData.get_class_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )
                spec_pop_all = aggregateData.get_spec_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents,
                    rows=spec_talent_rows,
                )
                hero_tree_difs = aggregateData.get_hero_tree_differences(
                    conn, cursor, spec_id, current_season_id, valid_subtrees
                )

                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching slots...")
                # Per-slot run totals: denominators for the gear-list noise filter.
                slot_totals = databaseConnector.fetch_slot_totals(
                    conn, cursor, spec_id, current_season_id
                )
                # Split slots into left/right/weapon/trinket
                left_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s, slot_totals)
                    for s in LEFT_ORDER
                ]
                right_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s, slot_totals)
                    for s in RIGHT_ORDER
                ]
                weapon_slots = filter_weapon_gear_entries(
                    [
                        fetch_slot_info(conn, cursor, spec_id, current_season_id, s, slot_totals)
                        for s in WEAPON_SLOTS
                    ],
                    slot_totals,
                )
                trinket_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s, slot_totals)
                    for s in TRINKET_SLOTS
                ]
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching routes...")
                top_routes = databaseConnector.fetch_top_routes_for_spec(
                    conn, cursor, spec_id
                )

                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching hero tree info..."
                )
                (
                    hero_trees,
                    popular_hero_tree,
                    popular_hero_tree_count,
                    hero_tree_count,
                ) = fetch_hero_tree_info(
                    conn, cursor, spec_id, current_season_id, valid_subtrees
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching spec sample size..."
                )
                spec_sample_size = databaseConnector.fetch_spec_sample_size(
                    conn, cursor, spec_id, current_season_id
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching enchants..."
                )
                enchant_slots, total_enchant_counts = fetch_enchant_info(
                    conn, cursor, spec_id, current_season_id, enchant_lookup, spec_sample_size,
                    current_expansion
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching missives..."
                )
                missives = databaseConnector.fetch_missive_count(
                    conn, cursor, spec_id, current_season_id
                )
                total_missive_count = sum(e[1] for e in missives)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching embellishments..."
                )
                embellishments = databaseConnector.fetch_embellishment_count(
                    conn, cursor, spec_id, current_season_id
                )
                total_embellishment_count = sum(e[1] for e in embellishments)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching crafted items..."
                )
                crafted_items = databaseConnector.fetch_crafted_items_count(
                    conn, cursor, spec_id, current_season_id
                )
                total_crafted_items_count = sum(e[1] for e in crafted_items)
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetched {total_crafted_items_count} crafted items")
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching comps..."
                )
                try:
                    embellishment_comps_raw = databaseConnector.fetch_embellishment_comps(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_embellishment_comps failed: {e}")
                    embellishment_comps_raw = []
                try:
                    crafted_comps_raw = databaseConnector.fetch_crafted_comps(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_crafted_comps failed: {e}")
                    crafted_comps_raw = []
                try:
                    tier_set_comps_raw = databaseConnector.fetch_tier_set_comps(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_tier_set_comps failed: {e}")
                    tier_set_comps_raw = []
                try:
                    gem_comps_raw = databaseConnector.fetch_gem_comps(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_gem_comps failed: {e}")
                    gem_comps_raw = []
                try:
                    enchant_comps_raw = databaseConnector.fetch_enchant_comps(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_enchant_comps failed: {e}")
                    enchant_comps_raw = []
                # SUM() comes back as decimal.Decimal from the connector
                total_embellishment_comps = sum(int(e[1] or 0) for e in embellishment_comps_raw)
                total_crafted_comps = sum(int(e[1] or 0) for e in crafted_comps_raw)
                total_tier_set_comps = sum(int(e[1] or 0) for e in tier_set_comps_raw)
                total_gem_comps = sum(int(e[1] or 0) for e in gem_comps_raw)
                total_enchant_comps = sum(int(e[1] or 0) for e in enchant_comps_raw)
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching socket limits...")
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching sockets...")
                sockets = aggregateData.get_sockets(
                    conn, cursor, spec_id, current_season_id
                )
                total_socket_count = sum(s.get("count", 0) for s in sockets)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching spec data count..."
                )
                data_count = databaseConnector.fetch_spec_data_count(
                    conn, cursor, spec_id
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching spec runs..."
                )
                spec_runs = databaseConnector.fetch_runs_per_spec(
                    conn, cursor, spec_id
                )

                # Filter embellishments to remove very rarely used entries.
                # Threshold against the 14-day gear sample, not season-wide
                # run counts (those outgrow the fixed-size sample and would
                # filter out everything for popular specs).
                # the overview image ranks best/worst against the unfiltered
                # list (its totals predate the rarity filter below)
                embellishments_unfiltered = embellishments
                embellishment_threshold = spec_sample_size * 0.001
                if embellishments and embellishment_threshold > 0:
                    filtered_embs = []
                    for e in embellishments:
                        # support tuple/list rows or dict rows
                        count = e[1] if isinstance(e, (list, tuple)) else (e.get('total_runs') or e.get('run_count') or 0)
                        if count >= embellishment_threshold:
                            filtered_embs.append(e)
                    embellishments = filtered_embs

                # Comp tables only cover the ~2 weeks of retained gear data, so
                # threshold against their own totals instead of season-wide
                # spec run counts (which would filter everything out).
                embellishment_comps = build_comps(
                    embellishment_comps_raw, total_embellishment_comps * 0.005,
                    item_lookup, "embellishment", spec_id, slot_sorted=False
                )
                crafted_comps = build_comps(
                    crafted_comps_raw, total_crafted_comps * 0.005,
                    item_lookup, "crafted", spec_id
                )
                tier_set_comps = build_comps(
                    tier_set_comps_raw, total_tier_set_comps * 0.005,
                    item_lookup, "tier set", spec_id
                )
                # Gems carry no slot: the comp string records which gems were
                # socketed, never which item they went into, so they keep the
                # canonical order. Enchants sort into gear-overview slot order.
                gem_comps = build_multiset_comps(
                    gem_comps_raw, socket_lookup, total_gem_comps * 0.005
                )
                enchant_comps = build_multiset_comps(
                    enchant_comps_raw, enchant_lookup, total_enchant_comps * 0.005,
                    slot_rank=enchant_slot_pos
                )

                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching loadout...")
                loadouts = aggregateData.get_loadout(
                    conn, cursor, spec_id, current_season_id
                )
                # Per-dungeon export strings for the Talent Differences modal's
                # "copy full build" button.
                dungeon_loadouts = aggregateData.get_loadout_per_dungeon(
                    conn, cursor, spec_id, current_season_id
                )
                # Verified loadouts of the top 50 players (meta +
                # items/gems/enchants/talents), one per dungeon each.
                try:
                    top50_raw = databaseConnector.fetch_top50_loadouts(
                        conn, cursor, spec_id, current_season_id, limit=50
                    )
                except Exception as e:
                    print(f"Warning: fetch_top50_loadouts failed: {e}")
                    top50_raw = []

                # hero node -> hero tree, so the loadouts can be split by the
                # hero tree they run (per-tree per-dungeon talent deviations)
                tree_nodes = tree_by_spec.get(int(spec_id), {})
                # Per-node meta pick rate for the analyzer tree badges. Class/spec
                # nodes use the spec-wide rate; hero nodes use the rate within
                # their own tree. build_ui_tree gives the same freeNode=100 math
                # the spec page renders, so both pages agree on the number.
                node_pct = {}
                for _n in build_ui_tree(
                    tree_nodes.get("classNodes", []), class_pop_all
                )["nodes"]:
                    node_pct[int(_n["id"])] = int(round(_n["pct_val"]))
                for _n in build_ui_tree(
                    tree_nodes.get("specNodes", []), spec_pop_all
                )["nodes"]:
                    node_pct[int(_n["id"])] = int(round(_n["pct_val"]))
                for _tid, _hero_pop in hero_by_tree.items():
                    for _n in build_ui_tree(
                        tree_nodes.get("heroNodes", []), _hero_pop,
                        is_hero=True, pop_hero_tree_id=_tid,
                    )["nodes"]:
                        node_pct[int(_n["id"])] = int(round(_n["pct_val"]))
                hero_node_subtree = {
                    int(hn["id"]): int(hn["subTreeId"])
                    for hn in tree_nodes.get("heroNodes", [])
                    if "id" in hn and hn.get("subTreeId") is not None
                }

                # Universe of crafted item ids for this spec (build-time stand-in
                # for the DB crafted_item_ids registry): the item ids the general
                # crafted-items aggregation surfaced.
                crafted_item_id_set = {
                    int(e[0]) for e in (crafted_items or []) if e and e[0] is not None
                }
                bis_summary = compute_bis_from_top_loadouts(
                    top50_raw,
                    item_lookup=item_lookup,
                    missive_lookup=missive_lookup,
                    embellishment_lookup=embellishment_lookup,
                    crafted_item_ids=crafted_item_id_set,
                    hero_node_subtree=hero_node_subtree,
                )
                print(f"[{datetime.now(timezone.utc).isoformat()}] BIS summary from top loadouts: {bis_summary}")

                # Annotate the general combo lists with the top-50 ("TOP") signal:
                # a comp gets is_bis/bis_pct when the top-50 players run the same
                # canonical id-set (same key the DB builds) in more than
                # BIS_PCT_THRESHOLD% of their loadouts -- the same gate the
                # gear-slot BIS badges use, so combo badges aren't more liberal.
                def _annotate_comps(comps, summary_section, multiset=False):
                    by_key = (summary_section or {}).get("by_key", {})
                    if not by_key:
                        return
                    for comp in comps or []:
                        if multiset:
                            ids = []
                            for e in comp.get("entries", []):
                                ids.extend([int(e["id"])] * int(e.get("qty", 1)))
                        else:
                            ids = [int(i) for i in comp.get("ids", [])]
                        hit = by_key.get(",".join(str(i) for i in sorted(ids)))
                        if hit and hit["pct"] > BIS_PCT_THRESHOLD:
                            comp["is_bis"] = True
                            comp["bis_pct"] = hit["pct"]

                _annotate_comps(embellishment_comps, bis_summary.get("embellishment_comps"))
                _annotate_comps(crafted_comps, bis_summary.get("crafted_comps"))
                _annotate_comps(tier_set_comps, bis_summary.get("tier_set_comps"))
                _annotate_comps(gem_comps, bis_summary.get("gem_comps"), multiset=True)
                _annotate_comps(enchant_comps, bis_summary.get("enchant_comps"), multiset=True)

                # Detail-list TOP maps (item_id -> top-50 pct). The missive /
                # embellishment / crafted single-item rows are tuples, so the
                # template receives side maps instead of an annotation.
                def _bis_map(summary_section):
                    return {
                        int(k): v["pct"]
                        for k, v in (summary_section or {}).get("by_key", {}).items()
                        if v["pct"] > BIS_PCT_THRESHOLD
                    }

                missive_bis = _bis_map(bis_summary.get("missives"))
                embellishment_bis = _bis_map(bis_summary.get("embellishments"))
                crafted_bis = _bis_map(bis_summary.get("crafted_items"))

                # SimulationCraft per-slot BiS (sim-derived highlight, parallel to the
                # frequency-based "TOP" highlight above). slot -> ranked candidate list.
                try:
                    simc_bis = databaseConnector.fetch_simc_bis(
                        conn, cursor, spec_id, current_season_id
                    )
                except Exception as e:
                    print(f"Warning: fetch_simc_bis failed: {e}")
                    simc_bis = {}
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching highest run..."
                )
                highest_run = databaseConnector.fetch_max_key_run_per_spec(
                    conn, cursor, spec_id, current_season_id
                )

                print(f"[{datetime.now(timezone.utc).isoformat()}] converting slots...")
                convert_slots(
                    conn,
                    cursor,
                    spec_id,
                    current_season_id,
                    left_slots + right_slots + weapon_slots + trinket_slots,
                    item_lookup,
                    bonus_lookup,
                    missive_lookup,
                    embellishment_lookup,
                    bonus_quality_lookup,
                    sockets,
                    socket_lookup,
                    enchant_slots,
                    set_members,
                    missives,
                    embellishments,
                    bis_summary=bis_summary,
                    simc_bis=simc_bis,
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] normalizing slots..."
                )
                left_slots = normalize_slot_collections(left_slots, LEFT_ORDER)
                right_slots = normalize_slot_collections(right_slots, RIGHT_ORDER)
                weapon_slots = normalize_slot_collections(weapon_slots, WEAPON_SLOTS)
                trinket_slots = normalize_slot_collections(trinket_slots, TRINKET_SLOTS)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] adjusting weapon slots..."
                )
                # remove offhand if 2 hander is equipped
                mh = next((g for g in weapon_slots if g["slot"] == "MAIN_HAND"), None)
                oh = next((g for g in weapon_slots if g["slot"] == "OFF_HAND"), None)
                if mh and mh["entries"] and len(mh["entries"]) > 0:
                    mh_item_id = mh["entries"][0]["id"]
                    # look up its inventoryType; two‑handers are 17 and ranged weapons are 15
                    print(f"Checking MAIN_HAND item {mh_item_id} for two‑hander or ranged type to determine if OFF_HAND slot should be removed")
                    print(f"MAIN_HAND item {mh_item_id} inventoryType: {item_lookup.get(mh_item_id, {}).get('inventoryType')}, itemSubClass: {item_lookup.get(mh_item_id, {}).get('itemSubClass')}")
                    # Passing spec_id keeps Titan's Grip Fury's off-hand: it
                    # wields a two-hander in that hand too (DUAL_WIELD_TWOHAND_SPECS).
                    if occupies_both_hands(item_lookup.get(mh_item_id), spec_id):
                        # always build combined list (falls back to just mh entries if oh is None)
                        combined = mh["entries"] + (oh.get("entries", []) if oh else [])
                        # re‑sort + trim to top 10
                        mh["entries"] = combined
                        # if there was an Off Hand slot, drop it entirely
                        if oh:
                            weapon_slots = [
                                g for g in weapon_slots if g["slot"] != "OFF_HAND"
                            ]
                # Annotate the global sockets list with BIS flags (so Gem Details can read it directly)
                try:
                    if bis_summary and isinstance(bis_summary, dict) and bis_summary.get("gems") and sockets:
                        gems_list = bis_summary.get("gems", []) or []
                        top_two = [g for g in gems_list[:2] if g.get("id")]
                        top_two_ids = {int(g.get("id")) for g in top_two}
                        top_two_map = {int(g.get("id")): g for g in top_two}
                        for s in sockets:
                            sid = s.get("id") or s.get("gem_item_id") or s.get("itemId")
                            try:
                                sid_int = int(sid)
                            except Exception:
                                continue
                            if sid_int in top_two_ids:
                                s["is_bis"] = True
                                gem = top_two_map.get(sid_int, {})
                                s["bis_pct"] = float(gem.get("pct", 0.0))
                                s["bis_count"] = int(gem.get("count", 0))
                                for idx, g in enumerate(top_two):
                                    if int(g.get("id")) == sid_int:
                                        s["bis_rank"] = idx + 1
                                        break
                except Exception:
                    pass
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching upgrade counts..."
                )
                upgrade_counts = databaseConnector.fetch_spec_upgrade(
                    conn, cursor, spec_id
                )
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching stats...")
                stat_priority, tertiary_priority, health_priority = fetch_stat_info(
                    conn, cursor, spec_id, current_season_id, spec_lookup
                )
                # Top team-comp families this spec is actually played in, with the spec
                # swapped into the shown comp. If it belongs to no family, fall back to the
                # most popular raw comps that include it so the panel is never empty.
                team_comp_families = team_spec_comps.get(int(spec_id), [])
                if not team_comp_families:
                    team_comp_families = compArchetypes.top_comps_with_spec(
                        _collapsed_comps, int(spec_id), spec_lookup, class_lookup)

            if not tree_by_spec.get(int(spec_id)):
                raise ValueError(f"No talent tree data for spec {spec_id}")

            # Build one talent-overview variant per hero tree (most popular
            # first, which becomes the default shown). Each variant carries its
            # own class/spec/hero UI trees, per-dungeon deviations, and loadout
            # string so the page can switch the whole overview client-side.
            sub_trees = talent_lookup.get("subTrees", {})

            # Top-50 verified-player usage, used for the talent "TOP" highlight.
            # `top_pct_map` drives per-node elite-vs-popular divergence; the hero
            # counts below pick the hero tree the top players actually run.
            top_pct_map = bis_summary.get("talent_node_pct", {}) if bis_summary else {}
            # Per-choice top-50 usage, so choice nodes can highlight which option
            # the elite players actually pick (drives the per-choice TOP badge).
            top_entry_pct_map = bis_summary.get("talent_node_entry_pct", {}) if bis_summary else {}

            # Raw per-dungeon talent counts from the verified loadouts, split by
            # hero tree; the Talent Differences modal is built purely from these.
            top_tree_talent_stats = (
                bis_summary.get("talent_dungeon_stats", {}) if bis_summary else {}
            )

            # Which hero tree each verified loadout runs (counted per tree).
            hero_tree_top_counts = (
                bis_summary.get("hero_tree_loadout_counts", {}) if bis_summary else {}
            )
            n_top = len(top50_raw) if top50_raw else 0
            hero_tree_top_pct = {
                st: (cnt / n_top * 100.0) if n_top else 0.0
                for st, cnt in hero_tree_top_counts.items()
            }
            top_hero_tree = (
                max(hero_tree_top_counts.items(), key=lambda x: x[1])[0]
                if hero_tree_top_counts else None
            )
            top_hero_tree_name = (
                sub_trees.get(str(top_hero_tree), {}).get("name")
                if top_hero_tree is not None else None
            )
            top_hero_tree_pct = (
                hero_tree_top_pct.get(top_hero_tree, 0.0)
                if top_hero_tree is not None else 0.0
            )

            # Node id -> talent tree, so the modal can split the flat node list
            # of a verified loadout into its Class / Spec / Hero sections.
            # Intersected with `valid_talents`: verified loadouts also list nodes
            # the per-spec talent lookup drops (free/automatic nodes, hero-tree
            # selection nodes), which the modal has no name or icon for.
            class_node_ids = {
                int(n["id"]) for n in tree_nodes.get("classNodes", []) if n.get("id")
            } & valid_talents
            spec_node_ids = {
                int(n["id"]) for n in tree_nodes.get("specNodes", []) if n.get("id")
            } & valid_talents
            hero_node_ids_by_tree = defaultdict(set)
            for _nid, _subtree in hero_node_subtree.items():
                if _nid in valid_talents:
                    hero_node_ids_by_tree[_subtree].add(_nid)

            def top_dungeon_difs(hero_tree_id, node_ids):
                """Per-dungeon talent swaps for one hero tree, top-50 only.

                One ranked pair of lists per dungeon rather than one per talent
                tree: which tree a swapped talent sits in tells a reader nothing
                they cannot see from the icon, and splitting by tree turned one
                short answer into six mostly-empty panels.
                """
                return aggregateData.dungeon_talent_deviations_from_top(
                    top_tree_talent_stats.get(int(hero_tree_id), {}),
                    node_ids=node_ids,
                    top_n=TALENT_DIFF_TOP_N,
                    min_loadouts=TALENT_DIFF_MIN_DUNGEON_LOADOUTS,
                    min_pct_points=TALENT_DIFF_MIN_PCT_POINTS,
                    recommend_min_pct=TALENT_DIFF_RECOMMEND_MIN_PCT,
                    drop_max_pct=TALENT_DIFF_DROP_MAX_PCT,
                )

            def top_dungeon_tree_usage(hero_tree_id, node_ids):
                """Per-dungeon node adoption for the modal's talent tree.

                {"<dungeon_id>": {"total": loadouts, "nodes": {node_id: [pct, avg_rank]}}}
                The page ships this as JSON and repaints a clone of the static
                talent tree with it -- rendering a tree per dungeon server-side
                would add megabytes of markup to every spec page.
                """
                stats = top_tree_talent_stats.get(int(hero_tree_id), {})
                usage = {}
                for dungeon, dungeon_stats in (stats.get("dungeons") or {}).items():
                    total = int(dungeon_stats.get("total", 0) or 0)
                    if total < TALENT_DIFF_MIN_DUNGEON_LOADOUTS:
                        continue
                    ranks = dungeon_stats.get("ranks") or {}
                    nodes = {}
                    for nid, count in (dungeon_stats.get("nodes") or {}).items():
                        if int(nid) not in node_ids or not count:
                            continue
                        nodes[str(nid)] = [
                            round(count / total * 100.0, 1),
                            round(int(ranks.get(nid, count)) / count, 2),
                        ]
                    if not nodes:
                        continue
                    # share of this dungeon's verified loadouts on this tree, so
                    # the cloned tree's hero badge is per-dungeon too
                    dungeon_all_trees = sum(
                        int(((s.get("dungeons") or {}).get(dungeon) or {}).get("total", 0) or 0)
                        for s in top_tree_talent_stats.values()
                    )
                    usage[str(dungeon)] = {
                        "total": total,
                        "tree_pct": round(total / dungeon_all_trees * 100.0, 1)
                        if dungeon_all_trees
                        else 0.0,
                        "nodes": nodes,
                    }
                return usage

            def dungeon_build_codes(hero_tree_id):
                """Most-run full build per dungeon for this hero tree."""
                codes = {}
                for dungeon, by_tree in (dungeon_loadouts or {}).items():
                    entry = by_tree.get(int(hero_tree_id))
                    code = escape_raidbot_code((entry or {}).get("loadout"))
                    if code:
                        codes[str(dungeon)] = {**code, "count": entry.get("count", 0)}
                return codes

            # Hero-tree preference per dungeon, from the same verified loadouts:
            # {dungeon: [{id, name, icon, dungeon_pct, overall_pct, diff}]}, only
            # where the shift is big enough to mean something, biggest first.
            hero_tree_shifts = defaultdict(list)
            top_total = sum(hero_tree_top_counts.values())
            for _tid, _stats in top_tree_talent_stats.items():
                _sub = sub_trees.get(str(_tid), {})
                if not _sub or not top_total:
                    continue
                _overall_pct = hero_tree_top_counts.get(_tid, 0) / top_total * 100.0
                for _dungeon, _dstats in (_stats.get("dungeons") or {}).items():
                    _dungeon_total = sum(
                        int(((s.get("dungeons") or {}).get(_dungeon) or {}).get("total", 0) or 0)
                        for s in top_tree_talent_stats.values()
                    )
                    if _dungeon_total < TALENT_DIFF_MIN_DUNGEON_LOADOUTS:
                        continue
                    _dungeon_pct = int(_dstats.get("total", 0) or 0) / _dungeon_total * 100.0
                    _diff = _dungeon_pct - _overall_pct
                    if abs(_diff) < HERO_TREE_DIFF_MIN_PCT_POINTS:
                        continue
                    hero_tree_shifts[str(_dungeon)].append({
                        "id": _tid,
                        "name": _sub.get("name"),
                        "icon": _sub.get("icon"),
                        "dungeon_pct": _dungeon_pct,
                        "overall_pct": _overall_pct,
                        "diff": _diff,
                    })
            for _rows in hero_tree_shifts.values():
                _rows.sort(key=lambda r: r["diff"], reverse=True)

            hero_variants = []
            for ht in sorted(
                hero_trees, key=lambda t: t.get("count", 0), reverse=True
            ):
                tid = ht["id"]
                sub = sub_trees.get(str(tid), {})
                # every node this variant can render: class + spec + its own tree
                variant_node_ids = (
                    class_node_ids
                    | spec_node_ids
                    | hero_node_ids_by_tree.get(int(tid), set())
                )
                ui_class_tree = build_ui_tree(
                    tree_nodes.get("classNodes", []), class_by_tree.get(tid, {}),
                    top_pct_map=top_pct_map, top_entry_pct_map=top_entry_pct_map,
                )
                ui_spec_tree = build_ui_tree(
                    tree_nodes.get("specNodes", []), spec_by_tree.get(tid, {}),
                    top_pct_map=top_pct_map, top_entry_pct_map=top_entry_pct_map,
                )
                ui_hero_tree = build_ui_tree(
                    tree_nodes.get("heroNodes", []),
                    hero_by_tree.get(tid, {}),
                    is_hero=True,
                    pop_hero_tree_id=tid,
                    top_pct_map=top_pct_map, top_entry_pct_map=top_entry_pct_map,
                )
                hero_variants.append({
                    "id": tid,
                    "name": sub.get("name"),
                    "icon": sub.get("icon"),
                    "pct": (ht["count"] / hero_tree_count * 100) if hero_tree_count else 0,
                    "top_pct": hero_tree_top_pct.get(tid, 0.0),
                    "is_top": tid == top_hero_tree,
                    "is_default": tid == popular_hero_tree,
                    "ui_class_tree": ui_class_tree,
                    "ui_spec_tree": ui_spec_tree,
                    "ui_hero_tree": ui_hero_tree,
                    "loadout_code": escape_raidbot_code(
                        loadouts.get(tid, {}).get("loadout")
                    ),
                    # Per-dungeon talent swaps of the top players running THIS
                    # hero tree (its own hero nodes, plus class and spec).
                    "talent_difs": top_dungeon_difs(tid, variant_node_ids),
                    # Full per-dungeon picture behind those swaps: node adoption
                    # for the tree view, plus the dungeon's most-run build.
                    "dungeon_tree_usage": top_dungeon_tree_usage(tid, variant_node_ids),
                    "dungeon_loadouts": dungeon_build_codes(tid),
                })

            # One concrete "players swap this in for dungeon X" example for the
            # Talents intro. We pick the single biggest per-dungeon adoption jump
            # ("take") from the default hero tree's swaps, so the guide text can
            # name a real dungeon + talent instead of talking in the abstract.
            # None when the spec has no notable swaps (genuinely low-data specs);
            # the template falls back to a generic line.
            talent_swap_highlight = None
            _default_variant = next(
                (v for v in hero_variants if v.get("is_default")),
                hero_variants[0] if hero_variants else None,
            )
            if _default_variant:
                _best = None  # (pct_point_diff, dungeon_id, gain_item)
                for _dungeon_id, _difs in (_default_variant.get("talent_difs") or {}).items():
                    for _gain in (_difs.get("gains") or []):
                        _diff = _gain.get("pct_point_diff", 0) or 0
                        if _best is None or _diff > _best[0]:
                            _best = (_diff, _dungeon_id, _gain)
                if _best:
                    _, _dungeon_id, _gain = _best
                    _talent = talent_lookup.get("talents", {}).get(str(_gain["talent_id"]))
                    _dungeon = dungeon_lookup.get(str(_dungeon_id)) or dungeon_lookup.get(_dungeon_id)
                    if _talent and _dungeon:
                        talent_swap_highlight = {
                            "dungeon_name": (_dungeon.get("name") or {}).get("en_US"),
                            "talent_name": _talent.get("name"),
                            "spellId": _talent.get("spellId"),
                        }

            print(f"[{datetime.now(timezone.utc).isoformat()}] generating page...")
            # Build per-key-level stats for this spec to render stacked success chart
            # (spec_upgrades_all is season-constant, fetched once before the loop)
            try:
                level_stats = [
                    {
                        "keystone_level": int(r["keystone_level"]),
                        "upgrade_3": int(r.get("upgrade_3", 0)),
                        "upgrade_2": int(r.get("upgrade_2", 0)),
                        "upgrade_1": int(r.get("upgrade_1", 0)),
                        "depleted": int(r.get("depleted", 0)),
                        "total_runs": int(r.get("total_runs", 0)),
                    }
                    for r in spec_upgrades_all
                    if int(r.get("spec_id", -1)) == int(spec_id)
                ]
                level_stats.sort(key=lambda e: e["keystone_level"]) if level_stats else None
                if level_stats:
                    overall_stats = {
                        "total_runs": sum(e.get("total_runs", 0) for e in level_stats),
                        "upgrade_3": sum(e.get("upgrade_3", 0) for e in level_stats),
                        "upgrade_2": sum(e.get("upgrade_2", 0) for e in level_stats),
                        "upgrade_1": sum(e.get("upgrade_1", 0) for e in level_stats),
                        "depleted": sum(e.get("depleted", 0) for e in level_stats),
                    }
                else:
                    overall_stats = None
            except Exception:
                level_stats = []
                overall_stats = None


            # Machine-readable meta snapshot for the client-side "Am I meta?"
            # analyzer. Reuses data already assembled above; writes one small
            # JSON per spec that analyzer.js fetches by spec_id.
            spec_meta = build_spec_meta_json(
                spec_id, spec_data, class_data,
                left_slots, right_slots, weapon_slots, trinket_slots,
                enchant_slots, enchant_lookup, item_lookup, item_slug_map,
                bis_summary, socket_lookup,
                talent_lookup=talent_lookup, loadouts=loadouts, node_pct=node_pct,
            )
            spec_meta_dir = os.path.join("assets", "json", "spec_meta")
            os.makedirs(spec_meta_dir, exist_ok=True)
            with open(os.path.join(spec_meta_dir, f"{spec_id}.json"), "w", encoding="utf-8") as f:
                json.dump(spec_meta, f, separators=(",", ":"))

            # Merge the per-spec talent + subtree name/icon maps so the trends
            # bar can label talent movers (keys match aggregated_*_talent.talent_id).
            talent_name_map = dict(talent_lookup.get("talents", {}))
            talent_name_map.update(talent_lookup.get("subTrees", {}))
            # Some aggregated talent_ids are NODE ids (single-entry nodes) rather than
            # the entry ids that key `talents`/`subTrees`; those movers would resolve to
            # no icon and drop out of the bar. Backfill node-id -> {name, icon, spellId}
            # from the node's chosen entry so every talent mover renders its real icon.
            # Entry-keyed wins on conflict (only fill ids not already present).
            for node_id, node in (talent_lookup.get("nodes") or {}).items():
                key = str(node_id)
                if key in talent_name_map or not isinstance(node, dict):
                    continue
                entries = node.get("entries") or []
                if entries:
                    e = entries[0]
                    talent_name_map[key] = {
                        "name": e.get("name") or node.get("name"),
                        "icon": e.get("icon"),
                        "spellId": e.get("spellId"),
                    }
            # We're past the per-spec read block above, which already released its
            # pooled connection (the read -> release -> heavy-work pattern), so the
            # `conn` here is dead. Grab a fresh live connection just for the trends
            # lookup (the pool slot is free at this point).
            with closing(databaseConnector.get_connection()) as trends_conn:
                trends_cursor = trends_conn.cursor()
                databaseConnector.configure_read_session(trends_conn, trends_cursor)
                trends = build_trends(
                    trends_conn,
                    trends_cursor,
                    trend_feeds_for_spec(spec_id),
                    {
                        "specs": spec_lookup,
                        "items": trend_item_icons,
                        "talents": talent_name_map,
                    },
                )

            output_html = template.render(
                generated_at=datetime.now(timezone.utc).timestamp(),
                spec_id=spec_id,
                spec=spec_data,
                trends=trends,
                class_info=class_data,
                data_count=data_count,
                active_page="spec",
                spec_nav=spec_nav,
                dungeon_nav=dungeon_nav,
                summary_data={"count": spec_runs, "upgrade_counts": upgrade_counts},
                total_enchant_counts=total_enchant_counts,
                total_socket_count=total_socket_count,
                total_embellishment_count=total_embellishment_count,
                total_missive_count=total_missive_count,
                total_season_runs=total_runs,
                left_slots=left_slots,
                right_slots=right_slots,
                weapon_slots=weapon_slots,
                trinket_slots=trinket_slots,
                enchant_slots=enchant_slots,
                hero_trees=hero_trees,
                enchant_lookup=enchant_lookup,
                embellishment_lookup=embellishment_lookup,
                missive_lookup=missive_lookup,
                level_stats=level_stats,
                overall_stats=overall_stats,
                socket_lookup=socket_lookup,
                spec_lookup=spec_lookup,
                item_lookup=item_lookup,
                item_slug_map=item_slug_map,
                item_sources=item_sources,
                notifications=notifications,
                reagent_lookup=reagent_lookup,
                dungeon_lookup=dungeon_lookup,
                dungeon_lookup_slug=dungeon_lookup_slug,
                role=ROLE_FOLDERS[spec_data.get("role", 2)],
                talent_lookup=talent_lookup,
                bis_summary=bis_summary,
                current_spec=f"{spec_data['name']} {class_data.get('name')}",
                sockets=sockets,
                embellishments=embellishments,
                crafted_items=crafted_items,
                total_crafted_items=total_crafted_items_count,
                embellishment_comps=embellishment_comps,
                total_embellishment_comps=total_embellishment_comps,
                crafted_comps=crafted_comps,
                total_crafted_comps=total_crafted_comps,
                tier_set_comps=tier_set_comps,
                total_tier_set_comps=total_tier_set_comps,
                gem_comps=gem_comps,
                total_gem_comps=total_gem_comps,
                enchant_comps=enchant_comps,
                total_enchant_comps=total_enchant_comps,
                missives=missives,
                missive_bis=missive_bis,
                embellishment_bis=embellishment_bis,
                crafted_bis=crafted_bis,
                formatted_price=formatted_price,
                trending=spec_runs / total_runs if total_runs > 0 else 0,
                highest_run=highest_run,
                key_level_window=KEY_LEVEL_WINDOW,
                season_max_key=season_max_key,
                season_max_key_dungeon=season_max_key_dungeon,
                season_min_top_key=season_min_top_key,
                season_min_top_key_dungeon=season_min_top_key_dungeon,
                hero_variants=hero_variants,
                talent_swap_highlight=talent_swap_highlight,
                top_hero_tree_id=top_hero_tree,
                top_hero_tree_name=top_hero_tree_name,
                top_hero_tree_pct=top_hero_tree_pct,
                talent_dif_min_pct_points=TALENT_DIFF_MIN_PCT_POINTS,
                hero_tree_shifts=hero_tree_shifts,
                tree_data=tree_by_spec.get(int(spec_id)),
                hero_tree_difs=hero_tree_difs,
                hero_tree_count=hero_tree_count,
                top_routes=top_routes,
                team_comp_families=team_comp_families,
                season_info=season_info,
                stats=stat_priority,
                secondary_stats=SECONDARY_STATS,
                tertiary_priority=tertiary_priority,
                health_priority=health_priority,
                spec_runs=spec_runs,
                breadcrumbs=[
                    {"title": "Classes"},
                    {
                        "title": ROLE_FOLDERS[spec_data.get("role", 2)],
                        "href": f"/pages/search?q={ROLE_FOLDERS[spec_data.get('role', 2)]}",
                    },
                    {
                        "title": f"{spec_data.get('name')} {class_data.get('name')}",
                        "href": f"/Classes/{ROLE_FOLDERS[spec_data.get('role', 2)]}/{spec_data.get('name')}_{class_data.get('name')}",
                    },
                ],
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] saving page...")
            # Write output
            out_path = os.path.join(
                output_dir,
                ROLE_FOLDERS[spec_data.get("role", 2)],
                f"{spec_data.get('name')}_{class_data.get('name')}.html",
            )
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(output_html)
            print(f"[{datetime.now(timezone.utc).isoformat()}] Generated {out_path}")
            print(f"[{datetime.now(timezone.utc).isoformat()}] creating overview image...")
            spec_slug = f"{spec_data.get('name')}_{class_data.get('name')}"
            preview_path = os.path.join("assets", "img", "previews",  f"{spec_slug}.png")
            os.makedirs(os.path.dirname(preview_path), exist_ok=True)
            # pass the already-fetched data so the image step doesn't re-run
            # the same queries (incl. the expensive max-key-run join) on a
            # second connection
            createSpecOverviewImg(
                'tmp', preview_path, spec_id, current_season_id,
                spec_upgrade_counts=upgrade_counts,
                hero_trees=[
                    {"tree_id": h["id"], "count": h["count"]} for h in hero_trees
                ],
                highest=highest_run,
                missives=missives,
                embellishments=embellishments_unfiltered,
                sockets=sockets,
                stat_info=(stat_priority, tertiary_priority, health_priority),
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] Finished {spec_id}.")
            if debug:
                raise ValueError("Debug mode: stopping after first spec")
        except Exception as e:
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"Error processing spec {spec_id}: {type(e).__name__}: {e}"
            )
            traceback.print_exc()
            raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW M+ spec pages")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    parser.add_argument("--debug", required=False)
    parser.add_argument("--spec", required=False)

    args = parser.parse_args()

    # Pool size 2: createSpecOverviewImg holds a connection open while
    # get_run_data acquires a second one for the spec's highest run.
    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        2,
    )
    main(
        args.template,
        args.output_dir,
        args.debug,
        args.spec,
    )
