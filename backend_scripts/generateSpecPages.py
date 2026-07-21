import os
import json
import argparse
import traceback
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
import aggregateData
from simcBis import DUAL_WIELD_TWOHAND_SPECS
from collections import defaultdict
from datetime import datetime, timezone
from contextlib import closing
import re
from urllib.parse import quote_plus
from pageGeneration import ROLE_FOLDERS, generateSpecNav, generateDungeonNav, build_item_slug_map
# Re-exported for the many modules that import these from generateSpecPages;
# the implementations live in commonUtils so image_generation/social_posts can
# use them without importing this (jinja2-heavy) module.
from commonUtils import (
    LOOKUP_DIR,
    SECONDARY_STATS,
    TERTIARY_STATS,
    HEALTH_STATS,
    load_json,
    upgrade_info,
    humanize_number,
    format_duration,
    fetch_stat_info,
    stat_display_name,
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

# "TOP" highlight in the per-dungeon Talent Differences modal. That modal surfaces
# niche, dungeon-specific talents (e.g. a curse dispel only taken in a curse
# dungeon) which have low OVERALL usage, so the tree's divergence metric never
# flags them. Instead we badge a talent there when a majority of the top-50
# players took it IN THAT DUNGEON (per-dungeon adoption).
TALENT_DUNGEON_ELITE_MIN_PCT = 50.0

# Minimum top-50 usage share (%) for an item/gem/enchant/missive/embellishment or
# a combo to earn the gold "TOP" badge. Shared by the gear-slot BIS annotations
# and the combo/detail sections so every TOP badge on the page means the same
# thing: the top-50 players run it in more than this share of their loadouts.
BIS_PCT_THRESHOLD = 80.0

# Ranking blend for the Talent Differences modal: the score that decides which
# talents surface as the biggest per-dungeon gains/losses mixes the general
# population's relative change with the top-50 players' relative change. Top
# selections are weighted more heavily so the modal reflects what the best
# players actually swap per dungeon. Must sum to 1.0.
TALENT_DIFF_TOP_WEIGHT = 0.7
TALENT_DIFF_NORMAL_WEIGHT = 0.3

# Blizzard inventoryType -> display position matching the gear overview slot
# order (LEFT_ORDER + RIGHT_ORDER + WEAPON_SLOTS + TRINKET_SLOTS, columns
# flattened). Used to sort combo items the same way the overview lists slots.
INVTYPE_DISPLAY_ORDER = {
    1: 0,  # head
    2: 1,  # neck
    3: 2,  # shoulder
    16: 3,  # back
    5: 4,  # chest
    20: 4,  # robe (chest)
    9: 5,  # wrist
    10: 6,  # hands
    6: 7,  # waist
    7: 8,  # legs
    8: 9,  # feet
    11: 10,  # finger
    13: 11, 15: 11, 17: 11, 21: 11, 26: 11,  # main hand / two-hand / ranged
    14: 12, 22: 12, 23: 12,  # off hand / shield / held in off-hand
    12: 13,  # trinket
}

SLOT_GROUPS = [
    "BACK",
    "CHEST",
    "FEET",
    "FINGER",
    "HANDS",
    "HEAD",
    "LEGS",
    "WEAPON",
    "NECK",
    "SHOULDER",
    "TRINKET",
    "WAIST",
    "WRIST",
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

        # Choice nodes: node-level usage is ~100% for both populations, so the
        # rule above can never fire. Diverge per choice instead — flag the node
        # when the elite players' pick differs from the general population's.
        # Free choice nodes still qualify: the node is forced but the pick isn't.
        top_choice = None
        if n_type == "choice":
            diverging = [c for c in node_choices if c.get("is_top")]
            if diverging:
                top_choice = max(diverging, key=lambda c: c["top_pct"])
                is_top = True
                top_pct_val = top_choice["top_pct"]
            else:
                is_top = False

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




def build_spec_meta_json(
    spec_id, spec_data, class_data, stat_priority,
    left_slots, right_slots, weapon_slots, trinket_slots,
    enchant_slots, enchant_lookup, item_lookup, item_slug_map,
    bis_summary, socket_lookup,
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
    toward the score. Also carries the top enchant per slot group and the stat
    priority. Icons/quality are baked so the client can render item tiles.
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

    enchants = {}
    for slot_group, lst in (enchant_slots or {}).items():
        if not lst:
            continue
        top = lst[0]
        eid = top.get("id")
        ench = enchant_lookup.get(eid) or {}
        enchants[slot_group] = {
            "id": eid,
            # enchants store the readable label under itemName/displayName;
            # `name` is usually null for them. Tooltip links via the scroll itemId.
            "name": ench.get("itemName") or ench.get("displayName") or ench.get("name"),
            "icon": ench.get("itemIcon") or ench.get("icon") or ench.get("spellIcon"),
            "itemId": ench.get("itemId"),
            "quality": ench.get("quality"),
        }

    # Spec-wide top gems (Raider.io top-50 loadouts), enriched with display data
    # from socket_lookup (gems keyed by their item id). Gems aren't slot-specific
    # in the meta, so the analyzer scores them as one set for the whole build.
    gems = []
    for g in (bis_summary or {}).get("gems", []):
        gid = g.get("id")
        pct = g.get("pct") or 0
        if gid is None or pct < 5.0:  # drop the long noise tail
            continue
        sk = socket_lookup.get(gid) or socket_lookup.get(int(gid)) or {}
        gems.append({
            "id": gid,
            "name": sk.get("itemName"),
            "icon": sk.get("itemIcon"),
            "quality": sk.get("quality"),
            "pct": round(pct, 1),
        })
        if len(gems) >= 6:
            break

    return {
        "spec_id": int(spec_id),
        "spec": spec_data.get("name"),
        "class": class_data.get("name"),
        # secondary stat names in the same priority order the page displays.
        "stat_priority": [s.get("name") for s in (stat_priority or []) if s.get("name")],
        "slots": slots,
        "enchants": enchants,
        "gems": gems,
    }


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
):
    """Compute BIS summary from a list of top-player loadouts.

    Input: list of loadout dicts as returned by `databaseConnector.fetch_top50_loadouts`.
    Returns a dict with `items`, `enchants`, `gems`, `talents`, `full_loadout` summary.

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
        for t in lo.get("talents", []) or []:
            node = t.get("node_id") or t.get("id")
            if not node:
                continue
            talent_node_counts[int(node)] += 1
            if dungeon is not None:
                talent_dungeon_counts[int(dungeon)][int(node)] += 1
            entry_id = t.get("entry_id")
            spell_id = t.get("spell_id")
            if entry_id is not None or spell_id is not None:
                talent_entry_counts[int(node)][(entry_id, spell_id)] += 1

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

    # Per-dungeon adoption: {dungeon_id(str): {node_id(int): pct}} where pct is
    # the share of that dungeon's top-player loadouts that took the node.
    talent_pct_by_dungeon = {}
    for d, nodes in talent_dungeon_counts.items():
        total = dungeon_loadout_totals.get(d, 0)
        if total <= 0:
            continue
        talent_pct_by_dungeon[str(d)] = {
            int(nid): (int(cnt) / total) * 100.0 for nid, cnt in nodes.items()
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
        "talent_pct_by_dungeon": talent_pct_by_dungeon,
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
    conn, cursor, spec_id, current_season_id, enchant_lookup, spec_sample_size
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
                if enchant_id and enchant_lookup.get(enchant_id):
                    valid_enchants.append(enchant)
                    total_enchant_counts[slot_group] += enchant.get("count")
                else:
                    print(
                        f"Warning: enchant {enchant_id} (slot {slot_group}, count {enchant.get('count')}) not in enchantments.json for spec {spec_id} - skipping"
                    )
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
    spec_talents_difs=None,
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
                    if bonus_quality_lookup.get(str(bonus)):
                        item["quality_override"] = bonus_quality_lookup[str(bonus)]
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


def main(template_path, output_dir, CLIENT_ID, CLIENT_SECRET, debug=False, spec=None):
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
    socket_lookup = {
        e["itemId"]: e for e in enchant_lookup_all if e.get("slot") == "socket"
    }
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
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    os.makedirs(output_dir, exist_ok=True)

    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)

    access_token = aggregateData.get_access_token(CLIENT_ID, CLIENT_SECRET)
    current_season_id = aggregateData.get_current_season_id(access_token)
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
        print(f"[{datetime.now(timezone.utc).isoformat()}] fetching season-wide top comps...")
        top_comps_by_spec = databaseConnector.fetch_spec_top_comps_all(
            conn, cursor, current_season_id
        )

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
                # One fetch feeds both the overall and the per-hero-tree
                # spec-talent breakdowns (they run the identical query).
                spec_talent_rows = databaseConnector.fetch_spec_talents_differences(
                    conn, cursor, spec_id, current_season_id
                )
                # spec_talents_difs is still threaded into convert_slots below.
                spec_talents_full = aggregateData.get_spec_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents,
                    rows=spec_talent_rows,
                )
                spec_talents_difs = aggregateData.biggest_deviations_per_dungeon(spec_talents_full)
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
                    conn, cursor, spec_id, current_season_id, enchant_lookup, spec_sample_size
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

                # Filter rare comps and parse the comp strings into id lists
                def build_comps(raw_rows, threshold, limit=10):
                    comps = []
                    for row in raw_rows:
                        count = int(row[1] or 0)
                        if threshold > 0 and count < threshold:
                            continue
                        try:
                            ids = [int(i) for i in str(row[0]).split(",") if i]
                        except (ValueError, TypeError):
                            continue
                        # comp strings are id-sorted (canonical DB key); show the
                        # items grouped by set (largest set first, tie broken by
                        # earliest slot) in gear-overview slot order within each
                        # group. Setless items (crafted/embellishment comps) all
                        # land in one group, i.e. plain slot order.
                        def slot_pos(item_id):
                            return INVTYPE_DISPLAY_ORDER.get(
                                item_lookup.get(item_id, {}).get("inventoryType"), 99
                            )

                        set_counts = {}
                        set_first_slot = {}
                        for i in ids:
                            sid = item_lookup.get(i, {}).get("itemSetId")
                            set_counts[sid] = set_counts.get(sid, 0) + 1
                            set_first_slot[sid] = min(
                                set_first_slot.get(sid, 99), slot_pos(i)
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
                        comps.append(
                            {
                                "ids": ids,
                                "count": count,
                                "max_timed": row[2],
                                "max_depleted": row[3],
                            }
                        )
                        if len(comps) >= limit:
                            break
                    return comps

                # Gem/enchant combos are multisets (the DB comp string keeps
                # repeats, e.g. two of the same gem). This collapses those repeats
                # into {id, qty} for a compact "x2" display, drops ids the render
                # lookup doesn't know (cosmetic gems / filtered old enchants), and
                # re-merges any rows that become identical once those ids are gone
                # so their counts sum instead of showing as duplicate rows.
                def build_multiset_comps(raw_rows, lookup, threshold, limit=10):
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
                    )
                    return ranked[:limit]

                # Comp tables only cover the ~2 weeks of retained gear data, so
                # threshold against their own totals instead of season-wide
                # spec run counts (which would filter everything out).
                embellishment_comps = build_comps(
                    embellishment_comps_raw, total_embellishment_comps * 0.005
                )
                crafted_comps = build_comps(
                    crafted_comps_raw, total_crafted_comps * 0.005
                )
                tier_set_comps = build_comps(
                    tier_set_comps_raw, total_tier_set_comps * 0.005
                )
                gem_comps = build_multiset_comps(
                    gem_comps_raw, socket_lookup, total_gem_comps * 0.005
                )
                enchant_comps = build_multiset_comps(
                    enchant_comps_raw, enchant_lookup, total_enchant_comps * 0.005
                )

                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching loadout...")
                loadouts = aggregateData.get_loadout(
                    conn, cursor, spec_id, current_season_id
                )
                # fetch top-50 verified loadouts (meta + items/gems/enchants/talents)
                try:
                    top50_raw = databaseConnector.fetch_top50_loadouts(
                        conn, cursor, spec_id, current_season_id, limit=50
                    )
                except Exception as e:
                    print(f"Warning: fetch_top50_loadouts failed: {e}")
                    top50_raw = []

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
                    spec_talents_difs,
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
                    # Titan's Grip Fury wields a two-hander in the off-hand too,
                    # so never strip its off-hand (see DUAL_WIELD_TWOHAND_SPECS).
                    if int(spec_id) not in DUAL_WIELD_TWOHAND_SPECS and (
                        item_lookup.get(mh_item_id, {}).get("inventoryType") == 17
                        or item_lookup.get(mh_item_id, {}).get("itemSubClass") == 3 # guns
                        or item_lookup.get(mh_item_id, {}).get("itemSubClass") == 2 # bows
                        or item_lookup.get(mh_item_id, {}).get("itemSubClass") == 18 # Crossbows
                    ):
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
                top_comps_data = top_comps_by_spec.get(str(spec_id), [])

            if not tree_by_spec.get(int(spec_id)):
                raise ValueError(f"No talent tree data for spec {spec_id}")

            # Build one talent-overview variant per hero tree (most popular
            # first, which becomes the default shown). Each variant carries its
            # own class/spec/hero UI trees, per-dungeon deviations, and loadout
            # string so the page can switch the whole overview client-side.
            tree_nodes = tree_by_spec.get(int(spec_id), {})
            sub_trees = talent_lookup.get("subTrees", {})

            # Top-50 verified-player usage, used for the talent "TOP" highlight.
            # `top_pct_map` drives per-node elite-vs-popular divergence; the hero
            # counts below pick the hero tree the top players actually run.
            top_pct_map = bis_summary.get("talent_node_pct", {}) if bis_summary else {}
            # Per-choice top-50 usage, so choice nodes can highlight which option
            # the elite players actually pick (drives the per-choice TOP badge).
            top_entry_pct_map = bis_summary.get("talent_node_entry_pct", {}) if bis_summary else {}

            # Per-dungeon "TOP" set for the Talent Differences modal: flat
            # "<dungeon_id>:<node_id>" -> pct for talents a majority of top
            # players took in that dungeon. Keyed by string to match the modal's
            # dungeon loop variable.
            top_dungeon_talent_pct = {}
            for _d, _nodes in (bis_summary.get("talent_pct_by_dungeon", {}) if bis_summary else {}).items():
                for _nid, _pct in _nodes.items():
                    if _pct >= TALENT_DUNGEON_ELITE_MIN_PCT:
                        top_dungeon_talent_pct[f"{_d}:{_nid}"] = _pct

            # Inputs for the weighted Talent Differences ranking: overall top-50
            # adoption per node, plus the full per-dungeon adoption map.
            top_overall_pct = {
                int(nid): info.get("pct", 0.0)
                for nid, info in (bis_summary.get("talent_node_pct", {}) if bis_summary else {}).items()
            }
            top_dungeon_pct_map = bis_summary.get("talent_pct_by_dungeon", {}) if bis_summary else {}
            hero_node_subtree = {
                int(hn["id"]): int(hn["subTreeId"])
                for hn in tree_nodes.get("heroNodes", [])
                if "id" in hn and hn.get("subTreeId") is not None
            }
            hero_tree_top_counts = defaultdict(int)
            n_top = len(top50_raw) if top50_raw else 0
            for lo in (top50_raw or []):
                subtree_hits = defaultdict(int)
                for t in lo.get("talents", []) or []:
                    nid = t.get("node_id") or t.get("id")
                    if nid is None:
                        continue
                    st = hero_node_subtree.get(int(nid))
                    if st is not None:
                        subtree_hits[st] += 1
                if subtree_hits:
                    chosen = max(subtree_hits.items(), key=lambda x: x[1])[0]
                    hero_tree_top_counts[chosen] += 1
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

            hero_variants = []
            for ht in sorted(
                hero_trees, key=lambda t: t.get("count", 0), reverse=True
            ):
                tid = ht["id"]
                sub = sub_trees.get(str(tid), {})
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
                    "talent_difs": {
                        "Class": aggregateData.biggest_deviations_per_dungeon(
                            class_by_tree.get(tid, {}),
                            top_overall=top_overall_pct,
                            top_dungeon_pct=top_dungeon_pct_map,
                            top_weight=TALENT_DIFF_TOP_WEIGHT,
                            normal_weight=TALENT_DIFF_NORMAL_WEIGHT,
                        ),
                        "Hero": aggregateData.biggest_deviations_per_dungeon(
                            hero_by_tree.get(tid, {}),
                            top_overall=top_overall_pct,
                            top_dungeon_pct=top_dungeon_pct_map,
                            top_weight=TALENT_DIFF_TOP_WEIGHT,
                            normal_weight=TALENT_DIFF_NORMAL_WEIGHT,
                        ),
                        "Spec": aggregateData.biggest_deviations_per_dungeon(
                            spec_by_tree.get(tid, {}),
                            top_overall=top_overall_pct,
                            top_dungeon_pct=top_dungeon_pct_map,
                            top_weight=TALENT_DIFF_TOP_WEIGHT,
                            normal_weight=TALENT_DIFF_NORMAL_WEIGHT,
                        ),
                    },
                })

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
                spec_id, spec_data, class_data, stat_priority,
                left_slots, right_slots, weapon_slots, trinket_slots,
                enchant_slots, enchant_lookup, item_lookup, item_slug_map,
                bis_summary, socket_lookup,
            )
            spec_meta_dir = os.path.join("assets", "json", "spec_meta")
            os.makedirs(spec_meta_dir, exist_ok=True)
            with open(os.path.join(spec_meta_dir, f"{spec_id}.json"), "w", encoding="utf-8") as f:
                json.dump(spec_meta, f, separators=(",", ":"))

            output_html = template.render(
                generated_at=datetime.now(timezone.utc).timestamp(),
                spec_id=spec_id,
                spec=spec_data,
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
                hero_variants=hero_variants,
                top_hero_tree_id=top_hero_tree,
                top_hero_tree_name=top_hero_tree_name,
                top_hero_tree_pct=top_hero_tree_pct,
                top_dungeon_talent_pct=top_dungeon_talent_pct,
                tree_data=tree_by_spec.get(int(spec_id)),
                hero_tree_difs=hero_tree_difs,
                hero_tree_count=hero_tree_count,
                top_routes=top_routes,
                top_comps_data=top_comps_data,
                season_info=season_info,
                stats=stat_priority,
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
    parser.add_argument("--CLIENT_ID", required=True)
    parser.add_argument("--CLIENT_SECRET", required=True)
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
        args.CLIENT_ID,
        args.CLIENT_SECRET,
        args.debug,
        args.spec,
    )
