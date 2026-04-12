import os
import json
import argparse
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
import aggregateData
from collections import defaultdict
from datetime import datetime, timezone
from contextlib import closing
import re
from urllib.parse import quote_plus
from pageGeneration import ROLE_FOLDERS, generateSpecNav, generateDungeonNav

LOOKUP_DIR = "data/static"  # Default lookup directory, can be overridden by command line argument
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

STAT_NAMES = {
    "stragiint": "Mainstat",
    "stragi": "Str/Agi",
    "agiint": "Agi/Int",
    "strint": "Str/Int",
}

BLIZZARD_STAT_MAP = {
    32: "crit",
    36: "haste",
    40: "versatility",
    49: "mastery",
    61: "speed",
    62: "leech",
    63: "avoidance",
}

SECONDARY_STATS = ["haste", "versatility", "mastery", "crit"]
TERTIARY_STATS = [
    "avoidance",
    "lifesteal",
    "speed",
]
HEALTH_STATS = ["health", "stamina"]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def format_utc_timestamp(ms):
    """
    Convert a UTC timestamp in milliseconds (e.g. 1750986462000)
    into a string like "DD/MM/YYYY, HH:MM:SS".
    """

    dt = datetime.fromtimestamp(int(ms), timezone.utc)
    return dt.strftime("%d/%m/%Y, %H:%M:%S")


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


# helpers

def node_has_valid_spellid(node):
    entries = node.get("entries", [])
    # For choice/tiered nodes, at least one entry must have a nonzero spellId
    for e in entries:
        if e.get("spellId", 0):
            return True
    return False

def build_ui_tree(nodes, pop_data, is_hero=False, pop_hero_tree_id=None):

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
        
        for e in entries:
            e_pct = pop_map.get(e.get("definitionId"), 0.0)
            e_count = pop_count_map.get(e.get("definitionId"), 0)
            if e_pct == 0.0 and e.get("id"):
                e_pct = pop_map.get(e["id"], 0.0)
                e_count = pop_count_map.get(e["id"], 0)
            if e_pct == 0.0 and e.get("spellId"):
                e_pct = pop_map.get(e["spellId"], 0.0)
                e_count = pop_count_map.get(e["spellId"], 0)
            
            total_entry_pct += e_pct
            node_choices.append({
                "name": e.get("name", ""),
                "icon": e.get("icon", ""),
                "pct": min(e_pct, 100.0),
                "count": e_count,
                "spellId": e.get("spellId", 0),
                "maxRanks": e.get("maxRanks", 1)
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

        ui_nodes.append({
            "id": n["id"],
            "left": (n.get("posX", 0) - min_x) / w * 100,
            "top": (n.get("posY", 0) - min_y) / h * 100,
            "pct": "{:.1f}".format(pct),
            "pct_val": pct,
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


def fetch_slot_info(conn, cursor, spec_id, current_season_id, slot):
    if MULTI_SLOT_GROUPS.get(slot):
        num = re.search(r"\d+", slot)
        data = databaseConnector.fetch_top_items_for_slot_group_with_bonus(
            conn, cursor, spec_id, current_season_id, MULTI_SLOT_GROUPS.get(slot)
        )
        index_to_remove = int(num.group()) - 1
        if 0 <= index_to_remove < len(data):
            del data[index_to_remove]
        return data
    else:
        return databaseConnector.fetch_top_items_for_slot_with_bonus(
            conn, cursor, spec_id, current_season_id, slot
        )


def fetch_hero_tree_info(conn, cursor, spec_id, current_season_id):
    popular_hero_tree = 0
    popular_hero_tree_count = 0
    hero_trees = aggregateData.get_hero_trees(conn, cursor, spec_id, current_season_id)
    hero_tree_count = 0
    for tree in hero_trees:
        if tree.get("count"):
            count = tree["count"]
            hero_tree_count += count
            if count > popular_hero_tree_count:
                popular_hero_tree_count = count
                popular_hero_tree = tree.get("id")
    return hero_trees, popular_hero_tree, popular_hero_tree_count, hero_tree_count


def fetch_enchant_info(conn, cursor, spec_id, current_season_id, enchant_lookup):
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
            enchant_slots[slot_group] = valid_enchants
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
                if peers:
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
            if slot in WEAPON_SLOTS:
                if item_lookup[int(item["item"])].get("itemClass") == 2 and enchant_slots.get("WEAPON") and len(enchant_slots["WEAPON"]) > 0:
                    enchantment_data = enchant_slots["WEAPON"][0]
            elif enchant_slots.get(slot) and len(enchant_slots[slot]) > 0:
                enchantment_data = enchant_slots[slot][0]
            elif (
                MULTI_SLOT_GROUPS.get(slot)
                and enchant_slots.get(MULTI_SLOT_GROUPS[slot])
                and len(enchant_slots[MULTI_SLOT_GROUPS[slot]]) > 0
            ):
                enchantment_data = enchant_slots.get(MULTI_SLOT_GROUPS[slot], [])[0]
            item["enchantment"] = enchantment_data


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


def fetch_hunter_pets(
    conn, cursor, spec_id, spec_data, spec_runs, creature_lookup, non_tameable_creatures
):
    hunter_pets = []

    # check for hunter
    if str(spec_data.get("classID")) == "3":
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] fetching hunter pets for spec {spec_id}..."
        )
        try:
            pet_rows = databaseConnector.fetch_top_hunter_pets_by_spec(
                conn, cursor, spec_id
            )
        except Exception as e:
            print(f"Error fetching hunter pets: {e}")
            pet_rows = []

        # pet_rows expected: list of dicts { 'creature_id': int, 'run_count': int }
        # Determine spec_runs_count: spec_runs may be an int or list — be defensive
        if isinstance(spec_runs, int):
            spec_runs_count = spec_runs
        elif isinstance(spec_runs, (list, tuple)):
            spec_runs_count = len(spec_runs)
        else:
            # fallback if spec_runs is missing or something else
            spec_runs_count = int(spec_runs) if spec_runs else 0

        # avoid zero division later
        if spec_runs_count == 0:
            spec_runs_count = 1

        total_pet_runs = sum(int(p.get("run_count", 0)) for p in pet_rows) or 1

        for p in pet_rows:
            cid = str(p.get("creature_id"))
            info = creature_lookup.get(cid, {})
            name = info.get("name", {}).get("en_US") or info.get("name") or cid
            family = info.get("family", {}).get("en_US") or ""
            family_id = info.get("family_id") or ""
            if family_id in non_tameable_creatures[spec_id]:
                print(f"Skipping non-tameable pet {name} ({cid}) for spec {spec_id}")
                continue
            ctype = info.get("type", {}).get("en_US") or ""
            image = info.get("image") or f"data/creature_img/{cid}.jpg"
            run_count = int(p.get("run_count", 0))
            pet = {
                "creature_id": cid,
                "name": name,
                "family": family,
                "type": ctype,
                "image": image,
                "run_count": run_count,
                # % of this spec's runs that included this pet
                "pet_pct_spec": (run_count / spec_runs_count) * 100,
                # % of total pet observations
                "pet_pct_of_pet_total": (run_count / total_pet_runs) * 100,
            }
            hunter_pets.append(pet)
    hunter_pets = hunter_pets[:10]
    return hunter_pets


def main(template_path, output_dir, CLIENT_ID, CLIENT_SECRET, debug=False, spec=None):
    from generateSocialsPost import createSpecOverviewImg # local import so we don't get circular dependency issues
    # Prepare Jinja2 environment
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
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
    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)
    crafting_all = load_json(os.path.join(LOOKUP_DIR, "crafting.json"))
    reagent_lookup = {r["id"]: r for r in crafting_all.get("reagents", [])}
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    creature_lookup = load_json(os.path.join(LOOKUP_DIR, "creatures.json"))
    non_tameable_creatures = load_json(
        os.path.join(LOOKUP_DIR, "notTamablePetFamilies.json")
    )
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

                spec_data = spec_lookup.get(spec_id, {})
                class_data = class_lookup.get(spec_data.get("classID", ""), {})

                talent_lookup = load_json(
                    os.path.join(LOOKUP_DIR, "talents", f"{spec_id}.json")
                )
                valid_talents = {int(tid) for tid in talent_lookup.get("talents", {})}
                print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching talents...")
                hero_talents_full = aggregateData.get_hero_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )
                spec_talents_full = aggregateData.get_spec_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )

                class_talents_full = aggregateData.get_class_talent_differences(
                    conn, cursor, spec_id, current_season_id, valid_talents
                )

                hero_talents_difs = aggregateData.biggest_deviations_per_dungeon(hero_talents_full)
                spec_talents_difs = aggregateData.biggest_deviations_per_dungeon(spec_talents_full)
                class_talents_difs = aggregateData.biggest_deviations_per_dungeon(class_talents_full)
                hero_tree_difs = aggregateData.get_hero_tree_differences(
                    conn, cursor, spec_id, current_season_id
                )
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching slots...")
                # Split slots into left/right/weapon/trinket
                left_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s)
                    for s in LEFT_ORDER
                ]
                right_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s)
                    for s in RIGHT_ORDER
                ]
                weapon_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s)
                    for s in WEAPON_SLOTS
                ]
                trinket_slots = [
                    fetch_slot_info(conn, cursor, spec_id, current_season_id, s)
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
                ) = fetch_hero_tree_info(conn, cursor, spec_id, current_season_id)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching enchants..."
                )
                enchant_slots, total_enchant_counts = fetch_enchant_info(
                    conn, cursor, spec_id, current_season_id, enchant_lookup
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
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching sockets...")
                sockets = aggregateData.get_sockets(
                    conn, cursor, spec_id, current_season_id
                )
                total_socket_count = sum(s.get("count", 0) for s in sockets)
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching spec data count..."
                )
                data_count = databaseConnector.fetch_spec_data_count(
                    conn, cursor, spec_id, current_season_id
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching total runs..."
                )
                total_runs = databaseConnector.fetch_total_season_runs(
                    conn, cursor, current_season_id
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching spec runs..."
                )
                spec_runs = databaseConnector.fetch_runs_per_spec(
                    conn, cursor, current_season_id, spec_id
                )

                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching loadout...")
                loadouts = aggregateData.get_loadout(
                    conn, cursor, spec_id, current_season_id
                )
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
                    # look up its inventoryType; two‑handers are 17 and ranged weapons are 26
                    if (
                        item_lookup.get(mh_item_id, {}).get("inventoryType") == 17
                        or item_lookup.get(mh_item_id, {}).get("itemSubClass") == 3
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
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching upgrade counts..."
                )
                upgrade_counts = databaseConnector.fetch_spec_upgrade(
                    conn, cursor, spec_id, current_season_id
                )
                print(f"[{datetime.now(timezone.utc).isoformat()}] fetching stats...")
                stat_priority, tertiary_priority, health_priority = fetch_stat_info(
                    conn, cursor, spec_id, current_season_id, spec_lookup
                )
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching hunter pets..."
                )
                hunter_pets = fetch_hunter_pets(
                    conn,
                    cursor,
                    spec_id,
                    spec_data,
                    spec_runs,
                    creature_lookup,
                    non_tameable_creatures,
                )

                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] fetching top comps..."
                )
                top_comps_data = databaseConnector.fetch_spec_top_comps(
                    conn, cursor, spec_id, current_season_id
                )

            if not tree_by_spec.get(int(spec_id)):
                raise ValueError(f"No talent tree data for spec {spec_id}")

            print(f"[{datetime.now(timezone.utc).isoformat()}] generating page...")
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
                loadout_code=escape_raidbot_code(
                    loadouts.get(popular_hero_tree, {}).get("loadout")
                ),
                enchant_lookup=enchant_lookup,
                embellishment_lookup=embellishment_lookup,
                missive_lookup=missive_lookup,
                socket_lookup=socket_lookup,
                spec_lookup=spec_lookup,
                item_lookup=item_lookup,
                notifications=notifications,
                reagent_lookup=reagent_lookup,
                dungeon_lookup=dungeon_lookup,
                dungeon_lookup_slug=dungeon_lookup_slug,
                role=ROLE_FOLDERS[spec_data.get("role", 2)],
                talent_lookup=talent_lookup,
                current_spec=f"{spec_data['name']} {class_data.get('name')}",
                sockets=sockets,
                embellishments=embellishments,
                crafted_items=crafted_items,
                total_crafted_items=total_crafted_items_count,
                missives=missives,
                formatted_price=formatted_price,
                stat_names=STAT_NAMES,
                trending=spec_runs / total_runs if total_runs > 0 else 0,
                highest_run=highest_run,
                talent_difs={
                    "Class": class_talents_difs,
                    "Hero": hero_talents_difs,
                    "Spec": spec_talents_difs,
                },
                talent_difs_full={
                    "Class": class_talents_full,
                    "Hero": hero_talents_full,
                    "Spec": spec_talents_full,
                },
                ui_class_tree=build_ui_tree(tree_by_spec.get(int(spec_id), {}).get("classNodes", []), class_talents_full) if spec_id else None,
                ui_hero_tree=build_ui_tree(tree_by_spec.get(int(spec_id), {}).get("heroNodes", []), hero_talents_full, is_hero=True, pop_hero_tree_id=popular_hero_tree) if spec_id else None,
                ui_spec_tree=build_ui_tree(tree_by_spec.get(int(spec_id), {}).get("specNodes", []), spec_talents_full) if spec_id else None,
                tree_data=tree_by_spec.get(int(spec_id)),
                hero_tree_difs=hero_tree_difs,
                hero_tree_count=hero_tree_count,
                top_routes=top_routes,
                top_comps_data=top_comps_data,
                season_info=season_info,
                stats=stat_priority,
                tertiary_priority=tertiary_priority,
                health_priority=health_priority,
                hunter_pets=hunter_pets,
                creature_lookup=creature_lookup,
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
            createSpecOverviewImg('tmp',preview_path, spec_id, current_season_id)
            print(f"[{datetime.now(timezone.utc).isoformat()}] Finished {spec_id}.")
            if debug:
                raise ValueError("Debug mode: stopping after first spec")
        except Exception as e:
            print(f"Error processing spec {spec_id}: {e}")


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

    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        1,
    )
    main(
        args.template,
        args.output_dir,
        args.CLIENT_ID,
        args.CLIENT_SECRET,
        args.debug,
        args.spec,
    )
