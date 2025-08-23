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

LOOKUP_DIR = "data/static"  # Default lookup directory, can be overridden by command line argument
ROLE_FOLDERS = {
    "0": "Tank",
    "1": "Healer",
    "2": "Dps",
}
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
    "WEAPON",
    "SHOULDER",
    "TRINKET",
    "WAIST",
    "WRIST",
]

STAT_NAMES = {
    "stragiint": "Mainstat",
    "stragi": "Str/Agi",
}

DUNGEON_DIFFERENCE_CATEGORIES = [
    # (json key in file, category name in template, entry‑dict id field)
    ("class_talents", "Class", "talent_id"),
    ("spec_talents", "Spec", "talent_id"),
    ("hero_talents", "Hero", "talent_id"),
    ("hero_trees", "Trees", "tree_id"),
]


def load_json(path):
    with open(path, "r") as f:
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
      - "HH:MM:SS.mmm" once you hit one hour or more

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

def escape_raidbot_code(code):
    """
    """
    loadout = {}
    if not code:
        return
    loadout['original'] = code
    loadout['code'] = quote_plus(code, safe="")
    return loadout

def get_top_route(routes):
    """
    Given a list of route dicts (with keys 'count', 'avg_key', 'highest_key', 'route_key'),
    return the route_key of the route that wins the tiebreaker:
      1) highest count
      2) if tie, highest avg_key
      3) if tie, highest highest_key
    """
    return max(
        routes,
        key=lambda r: (r["count"], r["highest_key"]),
    )


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
                "quality_override": e.get("quality_override"),
                "slot_slug": slot_slug,
            }
            total_count += e.get("count", 0)
            entries.append(entry)

        normalized.append({"slot": slot_name, "slug": slot_slug, "entries": entries, "slot_count": total_count})
    return normalized

def checkItemLimits(sockets, socket_lookup, socket_limits):
    for socket in sockets:
        if not socket_lookup.get(int(socket['id'])):
            continue
        limit = socket_lookup[int(socket['id'])].get("itemLimitCategory")
        if limit:
            if socket_limits.get(limit['id']):
                if limit['quantity']>= socket_limits.get(limit['id']):
                    continue
                else:
                    socket_limits[limit['id']] += 1
                    return socket
            else:
                socket_limits[limit['id']] = limit['quantity']
                return socket
        else:
            return socket
    return 

def handleSocketsForItem(conn, cursor, spec_id, current_season_id, item_id, amount, sockets, socket_limits, socket_lookup):
    sockets_data = []
    if amount > 0:
        current_socket_items = databaseConnector.fetch_top_sockets_for_item(conn, cursor, spec_id, current_season_id, item_id)
        used_sockets = [pair[0] for pair in current_socket_items if len(pair) > 0]
        for i in range(0, amount):
            active_socket = None
            if len(sockets) > 0 and sockets[0] in used_sockets:  # might want to change this
                active_socket = checkItemLimits(sockets, socket_lookup, socket_limits)

            elif used_sockets and len(used_sockets) > 0:
                used_sockets_converted = [{"id": socket} for socket in used_sockets]
                active_socket = checkItemLimits(used_sockets_converted, socket_lookup, socket_limits)
            if active_socket:
                sockets_data.append(active_socket)
    return sockets_data

def fetch_slot_info(conn, cursor, spec_id, current_season_id,slot):
    if MULTI_SLOT_GROUPS.get(slot):
        num = re.search(r'\d+', slot)
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

def main(template_path, output_dir, CLIENT_ID, CLIENT_SECRET, debug=False , spec=None):
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
    embellishment_lookup = load_json(os.path.join(LOOKUP_DIR, "embellishments.json"))
    missive_lookup = load_json(os.path.join(LOOKUP_DIR, "missives.json"))
    price_lookup = load_json(
        os.path.join(LOOKUP_DIR, "commodities", "eu.json")
    )  # this is temporarily just using eu prices
    bonus_lookup = load_json(os.path.join(LOOKUP_DIR, "bonuses.json"))
    route_data = load_json(os.path.join(LOOKUP_DIR, "routeData.json"))
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
    item_lookup = {
        i["id"]: i for i in load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
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
    os.makedirs(output_dir, exist_ok=True)

    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)

    # Build a dict mapping role names to lists of specs
    spec_nav = {role_name: [] for role_name in ROLE_FOLDERS.values()}

    for sid, sdata in spec_lookup.items():
        role_key = str(sdata.get("role", 2))
        role_name = ROLE_FOLDERS.get(role_key, "Other")
        class_data = class_lookup.get(str(sdata.get("classID", "")), {})
        filename = f"{sdata['name']}_{class_data.get('name')}"
        spec_nav[role_name].append(
            {
                "name": f"{sdata['name']} {class_data.get('name')}",
                "url": f"/classes/{role_name}/{filename}",
                "icon": sdata.get("SpellIconFileId"),
                "color": {
                    "r": class_data.get("color", {}).get("r", 0),
                    "g": class_data.get("color", {}).get("g", 0),
                    "b": class_data.get("color", {}).get("b", 0),
                },
            }
        )

    # Optionally sort each list by name:
    for lst in spec_nav.values():
        lst.sort(key=lambda x: x["name"])

    access_token = aggregateData.get_access_token(CLIENT_ID, CLIENT_SECRET)
    current_season_id = aggregateData.get_current_season_id(access_token)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Current season ID: {current_season_id}"
    )

    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))

    if spec:
        spec_keys = [spec]
    else:
        spec_keys = list(spec_lookup.keys())

    # Iterate over each spec folder
    for spec_id in spec_keys:
        print(f"[{datetime.now(timezone.utc).isoformat()}] Processing spec {spec_id}...")
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()

            spec_data = spec_lookup.get(spec_id, {})
            class_data = class_lookup.get(spec_data.get("classID", ""), {})

            talent_lookup = load_json(
                os.path.join(LOOKUP_DIR, "talents", f"{spec_id}.json")
            )
            valid_talents = {int(tid) for tid in talent_lookup.get("talents", {})}
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
            dungeon_entries = route_data.get(spec_id, {}).get("dungeons", [])
            # produce a mapping dungeon_id → best route_key
            top_routes = {}
            for d in dungeon_entries:
                if d.get("routes"):
                    top_routes[d["dungeon_id"]] = get_top_route(d["routes"])
                    top_routes[d["dungeon_id"]]["dungeon_id"] = d["dungeon_id"]
            # Render template

            

            popular_hero_tree = 0
            popular_hero_tree_count = 0
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching hero tree info...")
            hero_trees = aggregateData.get_hero_trees(
                conn, cursor, spec_id, current_season_id
            )

            for tree in hero_trees:
                if tree.get("count"):
                    count = tree["count"]
                    if count > popular_hero_tree_count:
                        popular_hero_tree_count = count
                        popular_hero_tree = tree.get("id")
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching enchants...")
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
                            total_enchant_counts[slot_group] += enchant.get('count')
                        else:
                            print(f"Missing enchant lookup for {enchant_id}")
                    enchant_slots[slot_group] = valid_enchants
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching missives...")
            missives = databaseConnector.fetch_missive_count(
                conn, cursor, spec_id, current_season_id
            )
            total_missive_count = sum(e[1] for e in missives)
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching embellishments...")
            embellishments = databaseConnector.fetch_embellishment_count(
                conn, cursor, spec_id, current_season_id
            )

            total_embellishment_count = sum(e[1]for e in embellishments)
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching sockets...")
            sockets = aggregateData.get_sockets(conn, cursor, spec_id, current_season_id)

            total_socket_count = sum(s.get("count", 0) for s in sockets)
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching spec data count...")
            data_count = databaseConnector.fetch_spec_data_count(
                conn, cursor, spec_id, current_season_id
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching total runs...")
            total_runs = databaseConnector.fetch_total_season_runs(
                conn, cursor, current_season_id
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching spec runs...")
            spec_runs = databaseConnector.fetch_runs_per_spec(conn, cursor, current_season_id, spec_id)

            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching loadout...")
            loadouts = aggregateData.get_loadout(conn, cursor, spec_id, current_season_id)
            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching highest run...")
            highest_run = databaseConnector.fetch_max_key_run_per_spec(conn, cursor, spec_id, current_season_id)

            print(f"[{datetime.now(timezone.utc).isoformat()}] converting slots...")
            primary_ids = {
                int(items[0]['item']) for items in left_slots + right_slots + weapon_slots + trinket_slots if len(items) > 0
            }

            socket_limits = {}
            for items, slot in zip(left_slots + right_slots + weapon_slots + trinket_slots, LEFT_ORDER+RIGHT_ORDER+WEAPON_SLOTS+TRINKET_SLOTS):
                for item in items:
                    sid = item_lookup[int(item['item'])].get("itemSetId")
                    if sid:
                        raw_peers = [pid for pid in set_members[sid]]
                        peers = [pid for pid in raw_peers if pid in primary_ids]
                        if peers:
                            item["pcs"] = peers

                    amount = 0
                    if not item.get("bonus"):
                        continue
                    bonus = item.get('bonus', {}).get('ids','')
                    bonus_ids = bonus.split(',')
                    for bonus in bonus_ids:
                        if bonus_lookup.get(str(bonus)) and bonus_lookup[str(bonus)].get('socket'):
                            amount += bonus_lookup[str(bonus)].get('socket')
                        if missive_lookup.get(str(bonus)):
                            if missives and len(missives) > 0:
                                item["missive"] = missives[0][0]
                        if embellishment_lookup.get(str(bonus)):
                            if embellishments and len(embellishments) > 0:
                                item["embellishment"] = embellishments[0][0]
                        if bonus_quality_lookup.get(str(bonus)):
                            item["quality_override"] = bonus_quality_lookup[str(bonus)]
                    if amount < len(item_lookup.get(int(item['item']), {}).get('socketInfo', {}).get('sockets', [])):
                        print(f"Adjusting amount for item {item['item']}: {amount} {len(item_lookup.get(int(item['item']), {}).get('socketInfo', {}).get('sockets', []))}")
                        amount = len(item_lookup.get(int(item['item']), {}).get('socketInfo', {}).get('sockets', []))

                    sockets_data = handleSocketsForItem(conn, cursor, spec_id, current_season_id, item['item'], amount, sockets, socket_limits, socket_lookup)
                    if sockets_data:
                        item["socket"] = sockets_data

                    enchantment_data = {}
                    if enchant_slots.get(slot) and len(enchant_slots[slot]) > 0:
                        enchantment_data = enchant_slots[slot][0]
                    elif MULTI_SLOT_GROUPS.get(slot) and enchant_slots.get(MULTI_SLOT_GROUPS[slot]) and len(enchant_slots[MULTI_SLOT_GROUPS[slot]]) > 0:
                        enchantment_data = enchant_slots.get(MULTI_SLOT_GROUPS[slot], [])[0]
                    item["enchantment"] = enchantment_data


            left_slots = normalize_slot_collections(left_slots, LEFT_ORDER)
            right_slots = normalize_slot_collections(right_slots, RIGHT_ORDER)
            weapon_slots = normalize_slot_collections(weapon_slots, WEAPON_SLOTS)
            trinket_slots = normalize_slot_collections(trinket_slots, TRINKET_SLOTS)

            # remove offhand if 2 hander is equipped
            mh = next((g for g in weapon_slots if g["slot"] == "MAIN_HAND"), None)
            oh = next((g for g in weapon_slots if g["slot"] == "OFF_HAND"), None)
            if mh and mh["entries"] and len(mh["entries"]) > 0:
                mh_item_id = mh["entries"][0]["id"]
                # look up its inventoryType; two‑handers are 17
                if item_lookup.get(mh_item_id, {}).get("inventoryType") == 17:
                    # always build combined list (falls back to just mh entries if oh is None)
                    combined = mh["entries"] + (oh.get("entries", []) if oh else [])
                    # re‑sort + trim to top 10
                    mh["entries"] = combined
                    # if there was an Off Hand slot, drop it entirely
                    if oh:
                        weapon_slots = [g for g in weapon_slots if g["slot"] != "OFF_HAND"]

            print(f"[{datetime.now(timezone.utc).isoformat()}] fetching upgrade counts...")
            upgrade_counts = databaseConnector.fetch_spec_upgrade(conn, cursor, spec_id, current_season_id)
            print(f"[{datetime.now(timezone.utc).isoformat()}] generating page...")
            output_html = template.render(
                generated_at=datetime.now(timezone.utc).timestamp(),
                spec_id=spec_id,
                spec=spec_data,
                class_info=class_data,
                data_count=data_count,
                summary_data={
                    "count" : spec_runs,
                    "upgrade_counts": upgrade_counts
                },
                total_enchant_counts = total_enchant_counts,
                total_socket_count=total_socket_count,
                total_embellishment_count=total_embellishment_count,
                total_missive_count = total_missive_count,
                total_season_runs=total_runs,
                left_slots=left_slots,
                right_slots=right_slots,
                weapon_slots=weapon_slots,
                trinket_slots=trinket_slots,
                enchant_slots=enchant_slots,
                hero_trees=hero_trees,
                loadout_code=escape_raidbot_code(loadouts.get(popular_hero_tree, {}).get("loadout")),
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
                spec_nav=spec_nav,
                current_spec=f"{spec_data['name']} {class_data.get('name')}",
                sockets=sockets,
                embellishments=embellishments,
                missives=missives,
                formatted_price=formatted_price,
                stat_names=STAT_NAMES,
                trending=spec_runs/total_runs if total_runs > 0 else 0,
                highest_run=highest_run,
                dungeon_summaries={},
                top_routes=top_routes,
                season_info=season_info,
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
            print(f"Generated {out_path}")
            if debug: 
                raise ValueError("Debug mode: stopping after first spec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW M+ spec pages")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )

    parser.add_argument("--database_host", required=True)
    parser.add_argument("--database_user", required=True)
    parser.add_argument("--database_password", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--CLIENT_ID", required=True)
    parser.add_argument("--CLIENT_SECRET", required=True)
    parser.add_argument("--debug", required=False)
    parser.add_argument("--spec", required=False)

    args = parser.parse_args()

    databaseConnector.init_connection_pool(
        args.database_host, args.database_user, args.database_password, args.database, 1
    )
    main(args.template, args.output_dir, args.CLIENT_ID, args.CLIENT_SECRET, args.debug, args.spec)
