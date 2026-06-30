import os
import sys
import json
import argparse
from contextlib import closing
from collections import defaultdict
from datetime import datetime, timezone

from jinja2 import Environment, FileSystemLoader, select_autoescape

import databaseConnector
from pageGeneration import generateSpecNav, generateDungeonNav, build_item_slug_map
from generateSpecPages import LOOKUP_DIR, load_json, BLIZZARD_STAT_MAP

# How many entries to keep per item in each list (keeps per-item JSON small).
TOP_GEMS = 10
TOP_VARIANTS = 8

# Blizzard inventoryType -> readable slot label + canonical slot key.
# Only equippable gear types matter here; anything else falls back to "Other".
INVENTORY_TYPE_SLOT = {
    1: ("Head", "HEAD"),
    2: ("Neck", "NECK"),
    3: ("Shoulder", "SHOULDER"),
    5: ("Chest", "CHEST"),
    20: ("Chest", "CHEST"),
    6: ("Waist", "WAIST"),
    7: ("Legs", "LEGS"),
    8: ("Feet", "FEET"),
    9: ("Wrist", "WRIST"),
    10: ("Hands", "HANDS"),
    11: ("Finger", "FINGER"),
    12: ("Trinket", "TRINKET"),
    16: ("Back", "BACK"),
    13: ("One-Hand", "WEAPON"),
    14: ("Off Hand", "OFF_HAND"),
    15: ("Ranged", "WEAPON"),
    17: ("Two-Hand", "WEAPON"),
    21: ("Main Hand", "WEAPON"),
    22: ("Off Hand", "OFF_HAND"),
    23: ("Held In Off-hand", "OFF_HAND"),
    25: ("Ranged", "WEAPON"),
    26: ("Ranged", "WEAPON"),
}


# itemSubClass labels for weapons (itemClass 2), shown in the item header.
WEAPON_SUBCLASS = {
    0: "One-Handed Axe", 1: "Two-Handed Axe", 2: "Bow", 3: "Gun",
    4: "One-Handed Mace", 5: "Two-Handed Mace", 6: "Polearm",
    7: "One-Handed Sword", 8: "Two-Handed Sword", 9: "Warglaive",
    10: "Staff", 13: "Fist Weapon", 15: "Dagger", 16: "Thrown",
    18: "Crossbow", 19: "Wand",
}

# A spec is shown as a "top players' pick" when this share of its top-player
# loadouts equip the item.
TOP50_THRESHOLD = 50.0


def fail(msg):
    print("ERROR:", msg, file=sys.stderr)
    sys.exit(2)


def resolve_enchant(entry, enchant_lookup):
    """Turn a {id, pct} pick into a renderable enchant, or None."""
    if not entry:
        return None
    e = enchant_lookup.get(entry["id"]) or {}
    return {
        "id": entry["id"],
        "name": e.get("displayName", f"Enchant {entry['id']}"),
        "icon": e.get("spellIcon", ""),
        "spellId": e.get("spellId"),
        "pct": entry.get("pct"),
    }


def slot_for_item(item):
    """Return (label, key) for an item using its inventoryType."""
    return INVENTORY_TYPE_SLOT.get(int(item.get("inventoryType", 0) or 0), ("Other", "OTHER"))


def decode_bonus_list(bonus_str, bonus_lookup, embellishment_lookup=None, missive_lookup=None):
    """Decode a comma-separated bonus_list into a display-friendly summary.

    We surface only what is reliably decodable from bonuses.json (quality tag,
    added sockets, crafted stats, ilvl deltas). Exact final item level depends on
    Blizzard's curve math, so we keep the raw bonus string for the Wowhead link
    and let Wowhead render the precise tooltip.
    """
    tags = []
    sockets = 0
    crafted = []
    quality = None
    embellishment = None
    missive = None
    raw = [b.strip() for b in (bonus_str or "").split(",") if b.strip()]
    for bid in raw:
        # A crafted item carries its embellishment / missive as a bonus id that
        # maps to the embellishment/missive item.
        if embellishment_lookup and str(bid) in embellishment_lookup:
            embellishment = embellishment_lookup[str(bid)]
        if missive_lookup and str(bid) in missive_lookup:
            missive = missive_lookup[str(bid)]
        b = bonus_lookup.get(str(bid))
        if not b:
            continue
        # Only keep concise track tags (e.g. "Mythic", "Heroic"); skip the verbose
        # descriptive names (e.g. "Ascendant Voidforged: Myth") that duplicate them.
        tag = b.get("tag")
        if tag and ":" not in tag and tag not in tags:
            tags.append(tag)
        if b.get("socket"):
            try:
                sockets += int(b["socket"])
            except (TypeError, ValueError):
                pass
        if b.get("quality"):
            quality = b["quality"]
        for stat_id in b.get("craftedStats", []) or []:
            stat_type = BLIZZARD_STAT_MAP.get(stat_id)
            if stat_type and stat_type not in crafted:
                crafted.append(stat_type)
    return {
        "bonus": ":".join(raw),
        "tags": tags,
        "sockets": sockets,
        "crafted_stats": crafted,
        "quality": quality,
        "embellishment": embellishment,
        "missive": missive,
    }


def build_scope(total, max_timed, max_depleted, gem_runs, variant_runs,
                dungeon_data, gem_lookup, bonus_lookup,
                dungeon_totals, keylevel_totals,
                embellishment_lookup=None, missive_lookup=None, item_lookup=None,
                dungeon_keylevel_totals=None, reagent_lookup=None):
    """Assemble one scope payload (global or a single spec) from raw counters.

    ``dungeon_totals`` / ``keylevel_totals`` are this scope's item-independent run
    totals ({dungeon_id: runs} / {level: runs}); they turn the dungeon and
    key-level usage into adoption rates (% of that dungeon's / key level's runs
    that used the item) rather than raw counts.
    """
    item_lookup = item_lookup or {}
    # Gems: rank by usage, resolve name/icon, cap to TOP_GEMS.
    gem_total = sum(gem_runs.values()) or 1
    gems = []
    for gid, runs in sorted(gem_runs.items(), key=lambda x: x[1], reverse=True)[:TOP_GEMS]:
        g = gem_lookup.get(int(gid)) if str(gid).isdigit() else None
        gems.append({
            "id": int(gid),
            "name": (g or {}).get("itemName") or (g or {}).get("displayName") or f"Gem {gid}",
            "icon": (g or {}).get("itemIcon", ""),
            "quality": (g or {}).get("quality", 0),
            "runs": int(runs),
            "pct": round(runs / gem_total * 100, 1),
        })

    # Variants: decode each bonus combo, then merge combos that render
    # identically (same track/sockets/crafted) so we don't show duplicate rows.
    # Embellishments / missives are tallied separately (they're a per-item craft
    # choice, not an item-level tier).
    merged = {}
    emb_runs = defaultdict(int)
    mis_runs = defaultdict(int)
    for bonus_str, runs in variant_runs.items():
        dec = decode_bonus_list(bonus_str, bonus_lookup, embellishment_lookup, missive_lookup)
        if dec["embellishment"]:
            emb_runs[dec["embellishment"]] += runs
        if dec["missive"]:
            mis_runs[dec["missive"]] += runs
        sig = (tuple(dec["tags"]), dec["sockets"], tuple(dec["crafted_stats"]))
        m = merged.setdefault(sig, {"runs": 0, "dec": dec})
        m["runs"] += runs
    variant_total = sum(m["runs"] for m in merged.values()) or 1
    variants = []
    for m in sorted(merged.values(), key=lambda x: x["runs"], reverse=True)[:TOP_VARIANTS]:
        d = m["dec"]
        variants.append({
            "tags": d["tags"],
            "sockets": d["sockets"],
            "crafted_stats": d["crafted_stats"],
            "bonus": d["bonus"],
            "runs": int(m["runs"]),
            "pct": round(m["runs"] / variant_total * 100, 1),
        })

    # Dungeons: adoption rate = item runs / that dungeon's total runs. We also
    # keep the per-key-level item runs and (where the denominator is available)
    # per-key-level adoption so the page can filter to a single key level.
    dkl = dungeon_keylevel_totals or {}
    dungeons = []
    for did, d in dungeon_data.items():
        denom = dungeon_totals.get(str(did)) or dungeon_totals.get(did)
        adoption = round(min(100.0, d["runs"] / denom * 100), 1) if denom else None
        by_key = {}
        by_key_total = {}
        for lvl, r in sorted(d["by_key"].items()):
            by_key[str(lvl)] = int(r)
            t = dkl.get((str(did), int(lvl)))
            if t:
                by_key_total[str(lvl)] = int(t)
        dungeons.append({
            "id": int(did) if str(did).isdigit() else did,
            "runs": d["runs"],
            "max_key": d["max_key"],
            "adoption": adoption,
            "by_key": by_key,
            "by_key_total": by_key_total,
        })
    dungeons.sort(key=lambda x: (x["adoption"] if x["adoption"] is not None else -1, x["runs"]),
                  reverse=True)

    # Key levels: adoption rate per key level, summed across dungeons.
    kl_runs = defaultdict(int)
    for d in dungeon_data.values():
        for lvl, r in d["by_key"].items():
            kl_runs[int(lvl)] += r
    keylevels = []
    for lvl in sorted(kl_runs):
        denom = keylevel_totals.get(lvl) or keylevel_totals.get(str(lvl))
        adoption = round(min(100.0, kl_runs[lvl] / denom * 100), 1) if denom else None
        keylevels.append({"level": int(lvl), "runs": int(kl_runs[lvl]), "adoption": adoption})

    # Embellishment / missive: the most common one used on this crafted item.
    # These items live in the crafting reagents table, not equippable-items.
    reagents = reagent_lookup or {}
    def _top_craft(cmap):
        if not cmap:
            return None
        cid, runs = max(cmap.items(), key=lambda x: x[1])
        info = reagents.get(int(cid)) or (item_lookup.get(int(cid)) if str(cid).isdigit() else {}) or {}
        return {
            "id": int(cid),
            "name": info.get("name"),
            "icon": info.get("icon"),
            "pct": round(min(100.0, runs / (total or 1) * 100), 1),
        }

    return {
        "total_runs": int(total),
        "max_timed_key": int(max_timed),
        "max_depleted_key": int(max_depleted),
        "gems": gems,
        "variants": variants,
        "dungeons": dungeons,
        "keylevels": keylevels,
        "embellishment": _top_craft(emb_runs),
        "missive": _top_craft(mis_runs),
    }


def main(template_path, output_dir, items_dir="items", debug=False, target_item=None):
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    season = season_info.get("blizzard_season_id")
    if not season:
        fail("blizzard_season_id missing from seasonInfo.json")

    if (
        not os.environ.get("DATABASE_HOST")
        or not os.environ.get("DATABASE_USER")
        or not os.environ.get("DATABASE_PASSWORD")
    ):
        fail("Missing DB credentials (DATABASE_HOST/USER/PASSWORD).")

    # ---- static lookups -------------------------------------------------
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    bonus_lookup = load_json(os.path.join(LOOKUP_DIR, "bonuses.json"))
    embellishment_lookup = load_json(os.path.join(LOOKUP_DIR, "embellishments.json"))
    missive_lookup = load_json(os.path.join(LOOKUP_DIR, "missives.json"))
    # Embellishment / missive items aren't equippable gear; their names + icons
    # live in the crafting reagents table.
    crafting = load_json(os.path.join(LOOKUP_DIR, "crafting.json"))
    reagent_lookup = {r["id"]: r for r in crafting.get("reagents", [])}
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    enchant_all = load_json(os.path.join(LOOKUP_DIR, "enchantments.json"))
    gem_lookup = {e["itemId"]: e for e in enchant_all if e.get("slot") == "socket"}
    enchant_lookup = {e["id"]: e for e in enchant_all}

    equippable_items = load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
    item_lookup = {}
    for it in equippable_items:
        if "stats" in it:
            processed = []
            for s in sorted(it["stats"], key=lambda x: x.get("alloc", 0), reverse=True):
                stat_type = BLIZZARD_STAT_MAP.get(s["id"])
                if stat_type:
                    processed.append({"type": stat_type, "alloc": s.get("alloc", 0)})
            it = {**it, "stats": processed}
        item_lookup[it["id"]] = it

    # item_id -> URL slug (name-based; -<id> appended on name collisions). Built
    # from item names, so generateSpecPages.py derives the same slugs for links.
    slug_map = build_item_slug_map(item_lookup)

    set_members = defaultdict(list)
    for iid, itm in item_lookup.items():
        sid = itm.get("itemSetId")
        if sid:
            set_members[sid].append(iid)

    # Small maps embedded into the template for client-side name/icon resolution.
    specs_map = {}
    for sid, s in spec_lookup.items():
        c = class_lookup.get(s.get("classID", ""), {})
        specs_map[str(sid)] = {
            "name": s.get("name", "Unknown"),
            "className": c.get("name", "Unknown"),
            "classSlug": (c.get("name", "") or "").replace(" ", ""),
            "role": int(s.get("role", 2)),
            "icon": s.get("SpellIconFileId"),
        }
    dungeons_map = {}
    for did, d in dungeon_lookup.items():
        name = d.get("name")
        if isinstance(name, dict):
            name = name.get("en_US") or next(iter(name.values()), did)
        dungeons_map[str(did)] = {
            "name": name,
            "slug": d.get("slug"),
            "short": d.get("short_name"),
            "icon": d.get("icon"),
        }

    # ---- sweep the aggregation tables -----------------------------------
    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME", "Mythistone"),
        os.environ.get("DATABASE_PORT", "3306"),
        1,
    )

    # counters keyed by item_id (str)
    spec_runs = defaultdict(lambda: defaultdict(int))          # item -> spec -> runs
    spec_maxtimed = defaultdict(lambda: defaultdict(int))      # item -> spec -> max timed key
    spec_maxdep = defaultdict(lambda: defaultdict(int))        # item -> spec -> max depleted key
    gem_runs = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))     # item -> spec -> gem -> runs
    variant_runs = defaultdict(lambda: defaultdict(lambda: defaultdict(int))) # item -> spec -> bonus -> runs
    # item -> spec -> dungeon -> {runs,timed,depleted,max_key,by_key}
    dungeon_runs = defaultdict(lambda: defaultdict(lambda: defaultdict(
        lambda: {"runs": 0, "timed": 0, "depleted": 0, "max_key": 0, "by_key": defaultdict(int)})))

    # Sweep one spec at a time so every query is index-assisted (each table's PK
    # starts with spec_id) and result sets stay bounded.
    spec_ids = [str(s) for s in spec_lookup.keys()]
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        # Read-only build: don't hold a long-lived snapshot/transaction (a hung
        # build under autocommit=0 becomes an MDL holder that blocks the daily
        # TRUNCATE+rebuild events, which then blocks every later build). READ
        # UNCOMMITTED matches the events' isolation. lock_wait_timeout bounds how
        # long a query waits on a metadata lock so a contended table fails fast
        # (fetch_with_retry rides out transient TRUNCATE windows) instead of
        # hanging for the server default (~1 year).
        conn.autocommit = True
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.execute("SET SESSION lock_wait_timeout = 120")
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 30")
        for i, sp in enumerate(spec_ids, 1):
            print(f"[{datetime.now(timezone.utc).isoformat()}] sweeping spec {sp} ({i}/{len(spec_ids)})...")

            for item_id, rc, mt, md in databaseConnector.fetch_item_spec_usage(conn, cursor, season, sp):
                spec_runs[str(item_id)][sp] += int(rc)
                spec_maxtimed[str(item_id)][sp] = max(spec_maxtimed[str(item_id)][sp], int(mt or 0))
                spec_maxdep[str(item_id)][sp] = max(spec_maxdep[str(item_id)][sp], int(md or 0))

            for item_id, gem, rc in databaseConnector.fetch_item_socket_usage(conn, cursor, season, sp):
                if gem is None:
                    continue
                gem_runs[str(item_id)][sp][str(gem)] += int(rc)

            for item_id, bonus, rc in databaseConnector.fetch_item_bonus_usage(conn, cursor, season, sp):
                if not bonus:
                    continue
                variant_runs[str(item_id)][sp][bonus] += int(rc)

            for item_id, did, lvl, timed, depleted in databaseConnector.fetch_item_dungeon_usage(conn, cursor, season, sp):
                d = dungeon_runs[str(item_id)][sp][str(did)]
                timed = int(timed or 0)
                depleted = int(depleted or 0)
                runs = timed + depleted
                d["runs"] += runs
                d["timed"] += timed
                d["depleted"] += depleted
                d["by_key"][int(lvl)] += runs
                if timed > 0 and int(lvl) > d["max_key"]:
                    d["max_key"] = int(lvl)

        # Item-independent run totals, for adoption-rate (% of runs) metrics.
        print(f"[{datetime.now(timezone.utc).isoformat()}] fetching run-total denominators...")
        spec_total = databaseConnector.fetch_spec_total_runs(conn, cursor, season)
        dungeon_spec_total = databaseConnector.fetch_dungeon_spec_total_runs(conn, cursor, season)
        keylevel_spec_total = databaseConnector.fetch_spec_keylevel_total_runs(conn, cursor)
        dungeon_total_global = {
            str(r["dungeon_id"]): r["total_runs"]
            for r in databaseConnector.fetch_runs_per_dungeon(conn, cursor, season)
        }
        keylevel_total_global = defaultdict(int)
        dungeon_keylevel_total_global = {}
        for r in databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, season):
            keylevel_total_global[int(r["keystone_level"])] += r["total_runs"]
            dungeon_keylevel_total_global[(str(r["dungeon_id"]), int(r["keystone_level"]))] = r["total_runs"]

        # "Best in slot" signals (item-level), fetched once.
        print(f"[{datetime.now(timezone.utc).isoformat()}] fetching BiS / enchant signals...")
        simc_bis_by_item = defaultdict(list)
        for sp, iid, dps in databaseConnector.fetch_simc_bis_rank1(conn, cursor, season):
            simc_bis_by_item[str(iid)].append({
                "spec_id": int(sp),
                "dps_pct": round(float(dps), 1) if dps is not None else None,
            })

        top50_totals = databaseConnector.fetch_top50_loadout_totals(conn, cursor, season)
        top_specs_by_item = defaultdict(list)
        for sp, iid, cnt in databaseConnector.fetch_top50_item_counts(conn, cursor, season):
            total = top50_totals.get(str(sp))
            if not total:
                continue
            pct = round(min(100.0, int(cnt) / total * 100), 1)
            if pct >= TOP50_THRESHOLD:
                top_specs_by_item[str(iid)].append({"spec_id": int(sp), "pct": pct})

        # Most-used enchant per (spec, slot_group) and globally per slot_group.
        ench_spec_sg = defaultdict(lambda: defaultdict(int))
        ench_spec_sg_total = defaultdict(int)
        ench_global_sg = defaultdict(lambda: defaultdict(int))
        ench_global_sg_total = defaultdict(int)
        for sp, sg, eid, rc in databaseConnector.fetch_enchant_slotgroup_usage(conn, cursor, season):
            if eid is None or eid not in enchant_lookup:
                continue
            rc = int(rc)
            ench_spec_sg[(str(sp), sg)][eid] += rc
            ench_spec_sg_total[(str(sp), sg)] += rc
            ench_global_sg[sg][eid] += rc
            ench_global_sg_total[sg] += rc

    def _top_enchant(emap, total):
        if not emap:
            return None
        eid, runs = max(emap.items(), key=lambda x: x[1])
        return {"id": eid, "pct": round(runs / total * 100, 1) if total else None}

    enchant_global_top = {sg: _top_enchant(ench_global_sg[sg], ench_global_sg_total[sg])
                          for sg in ench_global_sg}
    enchant_spec_top = {key: _top_enchant(ench_spec_sg[key], ench_spec_sg_total[key])
                        for key in ench_spec_sg}

    # Per-spec total maps (built once) for the bySpec scopes.
    dungeon_totals_by_spec = defaultdict(dict)
    for (sp, did), v in dungeon_spec_total.items():
        dungeon_totals_by_spec[sp][did] = v
    keylevel_totals_by_spec = defaultdict(dict)
    for (sp, lvl), v in keylevel_spec_total.items():
        keylevel_totals_by_spec[sp][lvl] = v

    # ---- assemble per-item payloads -------------------------------------
    payloads = {}        # item_id (str) -> full payload dict (rendered + embedded)
    manifest = []
    for item_id, per_spec in spec_runs.items():
        item = item_lookup.get(int(item_id)) if item_id.isdigit() else None
        if not item:
            continue  # not an item we can render (no static data)

        label, key = slot_for_item(item)
        total_runs = sum(per_spec.values())

        # Spec ranking by adoption rate = % of that spec's runs that use the item
        # (share = % of the item's total usage, kept for context). Ranking by
        # adoption surfaces the specs the item matters most to, rather than just
        # the most-played specs.
        specs_rank = []
        for sp, runs in per_spec.items():
            denom = spec_total.get(sp)
            adoption = round(min(100.0, runs / denom * 100), 1) if denom else None
            specs_rank.append({
                "spec_id": int(sp),
                "runs": int(runs),
                "adoption": adoption,
                "share_pct": round(runs / (total_runs or 1) * 100, 1),
                "max_timed_key": spec_maxtimed[item_id].get(sp, 0),
                "max_depleted_key": spec_maxdep[item_id].get(sp, 0),
            })
        specs_rank.sort(key=lambda x: (x["adoption"] if x["adoption"] is not None else -1, x["runs"]),
                        reverse=True)
        # For the browse card's "mostly X" hint, use the highest-usage spec.
        top_spec = int(max(per_spec, key=per_spec.get)) if per_spec else None

        # merge per-spec counters into global counters
        g_gems = defaultdict(int)
        for sp_map in gem_runs[item_id].values():
            for gem, rc in sp_map.items():
                g_gems[gem] += rc
        g_variants = defaultdict(int)
        for sp_map in variant_runs[item_id].values():
            for bonus, rc in sp_map.items():
                g_variants[bonus] += rc
        g_dungeons = {}
        for sp_map in dungeon_runs[item_id].values():
            for did, d in sp_map.items():
                gd = g_dungeons.setdefault(
                    did, {"runs": 0, "timed": 0, "depleted": 0, "max_key": 0, "by_key": defaultdict(int)})
                gd["runs"] += d["runs"]
                gd["timed"] += d["timed"]
                gd["depleted"] += d["depleted"]
                gd["max_key"] = max(gd["max_key"], d["max_key"])
                for lvl, rc in d["by_key"].items():
                    gd["by_key"][lvl] += rc

        global_scope = build_scope(
            sum(per_spec.values()),
            max(spec_maxtimed[item_id].values(), default=0),
            max(spec_maxdep[item_id].values(), default=0),
            g_gems, g_variants, g_dungeons, gem_lookup, bonus_lookup,
            dungeon_total_global, keylevel_total_global,
            embellishment_lookup, missive_lookup, item_lookup,
            dungeon_keylevel_total_global, reagent_lookup,
        )
        global_scope["specs"] = specs_rank

        by_spec = {}
        for sp in per_spec:
            by_spec[sp] = build_scope(
                per_spec[sp],
                spec_maxtimed[item_id].get(sp, 0),
                spec_maxdep[item_id].get(sp, 0),
                gem_runs[item_id].get(sp, {}),
                variant_runs[item_id].get(sp, {}),
                dungeon_runs[item_id].get(sp, {}),
                gem_lookup, bonus_lookup,
                dungeon_totals_by_spec.get(sp, {}), keylevel_totals_by_spec.get(sp, {}),
                embellishment_lookup, missive_lookup, item_lookup,
                None, reagent_lookup,
            )

        # Commonly-paired enchant for this slot (per scope; only enchantable slots
        # have data, others resolve to None).
        global_scope["enchant"] = resolve_enchant(enchant_global_top.get(key), enchant_lookup)
        for sp in by_spec:
            by_spec[sp]["enchant"] = resolve_enchant(enchant_spec_top.get((sp, key)), enchant_lookup)

        # set membership (other equipped pieces)
        set_block = None
        sid = item.get("itemSetId")
        if sid and set_members.get(sid):
            pieces = []
            for pid in set_members[sid]:
                p = item_lookup.get(pid, {})
                pieces.append({
                    "id": pid,
                    "name": p.get("name", f"Item {pid}"),
                    "icon": p.get("icon", ""),
                    "quality": p.get("quality", 0),
                })
            set_block = {"id": sid, "pieces": pieces}

        top_variant = global_scope["variants"][0]["bonus"] if global_scope["variants"] else ""

        payload = {
            "id": int(item_id),
            "name": item.get("name", f"Item {item_id}"),
            "icon": item.get("icon", ""),
            "quality": item.get("quality", 0),
            "slot": label,
            "slotKey": key,
            "ilvl": item.get("itemLevel"),
            "itemClass": item.get("itemClass"),
            "itemSubClass": item.get("itemSubClass"),
            "stats": item.get("stats", []),
            "sockets": (item.get("socketInfo") or {}).get("sockets", []),
            "uniqueEquipped": item.get("uniqueEquipped", False),
            "onUseTrinket": item.get("onUseTrinket", False),
            "weaponType": WEAPON_SUBCLASS.get(item.get("itemSubClass")) if item.get("itemClass") == 2 else None,
            "set": set_block,
            "wowheadBonus": top_variant,
            "simc_bis_specs": sorted(simc_bis_by_item.get(item_id, []),
                                     key=lambda x: -(x["dps_pct"] or 0)),
            "top_specs": sorted(top_specs_by_item.get(item_id, []), key=lambda x: -x["pct"]),
            "global": global_scope,
            "bySpec": by_spec,
        }

        payloads[item_id] = payload
        manifest.append({
            "id": int(item_id),
            "name": payload["name"],
            "icon": payload["icon"],
            "quality": payload["quality"],
            "slot": label,
            "slotKey": key,
            "slug": slug_map[int(item_id)],
            "runs": int(total_runs),
            "top_spec": top_spec,
        })

    manifest.sort(key=lambda x: x["runs"], reverse=True)
    os.makedirs(os.path.join("assets", "json"), exist_ok=True)
    with open(os.path.join("assets", "json", "items_index.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, separators=(",", ":"), ensure_ascii=False)
    print(f"[{datetime.now(timezone.utc).isoformat()}] assembled {len(manifest)} item payloads + manifest")

    # Per-slot lists (manifest already sorted by runs desc) for the "other popular
    # items in this slot" card on each item page.
    by_slot = defaultdict(list)
    for m in manifest:
        by_slot[m["slotKey"]].append(m)

    # ---- render templates ----------------------------------------------
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(items_dir, exist_ok=True)

    # One fully server-rendered page per item, at items/<slug>.html. In debug
    # mode we only render a single item page (the one named by --item, matched on
    # id or slug, else the most-used item) so the template can be eyeballed
    # quickly without writing thousands of files.
    render_items = list(payloads.items())
    if debug:
        chosen = None
        if target_item:
            for iid, pl in payloads.items():
                if str(pl["id"]) == str(target_item) or slug_map[int(iid)] == target_item:
                    chosen = iid
                    break
            if chosen is None:
                print(f"--item '{target_item}' not found; falling back to the most-used item.")
        if chosen is None and manifest:
            chosen = str(manifest[0]["id"])   # manifest is sorted by runs desc
        render_items = [(chosen, payloads[chosen])] if chosen else []
        print(f"[debug] rendering only item {chosen} ({slug_map[int(chosen)] if chosen else 'none'})")

    item_tmpl = env.get_template("item.html")
    for item_id, payload in render_items:
        slug = slug_map[int(item_id)]
        alternatives = [a for a in by_slot.get(payload["slotKey"], [])
                        if a["id"] != payload["id"]][:12]
        item_html = item_tmpl.render(
            item=payload,
            slug=slug,
            slug_map=slug_map,
            alternatives=alternatives,
            active_page="items",
            cur_page="items",
            breadcrumbs=[
                {"title": "Pages", "href": "/pages"},
                {"title": "Items", "href": "/pages/items"},
                {"title": payload["name"], "href": f"/items/{slug}"},
            ],
            spec_nav=spec_nav,
            dungeon_nav=dungeon_nav,
            season_info=season_info,
            notifications=notifications,
            specs_map=specs_map,
            dungeons_map=dungeons_map,
        )
        with open(os.path.join(items_dir, f"{slug}.html"), "w", encoding="utf-8") as f:
            f.write(item_html)
    print(f"[{datetime.now(timezone.utc).isoformat()}] wrote {len(render_items)} item page(s) to {items_dir}/")

    # The browse grid page (filterable list of all items) at pages/items.html.
    page = env.get_template(os.path.basename(template_path))
    page_html = page.render(
        active_page="items",
        cur_page="items",
        breadcrumbs=[
            {"title": "Pages", "href": "/pages"},
            {"title": "Items", "href": "/pages/items"},
        ],
        item_count=len(manifest),
        spec_nav=spec_nav,
        dungeon_nav=dungeon_nav,
        season_info=season_info,
        notifications=notifications,
        specs_map=specs_map,
        dungeons_map=dungeons_map,
    )
    with open(os.path.join(output_dir, "items.html"), "w", encoding="utf-8") as f:
        f.write(page_html)
    print(f"Generated {os.path.join(output_dir, 'items.html')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the items browse page and one static page per item")
    parser.add_argument("--template", required=True, help="Path to the items browse page template")
    parser.add_argument("--output_dir", required=True, help="Directory to write the browse page (pages/)")
    parser.add_argument("--items_dir", default="items", help="Directory to write per-item pages (default: items)")
    parser.add_argument("--debug", action="store_true",
                        help="Render only the browse page and a single item page")
    parser.add_argument("--item", dest="target_item",
                        help="In --debug, the item id or slug to render (default: most-used item)")
    args = parser.parse_args()
    main(args.template, args.output_dir, args.items_dir, args.debug, args.target_item)
