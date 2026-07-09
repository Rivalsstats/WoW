"""SimulationCraft "best item per slot" collector.

Runs continuously inside the collector container (registered alongside
``run_raiderio_top_loadouts``). For each DPS / Tank spec it:

  1. Builds the candidate "bag" per slot from our most-popular loadout data
     (top-N most-common items per slot + most-common talent loadout).
  2. Detects the tier slots dynamically via Blizzard ``itemSetId``.
  3. Runs a small tier-scenario sweep to decide which slots wear the set and
     locks those slots to the tier piece.
  4. Evaluates whole-set combinations (Raidbots "Top Gear" style): the cartesian
     product of each non-tier slot's candidate bag, pruning any set that breaks
     an equip limit (<=2 embellishments via itemLimit category 512, no duplicate
     unique-equipped item, other itemLimit categories), and evaluating each legal
     set as a full profileset in a single simc invocation. The bag is trimmed
     (least-popular first) so the product fits ``SIMC_MAX_COMBINATIONS``.
  5. Derives a per-slot ranking from the full-set DPS results and persists it to
     ``simc_bis_meta`` / ``simc_bis_items`` for the page build's "SIM" badge.

Enchants and gems are held constant rather than searched: every candidate
carries the top-50 players' most popular enchant for its slot group and fills
its sockets from the spec-wide gem ranking (see apply_enchants_and_gems), so
they never add profilesets — they only make absolute DPS (and thus the
cross-spec tierlist built from ``baseline_dps``) more realistic.

SimulationCraft itself is executed as a short-lived sibling Docker container
(``docker run --rm``) over a shared volume, so watchtower keeps simc patch-current.
Set ``SIMC_BIN`` to run a local binary instead (used for local debugging).

Profilesets are the core mechanism: one baseline set is simulated, then each
combination overrides its (non-locked) gear slots and is evaluated in isolation.
One simc invocation evaluates every combination and emits JSON (``json2``) with
``sim.profilesets.results[]``.
"""

import os
import re
import json
import asyncio
import argparse
import itertools
from datetime import datetime, timezone
from pathlib import Path

import databaseConnector


# --------------------------------------------------------------------------
# Configuration (env-overridable)
# --------------------------------------------------------------------------

DATA_DIR = Path("data")
STATIC_DIR = DATA_DIR / "static"

SIMC_BIN = os.environ.get("SIMC_BIN")  # if set, run a local binary instead of docker
# Official image (https://hub.docker.com/r/simulationcraftorg/simc). Its ENTRYPOINT
# is "./simc", so we pass only the profile + options as the container command.
SIMC_DOCKER_IMAGE = os.environ.get("SIMC_DOCKER_IMAGE", "simulationcraftorg/simc:latest")
SIMC_CMD = os.environ.get("SIMC_CMD", "")  # extra leading arg before the profile (usually empty)
SIMC_IO_DIR = Path(os.environ.get("SIMC_IO_DIR", str(DATA_DIR / "simc_io")))  # our side of the shared dir
# Named docker volume shared with the sibling container (set in production compose).
# When empty (e.g. local testing), we bind-mount the absolute SIMC_IO_DIR instead.
SIMC_IO_VOLUME = os.environ.get("SIMC_IO_VOLUME", "")
SIMC_PULL_INTERVAL = int(os.environ.get("SIMC_PULL_INTERVAL", str(6 * 60 * 60)))  # self-pull cadence (s)
SIMC_THREADS = os.environ.get("SIMC_THREADS", "2")
SIMC_CPUS = os.environ.get("SIMC_CPUS")  # optional docker --cpus hard cap
# Relative scheduling weight (docker --cpu-shares; default weight is 1024). Unlike
# SIMC_CPUS this is NOT a hard cap: it only matters when CPUs are contended, so a
# low value lets simc burst to 100% of every core when nothing else needs them but
# yields to other containers/processes under load. Linux enforces a minimum of 2.
SIMC_CPU_SHARES = os.environ.get("SIMC_CPU_SHARES", "128")
# Hard CPU pin (docker --cpuset-cpus, e.g. "1" or "1-3"). On a small host this is
# stronger than nano_cpus/cpu_shares: it guarantees simc NEVER runs on the
# reserved core(s), so a DB or other service pinned to a *different* core is never
# preempted by simc. cpu_shares only arbitrates between cgroups, which does not
# help against a host-level mysqld; cpuset does. Leave unset to let simc float
# across all cores (old behaviour).
SIMC_CPUSET = os.environ.get("SIMC_CPUSET")
# Optional hard memory cap (docker --memory, e.g. "1g") so a large profileset pass
# can't push the host into swap and stall a co-located DB. Unset = no limit.
SIMC_MEM_LIMIT = os.environ.get("SIMC_MEM_LIMIT")
# Optional block-IO weight (docker --blkio-weight, 10-1000; default 500). A low
# value makes simc's profileset disk writes yield bandwidth under contention.
SIMC_BLKIO_WEIGHT = os.environ.get("SIMC_BLKIO_WEIGHT")
SIMC_PROFILESET_WORK_THREADS = os.environ.get("SIMC_PROFILESET_WORK_THREADS", "1")
SIMC_ITERATIONS = os.environ.get("SIMC_ITERATIONS")  # e.g. "5000"; if unset, use target_error
SIMC_TARGET_ERROR = os.environ.get("SIMC_TARGET_ERROR", "0.1")
SIMC_RUN_TIMEOUT = int(os.environ.get("SIMC_RUN_TIMEOUT", str(8 * 60 * 60)))  # seconds per invocation
# Applied to every sibling simc container we launch so it can be found and torn
# down independently of our own process state (e.g. when watchtower replaces this
# collector container, these siblings aren't tracked/updated by watchtower at all).
SIMC_CONTAINER_LABEL = {"mythistone.role": "simc-sim"}
SIMC_CANDIDATES_PER_SLOT = int(os.environ.get("SIMC_CANDIDATES_PER_SLOT", "10"))
# Top-Gear combination budget: hard cap on the number of full-set profilesets we
# evaluate per spec. The per-slot candidate "bag" is trimmed (least-popular items
# first) until its cartesian product fits this cap. One simc invocation handles
# them all as profilesets.
SIMC_MAX_COMBINATIONS = int(os.environ.get("SIMC_MAX_COMBINATIONS", "2000"))
# Maximum iteration count for the combination pass: every combo converges to
# SIMC_TARGET_ERROR but is capped here, stopping at whichever comes first. The cap
# stops a slow-converging (high-variance) combo from running unbounded. Set to
# empty/0 to fall back to SIMC_ITERATIONS (else: no cap, target_error alone).
SIMC_COMBO_ITERATIONS = os.environ.get("SIMC_COMBO_ITERATIONS", "5000")
# Drop slot candidates used by fewer than this fraction of the slot's most-popular
# item (filters stale/old-expansion items that pollute the aggregated pool).
SIMC_MIN_CANDIDATE_FRACTION = float(os.environ.get("SIMC_MIN_CANDIDATE_FRACTION", "0.02"))
SIMC_SPEC_SLEEP = float(os.environ.get("SIMC_SPEC_SLEEP", "30"))  # pause between specs
# Suppress repeated identical Discord alerts for this many seconds.
SIMC_ALERT_THROTTLE = int(os.environ.get("SIMC_ALERT_THROTTLE", "3600"))


def _resolve_level():
    """Character level for the simulated profile, resolved from (in order):
    the SIMC_LEVEL env override, the `max_character_level` collected into
    seasonInfo.json (derived from wago.tools ContentTuning), then a fallback.
    """
    env = os.environ.get("SIMC_LEVEL")
    if env:
        return str(env)
    try:
        si = json.loads((STATIC_DIR / "seasonInfo.json").read_text(encoding="utf-8"))
        lvl = si.get("max_character_level")
        if lvl:
            return str(int(lvl))
    except Exception:
        pass
    return "90"


SIMC_LEVEL = _resolve_level()

# Blizzard equipment slot type (as stored in global_aggregated_equipment.slot) -> simc slot keyword.
DB_TO_SIMC_SLOT = {
    "HEAD": "head",
    "NECK": "neck",
    "SHOULDER": "shoulders",
    "BACK": "back",
    "CHEST": "chest",
    "WRIST": "wrists",
    "HANDS": "hands",
    "WAIST": "waist",
    "LEGS": "legs",
    "FEET": "feet",
    "FINGER_1": "finger1",
    "FINGER_2": "finger2",
    "TRINKET_1": "trinket1",
    "TRINKET_2": "trinket2",
    "MAIN_HAND": "main_hand",
    "OFF_HAND": "off_hand",
}
ALL_SLOTS = list(DB_TO_SIMC_SLOT.keys())

# Paired ring/trinket slots. The spec page (generateSpecPages.fetch_slot_info)
# ranks the DISTINCT items across the whole pair from the slot GROUP and assigns
# one to each numbered slot, rather than ranking each numbered slot on its own —
# the game fills FINGER_1/FINGER_2 fairly arbitrarily, so a per-slot ranking can
# surface the SAME ring as the top item for both slots. We mirror the page here so
# our baseline ("default starting profile") equals the gear shown on the spec page.
MULTI_SLOT_GROUPS = {
    "FINGER_1": "FINGER",
    "FINGER_2": "FINGER",
    "TRINKET_1": "TRINKET",
    "TRINKET_2": "TRINKET",
}

# Blizzard inventoryType values that can carry a tier set bonus (armor pieces).
# 1=head, 3=shoulder, 5=chest, 20=robe(chest), 7=legs, 10=hands.
TIER_INVTYPES = {1, 3, 5, 20, 7, 10}
TIER_INVTYPE_TO_SLOT = {1: "HEAD", 3: "SHOULDER", 5: "CHEST", 20: "CHEST", 7: "LEGS", 10: "HANDS"}

# Two-hand / ranged inventory types: when the main hand is one of these the
# off-hand slot does not exist and must be skipped.
TWO_HAND_INVTYPES = {17, 15, 25, 26}

# Specs that dual-wield two-handers (Titan's Grip Fury). For these the "2H main
# hand => no off-hand" rule is wrong: they equip a two-hander in BOTH hands, so
# the off-hand must be kept even though the main hand is a 2H. (Single-Minded
# Fury uses one-handers, so its main hand isn't a 2H and the rule never fires.)
DUAL_WIELD_TWOHAND_SPECS = {72}  # Fury Warrior

# simc class assignment keyword (no underscores), keyed by Blizzard class name.
CLASS_TOKENS = {
    "death knight": "deathknight",
    "demon hunter": "demonhunter",
    "druid": "druid",
    "evoker": "evoker",
    "hunter": "hunter",
    "mage": "mage",
    "monk": "monk",
    "paladin": "paladin",
    "priest": "priest",
    "rogue": "rogue",
    "shaman": "shaman",
    "warlock": "warlock",
    "warrior": "warrior",
}

# A valid race per class. Race is constant across every profileset of a spec, so
# it cancels out of the per-slot ranking entirely; it only needs to be valid.
DEFAULT_RACE = {
    "deathknight": "orc",
    "demonhunter": "blood_elf",
    "druid": "night_elf",
    "evoker": "dracthyr",
    "hunter": "orc",
    "mage": "gnome",
    "monk": "pandaren",
    "paladin": "blood_elf",
    "priest": "human",
    "rogue": "orc",
    "shaman": "orc",
    "warlock": "orc",
    "warrior": "orc",
}

# role int (specs.json) -> we only simulate dps (2) and tank (0); healers (1) are skipped.
SIMULATED_ROLES = {0, 2}

# Specs excluded from topgear regardless of role. Augmentation (1473) is a DPS
# spec but a *support* one: simc disables single_actor_batch for it and sims the
# whole group per profileset, so a full topgear run needs ~30h and never fits the
# per-spec timeout (see _summarize_simc_progress). Skip it like the healers.
SKIPPED_SPEC_IDS = {1473}


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def _log(msg):
    print(f"[simcBis {datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _stat_log(stats, msg):
    if stats is not None:
        try:
            stats.console_log(msg)
            return
        except Exception:
            pass
    _log(msg)


async def _alert(reporter, stats, title, message, level="error", throttle_key=None):
    """Log and (best-effort) push an alert embed to Discord."""
    _stat_log(stats, f"simc ALERT[{level}] {title}: {message}")
    if reporter is not None:
        try:
            await reporter.send_alert(
                title, message, level=level,
                throttle_key=throttle_key, throttle_seconds=SIMC_ALERT_THROTTLE,
            )
        except Exception as e:
            _log(f"failed to send discord alert: {e}")


def slug(name):
    return (name or "").lower().replace("'", "").strip()


def spec_slug(name):
    return slug(name).replace(" ", "_")


def class_token(class_name):
    return CLASS_TOKENS.get(slug(class_name).replace("_", " "))


def load_static():
    specs = json.loads((STATIC_DIR / "specs.json").read_text(encoding="utf-8"))
    classes = json.loads((STATIC_DIR / "classes.json").read_text(encoding="utf-8"))
    return specs, classes


def load_item_lookup():
    """id -> item dict from equippable-items.json (has inventoryType, itemSetId,
    uniqueEquipped and itemLimit:{category,quantity})."""
    items = json.loads((STATIC_DIR / "equippable-items.json").read_text(encoding="utf-8"))
    return {int(i["id"]): i for i in items if i.get("id") is not None}


_EMBELLISH_BONUS_IDS = None


def load_embellishment_bonus_ids():
    """Set of bonus_id strings that apply an embellishment.

    embellishments.json maps embellishment bonus_id -> reagent item_id. Every
    embellishment reagent shares itemLimit {category: 512, quantity: 2}, so a
    crafted item carries an embellishment (and counts toward that cap) when any
    of its bonus_ids is one of these keys."""
    global _EMBELLISH_BONUS_IDS
    if _EMBELLISH_BONUS_IDS is None:
        try:
            data = json.loads((STATIC_DIR / "embellishments.json").read_text(encoding="utf-8"))
            _EMBELLISH_BONUS_IDS = {str(k) for k in data.keys()}
        except Exception as e:
            _log(f"could not load embellishments.json: {e}")
            _EMBELLISH_BONUS_IDS = set()
    return _EMBELLISH_BONUS_IDS


# Embellishment item-limit category/quantity (Blizzard crafting category 512).
EMBELLISH_LIMIT_CATEGORY = 512
EMBELLISH_LIMIT_QUANTITY = 2


_BONUS_SOCKET_COUNTS = None


def load_bonus_socket_counts():
    """bonus_id (str) -> number of sockets that bonus grants (bonuses.json)."""
    global _BONUS_SOCKET_COUNTS
    if _BONUS_SOCKET_COUNTS is None:
        try:
            data = json.loads((STATIC_DIR / "bonuses.json").read_text(encoding="utf-8"))
            _BONUS_SOCKET_COUNTS = {
                str(k): int(v.get("socket", 0))
                for k, v in data.items()
                if isinstance(v, dict) and v.get("socket")
            }
        except Exception as e:
            _log(f"could not load bonuses.json: {e}")
            _BONUS_SOCKET_COUNTS = {}
    return _BONUS_SOCKET_COUNTS


_ENCHANT_STATIC = None


def load_enchant_static():
    """(valid_enchant_ids, gem_lookup) from enchantments.json.

    valid_enchant_ids: set of int enchantment ids the static data knows — used
    to drop stale/bogus ids the same way the spec page's fetch_enchant_info
    does. gem_lookup: gem item_id (int) -> {limit_category, limit_quantity}
    for entries with slot == "socket" (itemLimitCategory caps unique gems).
    """
    global _ENCHANT_STATIC
    if _ENCHANT_STATIC is None:
        valid, gems = set(), {}
        try:
            data = json.loads((STATIC_DIR / "enchantments.json").read_text(encoding="utf-8"))
            for e in data:
                if e.get("id") is not None:
                    valid.add(int(e["id"]))
                if e.get("slot") == "socket" and e.get("itemId") is not None:
                    lim = e.get("itemLimitCategory") or {}
                    gems[int(e["itemId"])] = {
                        "limit_category": lim.get("id"),
                        "limit_quantity": lim.get("quantity"),
                    }
        except Exception as ex:
            _log(f"could not load enchantments.json: {ex}")
        _ENCHANT_STATIC = (valid, gems)
    return _ENCHANT_STATIC


def enchant_group(slot):
    """Group key for enchant popularity lookups.

    top_player_loadout_enchants stores the raw Blizzard slot the collector saw
    (FINGER_1, MAIN_HAND, ...) while the aggregated fallback table keys real
    slot groups (FINGER, WEAPON). Both are normalised to the group form.
    """
    if slot in MULTI_SLOT_GROUPS:
        return MULTI_SLOT_GROUPS[slot]
    if slot in ("MAIN_HAND", "OFF_HAND"):
        return "WEAPON"
    return slot


def bonus_to_simc(bonus_list):
    """DB bonus_list (comma string) -> simc bonus_id value (slash-separated)."""
    if not bonus_list:
        return None
    ids = [b.strip() for b in str(bonus_list).split(",") if b.strip()]
    return "/".join(ids) if ids else None


# --------------------------------------------------------------------------
# Candidate gathering & tier detection
# --------------------------------------------------------------------------

def fetch_slot_rows(conn, cursor, spec_id, season, slot, group_cache=None):
    """Per-slot candidate rows (most-popular first), matching the spec page's
    ``generateSpecPages.fetch_slot_info`` so our baseline equals the page's gear.

    For paired ring/trinket slots we rank the distinct items across the GROUP and
    drop the slot's positional index (FINGER_1 -> drop the #1 item, FINGER_2 ->
    drop the #2 item). That hands the two slots the two most-popular *different*
    items, instead of each numbered slot's own top item — which is frequently the
    same ring/trinket in both and never appears that way on the spec page.

    The group query is the same for both numbered slots of a pair, so when a
    ``group_cache`` dict is supplied we run it once per group and reuse the result
    (the heavier slot_group_map join would otherwise run twice per pair).
    """
    group = MULTI_SLOT_GROUPS.get(slot)
    if group:
        if group_cache is not None and group in group_cache:
            base = group_cache[group]
        else:
            base = databaseConnector.fetch_top_items_for_slot_group_with_bonus(
                conn, cursor, spec_id, season, group
            )
            if group_cache is not None:
                group_cache[group] = base
        rows = list(base)  # copy: we drop a positional index per numbered slot
        idx = int(slot.rsplit("_", 1)[1]) - 1  # FINGER_1 -> 0, FINGER_2 -> 1
        if 0 <= idx < len(rows):
            del rows[idx]
        return rows
    return databaseConnector.fetch_top_items_for_slot_with_bonus(
        conn, cursor, spec_id, season, slot
    )


def gather_candidates(conn, cursor, spec_id, season, item_lookup):
    """slot -> ordered list of candidate dicts (most-popular first).

    Each candidate: {item_id, count, bonus_list, simc_bonus, item_set_id, inv_type}.

    Rare/stale items are dropped: the aggregated pool occasionally surfaces old
    expansions' items (e.g. a Legion ring) that get current-season bonus_ids
    applied and produce nonsense in simc. We keep only candidates whose equip
    count is at least SIMC_MIN_CANDIDATE_FRACTION of the slot's most-popular item
    (the top item always passes).
    """
    embellish_ids = load_embellishment_bonus_ids()
    socket_bonus_counts = load_bonus_socket_counts()
    out = {}
    group_cache = {}  # slot_group -> group rows, so each pair's query runs once
    for slot in ALL_SLOTS:
        rows = fetch_slot_rows(conn, cursor, spec_id, season, slot, group_cache)
        if not rows:
            continue
        top_count = max((int(r.get("count", 0)) for r in rows), default=0)
        floor = top_count * SIMC_MIN_CANDIDATE_FRACTION
        cands = []
        for r in rows[:SIMC_CANDIDATES_PER_SLOT]:
            count = int(r.get("count", 0))
            if count < floor:
                continue
            item_id = int(r["item"])
            bonus_list = (r.get("bonus") or {}).get("ids") if r.get("bonus") else None
            meta = item_lookup.get(item_id, {})
            # bonus_list is a comma-separated string (e.g. "8791,12384,..."); split
            # into ids before testing membership — iterating the raw string would
            # walk it character-by-character and never match an embellishment id.
            bonus_ids = (
                [b.strip() for b in str(bonus_list).split(",") if b.strip()]
                if bonus_list else []
            )
            has_embellishment = any(b in embellish_ids for b in bonus_ids)
            # Socket count: sockets granted by the equipped bonus_ids, raised to
            # the item's inherent socket count (mirrors the spec page's
            # convert_slots so the simmed item matches the one shown there).
            socket_count = sum(socket_bonus_counts.get(b, 0) for b in bonus_ids)
            inherent = len((meta.get("socketInfo") or {}).get("sockets") or [])
            socket_count = max(socket_count, inherent)
            cands.append(
                {
                    "item_id": item_id,
                    "count": count,
                    "bonus_list": bonus_list,
                    "simc_bonus": bonus_to_simc(bonus_list),
                    "item_set_id": meta.get("itemSetId"),
                    "inv_type": meta.get("inventoryType"),
                    "unique_equipped": bool(meta.get("uniqueEquipped")),
                    "item_limit": meta.get("itemLimit"),
                    "has_embellishment": has_embellishment,
                    "socket_count": socket_count,
                }
            )
        if cands:
            out[slot] = cands
    return out


# --------------------------------------------------------------------------
# Enchants & gems (held constant, sourced from the top-50 player loadouts)
# --------------------------------------------------------------------------

def fetch_enchant_map(conn, cursor, spec_id, season):
    """Enchant group -> most popular valid enchantment_id.

    Primary source is the top-50 player loadouts; groups with no top-50 data
    fall back to the global aggregation (same source as the spec page's
    enchant dropdowns). Ids unknown to enchantments.json are dropped, matching
    the page's fetch_enchant_info filtering.
    """
    valid_ids, _ = load_enchant_static()
    merged = {}  # group -> {enchant_id: count}
    try:
        raw = databaseConnector.fetch_top50_enchant_ranking(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"could not fetch top-50 enchants for spec {spec_id}: {e}")
        raw = {}
    for sg, pairs in raw.items():
        grp = enchant_group(sg)
        for eid, cnt in pairs:
            if eid in valid_ids:
                merged.setdefault(grp, {})
                merged[grp][eid] = merged[grp].get(eid, 0) + cnt

    out = {grp: max(counts.items(), key=lambda x: x[1])[0]
           for grp, counts in merged.items() if counts}

    needed = {enchant_group(s) for s in ALL_SLOTS}
    for grp in sorted(needed - set(out)):
        try:
            rows = databaseConnector.fetch_top_enchant_for_slot(
                conn, cursor, spec_id, season, grp, 5
            )
        except Exception:
            rows = []
        for row in rows or []:
            eid = row.get("enchantment_id") if isinstance(row, dict) else row[0]
            if eid is not None and int(eid) in valid_ids:
                out[grp] = int(eid)
                break
    return out


def fetch_gem_ranking(conn, cursor, spec_id, season):
    """Spec-wide gem popularity, most popular first (gem item ids).

    Top-50 loadouts primary, global socket aggregation fallback. Only gems
    known to enchantments.json survive (simc rejects unknown gem ids).
    """
    _, gem_lookup = load_enchant_static()
    ranked = []
    try:
        ranked = databaseConnector.fetch_top50_gem_ranking(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"could not fetch top-50 gems for spec {spec_id}: {e}")
    if not ranked:
        try:
            ranked = databaseConnector.fetch_top_gems_spec_wide(conn, cursor, spec_id, season)
        except Exception as e:
            _log(f"could not fetch aggregated gems for spec {spec_id}: {e}")
    return [gid for gid, _ in ranked if gid in gem_lookup]


def apply_enchants_and_gems(candidates, enchant_map, gem_ranking, item_lookup):
    """Attach a constant enchant_id / gem_ids list to every candidate.

    Enchants and gems are deliberately NOT part of the search space: each
    candidate carries the same enchant/gems in every profileset it appears in,
    so they cancel out of the per-slot ranking and only move absolute DPS.

    Weapon enchants only land on actual weapons (itemClass 2), so off-hand
    shields/frills stay unenchanted like on the spec page.

    Gems respect itemLimitCategory caps (unique gems): slots consume a shared
    per-category budget in ALL_SLOTS order, sized by the slot's largest socket
    count. Only one candidate per slot is ever equipped, so per-slot
    consumption keeps every enumerated combo legal.
    """
    _, gem_lookup = load_enchant_static()
    cat_used = {}
    for slot in ALL_SLOTS:
        cands = candidates.get(slot)
        if not cands:
            continue
        grp = enchant_group(slot)
        ench = enchant_map.get(grp)

        max_sockets = max(c.get("socket_count", 0) for c in cands)
        slot_gems = []
        if max_sockets and gem_ranking:
            for gid in gem_ranking:
                if len(slot_gems) >= max_sockets:
                    break
                info = gem_lookup.get(gid, {})
                cat, qty = info.get("limit_category"), info.get("limit_quantity")
                if cat is not None and qty is not None:
                    take = min(qty - cat_used.get(cat, 0), max_sockets - len(slot_gems))
                    if take <= 0:
                        continue
                    slot_gems.extend([gid] * take)
                    cat_used[cat] = cat_used.get(cat, 0) + take
                else:
                    slot_gems.extend([gid] * (max_sockets - len(slot_gems)))
            if len(slot_gems) < max_sockets:
                # A ranking dominated by limit-capped gems can exhaust its
                # budgets before every socket is filled; a socket must never
                # stay empty, so pad with the most popular uncapped gem.
                filler = next(
                    (gid for gid in gem_ranking
                     if gem_lookup.get(gid, {}).get("limit_category") is None),
                    None,
                )
                if filler is not None:
                    slot_gems.extend([filler] * (max_sockets - len(slot_gems)))
                else:
                    _log(f"gem ranking for {slot} has no uncapped gem to fill "
                         f"{max_sockets - len(slot_gems)} remaining socket(s)")

        for cand in cands:
            if ench is not None:
                is_weapon = item_lookup.get(cand["item_id"], {}).get("itemClass") == 2
                if grp != "WEAPON" or is_weapon:
                    cand["enchant_id"] = ench
            n = cand.get("socket_count", 0)
            if n and slot_gems:
                cand["gem_ids"] = slot_gems[:n]


# --------------------------------------------------------------------------
# Equip-limit constraints (item-limit categories, unique-equipped)
# --------------------------------------------------------------------------

def candidate_limit_categories(cand):
    """Yield (category, max_quantity) limit contributions for a candidate:
    the item's own itemLimit (e.g. unique-equipped categories) plus the
    embellishment cap (category 512, quantity 2) when it carries one."""
    out = []
    lim = cand.get("item_limit")
    if lim and lim.get("category") is not None:
        out.append((lim["category"], lim.get("quantity")))
    if cand.get("has_embellishment"):
        out.append((EMBELLISH_LIMIT_CATEGORY, EMBELLISH_LIMIT_QUANTITY))
    return out


def set_is_valid(chosen):
    """True if a full equipped set respects every equip limit.

    chosen: dict slot -> candidate. Enforces unique-equipped (no duplicate of the
    same unique item across slots) and per-category itemLimit quantities (the
    embellishment cap, alchemist-stone-style unique categories, etc.)."""
    seen_unique = set()
    cat_counts = {}
    cat_limit = {}
    for cand in chosen.values():
        if not cand:
            continue
        if cand.get("unique_equipped"):
            iid = cand["item_id"]
            if iid in seen_unique:
                return False
            seen_unique.add(iid)
        for cat, qty in candidate_limit_categories(cand):
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
            if qty is not None:
                cat_limit[cat] = qty if cat not in cat_limit else min(cat_limit[cat], qty)
    for cat, n in cat_counts.items():
        q = cat_limit.get(cat)
        if q is not None and n > q:
            return False
    return True


def legalize_baseline_embellishments(baseline, candidates):
    """Demote excess embellished picks so a baseline respects the embellishment cap.

    The popular baseline takes the most-popular item per slot independently, which can
    equip more than EMBELLISH_LIMIT_QUANTITY embellishments — illegal in-game (only two
    ever apply), and simc would apply all of them and inflate the set's DPS. Keep the
    most-popular embellished picks up to the cap and swap every further embellished slot
    to its most-popular non-embellished candidate. Mutates and returns `baseline`.
    """
    emb_slots = [s for s, c in baseline.items() if c and c.get("has_embellishment")]
    if len(emb_slots) <= EMBELLISH_LIMIT_QUANTITY:
        return baseline
    # keep the most-popular embellished picks (highest equip count) up to the cap
    emb_slots.sort(key=lambda s: baseline[s].get("count", 0), reverse=True)
    for slot in emb_slots[EMBELLISH_LIMIT_QUANTITY:]:
        alt = next(
            (c for c in candidates.get(slot, []) if not c.get("has_embellishment")),
            None,
        )
        if alt is not None:
            _log(f"baseline: demoting embellished item {baseline[slot]['item_id']} in {slot} "
                 f"to non-embellished {alt['item_id']} to respect the "
                 f"{EMBELLISH_LIMIT_QUANTITY}-embellishment cap")
            baseline[slot] = alt
        else:
            _log(f"baseline: {slot} has no non-embellished candidate; keeping embellished "
                 f"item {baseline[slot]['item_id']} (set may exceed embellishment cap)")
    return baseline


def _combo_count(opts):
    """Cartesian size of a per-slot option bag (slot -> list of candidates)."""
    n = 1
    for v in opts.values():
        n *= len(v)
        if n > 10 ** 18:
            return n  # effectively unbounded; caller will trim
    return n


def trim_bag(opts, cap):
    """Trim the least-popular candidate from the bag's largest slot until the
    cartesian product fits `cap`. Candidates are most-popular first, so popping
    the tail drops the least-equipped option. Every slot keeps >= 1 candidate."""
    while _combo_count(opts) > cap:
        slot = max((s for s, v in opts.items() if len(v) > 1),
                   key=lambda s: len(opts[s]), default=None)
        if slot is None:
            break
        opts[slot] = opts[slot][:-1]
    return opts


def _same_cand(a, b):
    """True if two candidates are the same equipped item (id + bonus_list)."""
    if a is None or b is None:
        return a is None and b is None
    return a.get("item_id") == b.get("item_id") and a.get("bonus_list") == b.get("bonus_list")


def enumerate_valid_combos(fixed_gear, vary, cap):
    """Cartesian product of the varying slots, keeping only sets that pass
    set_is_valid (combined with the fixed/locked gear). Most-popular-first order
    is preserved, so the earliest valid combo is the most popular legal set.

    Returns a list of `chosen` dicts (slot -> candidate) over the varying slots."""
    slots = list(vary.keys())
    if not slots:
        return [{}] if set_is_valid(fixed_gear) else []
    combos = []
    for choice in itertools.product(*(vary[s] for s in slots)):
        chosen = dict(zip(slots, choice))
        if set_is_valid({**fixed_gear, **chosen}):
            combos.append(chosen)
            if len(combos) >= cap:
                break
    return combos


def detect_tier(candidates):
    """Detect the current tier set from the candidate pool.

    Returns (tier_set_id, tier_slots) where tier_slots is the set of Blizzard
    slot names whose candidates contain a member of the dominant item set that
    spans >= 4 of the tier-eligible armour slots. Returns (None, set()) if none.
    """
    # itemSetId -> set of tier slots it appears in (among candidates), with weight
    coverage = {}
    weight = {}
    for slot in ("HEAD", "SHOULDER", "CHEST", "HANDS", "LEGS"):
        for rank, cand in enumerate(candidates.get(slot, [])):
            sid = cand.get("item_set_id")
            if not sid or cand.get("inv_type") not in TIER_INVTYPES:
                continue
            coverage.setdefault(sid, set()).add(slot)
            # earlier (more popular) candidates weigh more
            weight[sid] = weight.get(sid, 0) + (SIMC_CANDIDATES_PER_SLOT - rank)

    best = None
    for sid, slots in coverage.items():
        if len(slots) >= 4:
            if best is None or (len(slots), weight[sid]) > (len(coverage[best]), weight[best]):
                best = sid
    if best is None:
        return None, set()
    return best, set(coverage[best])


def best_tier_candidate(candidates, slot, tier_set_id):
    for cand in candidates.get(slot, []):
        if cand.get("item_set_id") == tier_set_id:
            return cand
    return None


# --------------------------------------------------------------------------
# .simc text construction
# --------------------------------------------------------------------------

def gear_line(slot, cand):
    """One simc gear line, e.g. 'head=,id=12345,bonus_id=1808/1492'."""
    simc_slot = DB_TO_SIMC_SLOT[slot]
    parts = [f"{simc_slot}=,id={cand['item_id']}"]
    if cand.get("simc_bonus"):
        parts.append(f"bonus_id={cand['simc_bonus']}")
    if cand.get("enchant_id"):
        parts.append(f"enchant_id={cand['enchant_id']}")
    if cand.get("gem_ids"):
        parts.append("gem_id=" + "/".join(str(g) for g in cand["gem_ids"]))
    return ",".join(parts)


def build_header(class_name, spec_name, primary_stat, talents_code, actor_name=None):
    token = class_token(class_name)
    race = DEFAULT_RACE.get(token, "orc")
    role = "spell" if (primary_stat or "").upper() == "INTELLECT" else "attack"
    name = actor_name or f"mythistone_{spec_slug(spec_name)}"
    lines = [
        f'{token}="{name}"',
        # `source=default` selects simc's built-in generated APL for the spec —
        # present in every bundled profile; we rely on it for the rotation.
        "source=default",
        f"spec={spec_slug(spec_name)}",
        f"level={SIMC_LEVEL}",
        f"race={race}",
        f"role={role}",
        "position=back",
    ]
    if talents_code:
        lines.append(f"talents={talents_code}")
    return lines


# Raid-buff overrides applied to every sim so absolute DPS reflects a fully
# buffed group (and is thus comparable across specs). Shared by sim_options here
# and the CI tierlist profiles (generateSimcProfiles.py) so the two never drift.
RAID_BUFF_OVERRIDES = [
    "override.bloodlust=1",
    "override.arcane_intellect=1",
    "override.power_word_fortitude=1",
    "override.battle_shout=1",
    "override.mystic_touch=1",
    "override.chaos_brand=1",
    "override.skyfury=1",
    "override.mark_of_the_wild=1",
    "override.hunters_mark=1",
    "override.bleeding=1",
]


def sim_options(iterations=None):
    """simc-wide options.

    Convergence: every sim runs to SIMC_TARGET_ERROR but is capped at a maximum
    iteration count, and stops at whichever it reaches first. `iterations`, when
    given, pins that cap for this run (the combination pass passes
    SIMC_COMBO_ITERATIONS); otherwise the cap comes from SIMC_ITERATIONS if set,
    else there is no cap and target_error alone governs. In simc, specifying both
    target_error and iterations makes iterations the ceiling — so this yields
    "stop at 0.1% error OR N iterations, whichever first".
    """
    opts = [
        f"threads={SIMC_THREADS}",
        f"profileset_work_threads={SIMC_PROFILESET_WORK_THREADS}",
        "profileset_metric=dps",
        "single_actor_batch=1",
        "collect_action_sequence=0",
        "buff_stack_uptime_timeline=0",
        "buff_uptime_timeline=0",
        "report_details=0",
        "desired_targets=5",
        "fight_style=LightMovement",
        "max_time=300",
        "calculate_scale_factors=0",
        "scale_only=strength,intellect,agility,crit,mastery,vers,haste,weapon_dps,weapon_offhand_dps",
        *RAID_BUFF_OVERRIDES,
        "optimize_expressions=1",
    ]
    opts.append(f"target_error={SIMC_TARGET_ERROR}")
    cap = iterations or (int(SIMC_ITERATIONS) if SIMC_ITERATIONS else None)
    if cap:
        opts.append(f"iterations={cap}")
    return opts


def build_profile(header, baseline_gear, profilesets, iterations=None):
    """Assemble the full .simc text.

    baseline_gear: dict slot -> candidate (the current best-known set).
    profilesets: list of (name, [(slot, candidate), ...]) overrides.
    """
    out = []
    out.extend(sim_options(iterations))
    out.append("")
    out.extend(header)
    out.append("")
    out.append("### baseline gear")
    for slot, cand in baseline_gear.items():
        if cand is None:
            continue
        out.append(gear_line(slot, cand))
    out.append("")
    out.append("### profilesets")
    for name, overrides in profilesets:
        first = True
        for slot, cand in overrides:
            op = "=" if first else "+="
            out.append(f'profileset."{name}"{op}{gear_line(slot, cand)}')
            first = False
    return "\n".join(out) + "\n"


def build_combinations(candidates, baseline, active_slots, tier_set_id, tier_slots,
                       item_lookup, cap):
    """Build Top-Gear-style full-set combinations across every tier scenario.

    Each combination is a complete legal equipped set (equip limits enforced).
    Tier configuration is part of the search, not decided up front: we enumerate
    "wear the full set" plus, when there are >=5 tier slots, "drop one slot to an
    off-piece" (always keeping >=4pc). simc applies the set bonus per combo, so the
    tier-vs-off-piece choice — and which off-piece — is settled by full-set DPS.

    Returns (base_full, profilesets, index, all_combos, scenarios):
      base_full   : dict slot->cand seeding the simc base actor (most-popular combo)
      profilesets : list of (name, [(slot, cand), ...]) overrides vs base_full
      index       : name -> (full_set_dict, config_label)
      all_combos  : list of (full_set_dict, config_label)
      scenarios   : list of config labels explored
    """
    # Tier piece available per tier slot, and the tier scenarios to explore.
    tier_pieces = {}
    if tier_set_id:
        for s in tier_slots:
            tc = best_tier_candidate(candidates, s, tier_set_id)
            if tc:
                tier_pieces[s] = tc
    n_tier = len(tier_pieces)

    scenarios = []   # (config_label, kept_tier_gear, dropped_slot)
    if n_tier >= 4:
        scenarios.append(("all", dict(tier_pieces), None))
        if n_tier >= 5:                 # drop one slot to an off-piece, still >=4pc
            for drop in tier_pieces:
                kept = {s: c for s, c in tier_pieces.items() if s != drop}
                scenarios.append((f"drop:{drop}", kept, drop))
        tiered_slots = set(tier_pieces)
    else:
        scenarios.append(("none", {}, None))   # no meaningful set: optimise freely
        tiered_slots = set()

    # Non-tier varying slots, with the main hand pinned to the baseline's
    # handedness. A one-hand baseline never pulls in a two-hander (and vice
    # versa); the off-hand only rides along when the baseline kept one — i.e.
    # for 1H specs and Titan's Grip Fury, but not for plain two-hand specs.
    base_mh = baseline.get("MAIN_HAND")
    base_mh_2h = bool(base_mh and item_lookup.get(base_mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES)

    def slot_bag(slot, cands):
        if slot == "MAIN_HAND":
            cands = [c for c in cands
                     if (item_lookup.get(c["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES) == base_mh_2h]
            if not cands and base_mh:
                cands = [base_mh]
        return list(cands)

    normal_slots = [s for s in active_slots if s not in tiered_slots]
    normal_bag = {s: slot_bag(s, candidates.get(s, [])) for s in normal_slots if candidates.get(s)}
    normal_bag = {s: v for s, v in normal_bag.items() if v}

    # Share the combination budget across scenarios so the whole search fits.
    per_scenario_cap = max(1, cap // len(scenarios))

    all_combos = []   # list of (full_set_dict, config_label)
    used_labels = []
    for label, kept_tier, dropped in scenarios:
        bag = {s: list(v) for s, v in normal_bag.items()}
        if dropped:
            off = [c for c in candidates.get(dropped, []) if c.get("item_set_id") != tier_set_id]
            if not off:
                continue   # nothing to drop to; "all" already covers wearing it
            bag[dropped] = slot_bag(dropped, off)
        trim_bag(bag, per_scenario_cap)
        fixed_slots = {s: v[0] for s, v in bag.items() if len(v) == 1}
        vary = {s: v for s, v in bag.items() if len(v) > 1}
        scen_fixed = dict(baseline)        # most-popular per slot ...
        scen_fixed.update(kept_tier)       # ... tier slots wear the set ...
        if dropped:
            scen_fixed.pop(dropped, None)   # ... except the dropped slot (from bag)
        scen_fixed.update(fixed_slots)
        for chosen in enumerate_valid_combos(scen_fixed, vary, per_scenario_cap):
            all_combos.append(({**scen_fixed, **chosen}, label))
        used_labels.append(label)

    if not all_combos:
        return None, [], {}, [], used_labels

    # Seed the base actor with the first (most-popular) combo; express every other
    # combo as a profileset overriding only the slots that differ from it.
    base_full, _ = all_combos[0]
    profilesets = []
    index = {}
    for i, (full, label) in enumerate(all_combos[1:], start=1):
        overrides = [(s, full[s]) for s in full if not _same_cand(full.get(s), base_full.get(s))]
        if not overrides:
            continue
        name = f"g{i}"
        profilesets.append((name, overrides))
        index[name] = (full, label)
    return base_full, profilesets, index, all_combos, used_labels


# --------------------------------------------------------------------------
# Running simc
# --------------------------------------------------------------------------

async def run_simc(profile_text, token):
    """Write the profile, run simc, return (result_dict_or_None, error_str_or_None).

    Two execution modes:
      * SIMC_BIN set  -> run a local simc binary directly (local debugging).
      * otherwise     -> launch a short-lived sibling container via the Docker
                         SDK over the mounted docker socket, sharing the
                         SIMC_IO_VOLUME named volume mounted at /data.
    """
    SIMC_IO_DIR.mkdir(parents=True, exist_ok=True)
    in_path = SIMC_IO_DIR / f"{token}.simc"
    out_path = SIMC_IO_DIR / f"{token}.json"
    in_path.write_text(profile_text, encoding="utf-8")
    if out_path.exists():
        out_path.unlink()

    if SIMC_BIN:
        ok, err = await _run_simc_local(token, in_path, out_path)
    else:
        ok, err = await _run_simc_docker(token)
    if not ok:
        return None, err
    if not out_path.exists():
        msg = f"simc produced no output for {token}"
        _log(msg)
        return None, msg
    try:
        return json.loads(out_path.read_text(encoding="utf-8")), None
    except Exception as e:
        msg = f"failed to parse simc json for {token}: {e}"
        _log(msg)
        return None, msg


async def _run_simc_local(token, in_path, out_path):
    cmd = [SIMC_BIN, str(in_path), f"json2={out_path}"]
    _log(f"running simc: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=SIMC_RUN_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        # Drain whatever simc buffered before we killed it and distill it, so the
        # log explains how far it got and why (see _summarize_simc_progress).
        summary = ""
        try:
            stdout, _ = await proc.communicate()
            summary = _summarize_simc_progress(
                (stdout or b"").decode("utf-8", "replace"), elapsed_s=SIMC_RUN_TIMEOUT
            )
        except Exception:
            pass
        msg = f"simc timed out after {SIMC_RUN_TIMEOUT}s for {token}"
        if summary:
            msg += f" — {summary}"
        _log(msg)
        return False, msg
    if proc.returncode != 0:
        tail = (stdout or b"").decode("utf-8", "replace")[-1500:]
        msg = f"simc exited {proc.returncode} for {token}:\n{tail}"
        _log(msg)
        return False, tail
    return True, None


async def pull_simc_image(stats=None):
    """Pull the latest simc image so ephemeral `--rm` runs use a current build.

    simc containers are short-lived, so watchtower (which only tracks long-running
    containers) cannot keep them current — we refresh the image ourselves instead.
    """
    if SIMC_BIN:
        return True
    def _pull():
        import docker
        client = docker.from_env()
        img = client.images.pull(SIMC_DOCKER_IMAGE)
        tags = getattr(img, "tags", None)
        return tags[0] if tags else str(getattr(img, "id", ""))[:19]
    try:
        ref = await asyncio.to_thread(_pull)
        _stat_log(stats, f"simc: pulled image {SIMC_DOCKER_IMAGE} ({ref})")
        return True
    except Exception as e:
        _stat_log(stats, f"simc: image pull failed for {SIMC_DOCKER_IMAGE}: {e}")
        return False


def cleanup_orphaned_containers(reason="shutdown"):
    """Force-stop/remove any simc sibling containers we previously launched.

    Sibling containers aren't children of this process and aren't tracked by
    watchtower (they're short-lived and unnamed), so nothing else will clean
    them up when this collector container is replaced. Called from the
    entrypoint's SIGTERM trap so an update doesn't leave a sim running forever
    on the host. Matches purely by label, so it's safe to call even if no
    sibling is currently running.
    """
    try:
        import docker
        client = docker.from_env()
        label_filter = [f"{k}={v}" for k, v in SIMC_CONTAINER_LABEL.items()]
        containers = client.containers.list(all=True, filters={"label": label_filter})
    except Exception as e:
        _log(f"simc: cleanup skipped, could not reach docker ({reason}): {e}")
        return

    for container in containers:
        try:
            container.remove(force=True)
            _log(f"simc: removed orphaned container {container.short_id} ({reason})")
        except Exception as e:
            _log(f"simc: failed to remove container {container.short_id} ({reason}): {e}")


_PROFILESET_PROGRESS_RE = re.compile(r"Profilesets\s*\((\d+\*\d+)\):\s*(\d+)/(\d+)(.*)")


def _summarize_simc_progress(raw, elapsed_s=None):
    """Best-effort one-line diagnostic of how far a simc run got, from its console
    output. Used when a run times out so the collector log explains *why* without
    anyone having to attach to the container.

    simc rewrites a single progress line in place using carriage returns, so the
    captured logs are one long CR-delimited blob; we split on CR/LF and keep the
    last frame that reported profileset progress. When we know how long the run
    ran (elapsed_s) we extrapolate the count to a projected full-run time, which
    is usually the punchline ("would need ~22h, limit is 8h"). We also surface the
    single_actor_batch warning that fires for support/pet specs (Augmentation),
    since that disables simc's batch optimisation and makes every profileset sim
    the whole group — the most common reason a spec blows the timeout. Returns ''
    when nothing recognisable is present.
    """
    if not raw:
        return ""
    last = None
    warn_batch = False
    for frame in re.split(r"[\r\n]+", raw):
        frame = frame.strip()
        if not frame:
            continue
        m = _PROFILESET_PROGRESS_RE.search(frame)
        if m:
            last = m
        elif "single actor batch is not supported" in frame.lower():
            warn_batch = True
    parts = []
    if last:
        threads, done, total = last.group(1), int(last.group(2)), int(last.group(3))
        # Drop simc's ASCII progress bar "[====>....]" and collapse whitespace,
        # leaving the useful "avg=.. done=.." timing tail.
        tail = " ".join(re.sub(r"\[[.=>< ]*\]", "", last.group(4)).split())
        pct = (done / total * 100) if total else 0
        line = f"reached {done}/{total} profilesets ({pct:.0f}%, threads {threads})"
        if tail:
            line += f" {tail}"
        if elapsed_s and done:
            projected_h = elapsed_s * total / done / 3600
            line += (f"; at this rate a full run needs ~{projected_h:.1f}h "
                     f"(limit {SIMC_RUN_TIMEOUT / 3600:.1f}h)")
        parts.append(line)
    if warn_batch:
        parts.append("single_actor_batch disabled for a support/pet spec — every "
                     "profileset sims the full group (far slower); expect long runtimes")
    return "; ".join(parts)


async def _run_simc_docker(token):
    """Run simc in a sibling container via the Docker SDK.

    Launches detached (rather than blocking `containers.run`) so that if our
    own wait times out we can actually stop/remove the container ourselves —
    a blocking run() call can't be cancelled from the outside, which used to
    leave the container running indefinitely in the background after we'd
    already given up and moved on to the next spec.
    """
    import docker  # imported lazily so local/debug runs don't require the SDK

    def _start():
        client = docker.from_env()
        command = ([SIMC_CMD] if SIMC_CMD else []) + [
            f"/data/{token}.simc",
            f"json2=/data/{token}.json",
        ]
        # In production the collector is itself containerized, so the shared dir
        # must be a named volume the host daemon can resolve. Locally (no named
        # volume set) bind-mount the absolute host dir so testing works directly.
        mount_src = SIMC_IO_VOLUME or str(SIMC_IO_DIR.resolve())
        kwargs = {
            "image": SIMC_DOCKER_IMAGE,
            "command": command,
            "volumes": {mount_src: {"bind": "/data", "mode": "rw"}},
            "remove": False,
            "detach": True,
            "labels": SIMC_CONTAINER_LABEL,
        }
        if SIMC_CPUS:
            try:
                kwargs["nano_cpus"] = int(float(SIMC_CPUS) * 1e9)
            except Exception:
                pass
        if SIMC_CPU_SHARES:
            try:
                kwargs["cpu_shares"] = max(2, int(SIMC_CPU_SHARES))
            except Exception:
                pass
        if SIMC_CPUSET:
            # Passed through verbatim (e.g. "1" or "1-3"); docker validates it.
            kwargs["cpuset_cpus"] = str(SIMC_CPUSET)
        if SIMC_MEM_LIMIT:
            kwargs["mem_limit"] = SIMC_MEM_LIMIT
        if SIMC_BLKIO_WEIGHT:
            try:
                kwargs["blkio_weight"] = max(10, min(1000, int(SIMC_BLKIO_WEIGHT)))
            except Exception:
                pass
        return client.containers.run(**kwargs)

    def _wait_and_collect(container):
        try:
            status = container.wait(timeout=SIMC_RUN_TIMEOUT)
            exit_code = status.get("StatusCode", 1) if isinstance(status, dict) else status
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            return exit_code, logs
        finally:
            try:
                container.remove(force=True)
            except Exception:
                pass

    _log(f"running simc container {SIMC_DOCKER_IMAGE} for {token}")
    try:
        container = await asyncio.to_thread(_start)
    except ModuleNotFoundError:
        msg = ("simc: the 'docker' Python SDK is not installed. Either `pip install docker` "
               "(with Docker running) or set SIMC_BIN=<path to simc.exe> for a local run.")
        _log(msg)
        return False, msg
    except Exception as e:
        msg = f"simc container failed to start for {token}: {str(e)[-1500:]}"
        _log(msg)
        return False, msg

    try:
        exit_code, logs = await asyncio.wait_for(
            asyncio.to_thread(_wait_and_collect, container), timeout=SIMC_RUN_TIMEOUT
        )
    except asyncio.TimeoutError:
        # Grab the container's console output *before* we kill it so the log
        # records how far the sim got and why it was too slow (see
        # _summarize_simc_progress) instead of just "timed out".
        summary = ""
        try:
            raw = await asyncio.to_thread(
                lambda: container.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            )
            summary = _summarize_simc_progress(raw, elapsed_s=SIMC_RUN_TIMEOUT)
        except Exception as e:
            _log(f"simc: could not read logs from timed-out container for {token}: {e}")
        msg = f"simc container timed out after {SIMC_RUN_TIMEOUT}s for {token}"
        if summary:
            msg += f" — {summary}"
        _log(msg)
        try:
            await asyncio.to_thread(container.stop, timeout=10)
        except Exception:
            pass
        try:
            await asyncio.to_thread(container.remove, force=True)
        except Exception as e:
            _log(f"failed to remove timed-out container for {token}: {e}")
        return False, msg
    except Exception as e:
        msg = f"simc container failed for {token}: {str(e)[-1500:]}"
        _log(msg)
        return False, msg

    if exit_code != 0:
        tail = logs[-1500:]
        msg = f"simc container exited {exit_code} for {token}:\n{tail}"
        _log(msg)
        return False, tail
    return True, None


def parse_baseline_dps(result):
    try:
        players = result.get("sim", {}).get("players", [])
        return float(players[0]["collected_data"]["dps"]["mean"])
    except Exception:
        return None


def parse_profileset_means(result):
    """name -> mean dps for every profileset result."""
    means = {}
    try:
        for r in result.get("sim", {}).get("profilesets", {}).get("results", []):
            means[r["name"]] = float(r["mean"])
    except Exception:
        pass
    return means


def parse_simc_version(result):
    # The simc build string is at the JSON root: root["version"] (SC_VERSION),
    # with git_revision as a secondary identifier.
    try:
        ver = result.get("version") or result.get("git_revision") or ""
        return str(ver)[:64] or None
    except Exception:
        return None


# --------------------------------------------------------------------------
# Optimisation
# --------------------------------------------------------------------------

def _prepare_spec(spec_id, spec_info, class_info, season, conn, cursor, item_lookup, stats=None):
    """Gather everything needed to build profiles for a spec (no simming).

    Returns (dict, None) with header, candidates, baseline, tier info and
    active_slots, or (None, error_str) if the spec can't be prepared. Shared
    by optimize_spec and --dry-run.
    """
    spec_name = spec_info.get("name")
    class_name = class_info.get("name")
    if not class_token(class_name):
        msg = f"unknown class token for {class_name}"
        _stat_log(stats, f"simc: {msg}, skipping spec {spec_id}")
        return None, msg

    candidates = gather_candidates(conn, cursor, spec_id, season, item_lookup)
    if not candidates:
        msg = f"no candidate items for spec {spec_id}"
        _stat_log(stats, f"simc: {msg}, skipping")
        return None, msg

    # constant enchants/gems from the top-50 players (see apply_enchants_and_gems)
    enchant_map = fetch_enchant_map(conn, cursor, spec_id, season)
    gem_ranking = fetch_gem_ranking(conn, cursor, spec_id, season)
    apply_enchants_and_gems(candidates, enchant_map, gem_ranking, item_lookup)

    # most-popular talent loadout code
    talents_code = None
    try:
        rows = databaseConnector.fetch_top_loadout(conn, cursor, spec_id, season)
        best_row = None
        for r in rows or []:
            total = r.get("total_runs") if isinstance(r, dict) else r[2]
            loadout = r.get("loadout") if isinstance(r, dict) else r[1]
            if not loadout:
                continue
            if best_row is None or int(total or 0) > best_row[0]:
                best_row = (int(total or 0), loadout)
        if best_row:
            talents_code = best_row[1]
    except Exception as e:
        _log(f"could not fetch top loadout for spec {spec_id}: {e}")

    header = build_header(class_name, spec_name, spec_info.get("primary_stat"), talents_code)

    tier_set_id, tier_slots = detect_tier(candidates)
    _stat_log(stats, f"simc: spec {spec_id} ({class_name}/{spec_name}) tier_set={tier_set_id} slots={sorted(tier_slots)}")

    # ---- initial baseline = most-popular item per slot ----
    baseline = {slot: cands[0] for slot, cands in candidates.items()}

    # drop off_hand if main hand is a two-hander / ranged weapon — but not for
    # Titan's Grip Fury, which wields a two-hander in the off-hand too.
    mh = baseline.get("MAIN_HAND")
    if (mh and spec_id not in DUAL_WIELD_TWOHAND_SPECS
            and item_lookup.get(mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES):
        baseline.pop("OFF_HAND", None)

    # The per-slot popularity picks above ignore cross-slot equip limits; the most
    # common item in three+ slots can be embellished. Keep the popular set legal
    # (<=2 embellishments) so it isn't simmed with illegal, DPS-inflating stats.
    legalize_baseline_embellishments(baseline, candidates)

    active_slots = [s for s in ALL_SLOTS if s in baseline]

    return {
        "header": header,
        "candidates": candidates,
        "baseline": baseline,
        "tier_set_id": tier_set_id,
        "tier_slots": tier_slots,
        "active_slots": active_slots,
        "talents_code": talents_code,
        "enchant_map": enchant_map,
        "gem_ranking": gem_ranking,
    }, None


async def optimize_spec(spec_id, spec_info, class_info, season, conn, cursor,
                        item_lookup, stats=None):
    """Run the full optimisation for one spec.

    Returns (result_dict, None) on success, or (None, error_str) on failure.
    """
    prep, prep_err = _prepare_spec(spec_id, spec_info, class_info, season, conn, cursor, item_lookup, stats)
    if not prep:
        return None, prep_err
    header = prep["header"]
    candidates = prep["candidates"]
    baseline = prep["baseline"]
    tier_set_id = prep["tier_set_id"]
    tier_slots = prep["tier_slots"]
    active_slots = prep["active_slots"]

    # ---- Top-Gear-style full-set combinations (tier configs co-optimised) ----
    # Evaluate whole-set combinations rather than optimising one slot at a time,
    # pruning any set that breaks an equip limit. This captures cross-slot
    # interactions and keeps the recommended set legal (<=2 embellishments, no
    # duplicate unique-equipped item, itemLimit categories respected). The tier
    # set is co-optimised here too (see build_combinations): the tier-vs-off-piece
    # tradeoff is settled by full-set DPS, not decided up front by popularity.
    try:
        combo_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        combo_iters = None
    if combo_iters is not None and combo_iters <= 0:
        combo_iters = None

    base_full, profilesets, index, all_combos, scenarios = build_combinations(
        candidates, baseline, active_slots, tier_set_id, tier_slots,
        item_lookup, SIMC_MAX_COMBINATIONS,
    )
    if not all_combos:
        msg = f"spec {spec_id} produced no valid gear combinations"
        _stat_log(stats, f"simc: {msg}")
        return None, msg
    base_label = all_combos[0][1]

    _stat_log(stats, f"simc: spec {spec_id} evaluating {len(all_combos)} full-set combos "
                     f"across {len(scenarios)} tier scenario(s)")
    profile_text = build_profile(header, base_full, profilesets, iterations=combo_iters)
    result, run_err = await run_simc(profile_text, f"spec{spec_id}_topgear")
    if not result:
        return None, run_err or "simc produced no result"
    baseline_dps = parse_baseline_dps(result)
    if baseline_dps is None:
        return None, "could not parse baseline dps from simc result"
    simc_version = parse_simc_version(result)
    if simc_version and stats is not None:
        try:
            stats.set_status("simc_build", simc_version)
        except Exception:
            pass
    means = parse_profileset_means(result)
    if stats is not None:
        try:
            await stats.increment("simc_profilesets_run", len(means))
        except Exception:
            pass

    # Reassemble every simmed combo as (full set, dps, config_label).
    combo_results = [(base_full, baseline_dps, base_label)]
    for name, dps in means.items():
        full, label = index[name]
        combo_results.append((full, dps, label))
    best_full, best_dps, tier_config = max(combo_results, key=lambda x: x[1])

    # Per-slot ranking derived from the full-set sims. Every combo in
    # combo_results already passed set_is_valid as a whole set, so rank N per
    # slot must come from the Nth-best *valid combo*, not from independently
    # re-maximising each item's best DPS across every combo it ever appeared
    # in — that decomposition can stitch together items that were never
    # legal together (e.g. >2 embellishments, or the same unique trinket in
    # both trinket slots). Walking combos best-to-worst and taking each
    # slot's first (= best) occurrence of an item keeps every rank, including
    # rank 1, self-consistent with a single real valid combo.
    combos_by_dps = sorted(combo_results, key=lambda x: x[1], reverse=True)
    per_slot_ranked = {}
    slot_baseline_dps = {}
    for slot in active_slots:
        seen_items = set()
        ranked = []
        for full, dps, _ in combos_by_dps:
            cand = full.get(slot)
            if not cand:
                continue
            key = cand["item_id"]
            if key in seen_items:
                continue
            seen_items.add(key)
            ranked.append((cand, dps))
        if not ranked:
            continue
        per_slot_ranked[slot] = ranked
        cs = candidates.get(slot)
        me = next((cd for cd in ranked if cd[0]["item_id"] == cs[0]["item_id"]), None) if cs else None
        slot_baseline_dps[slot] = me[1] if me else best_dps

    if not per_slot_ranked:
        return None, f"spec {spec_id} produced no per-slot ranking"

    return {
        "spec_id": spec_id,
        "season": season,
        "baseline_dps": best_dps,
        "slot_baseline_dps": slot_baseline_dps,
        "simc_version": simc_version,
        "tier_set_id": tier_set_id,
        "tier_config": tier_config,
        "per_slot_ranked": per_slot_ranked,
        "combos": len(combo_results),
    }, None


# --------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------

def persist(conn, cursor, result, item_lookup):
    spec_id = result["spec_id"]
    season = result["season"]
    baseline_dps = result["baseline_dps"]
    slot_baseline_dps = result.get("slot_baseline_dps", {})
    tier_set_id = result.get("tier_set_id")

    item_rows = []
    for slot, ranked in result["per_slot_ranked"].items():
        # Reference for this slot is the most-equipped item's DPS (see
        # optimize_spec); fall back to the converged baseline if unavailable.
        ref_dps = slot_baseline_dps.get(slot) or baseline_dps
        for rank, (cand, dps) in enumerate(ranked, start=1):
            pct = ((dps - ref_dps) / ref_dps * 100.0) if (ref_dps and dps is not None) else None
            sid = item_lookup.get(cand["item_id"], {}).get("itemSetId")
            gem_ids = cand.get("gem_ids")
            item_rows.append(
                (
                    spec_id,
                    season,
                    slot,
                    rank,
                    cand["item_id"],
                    cand.get("bonus_list"),
                    None,  # ilevel: derived by simc from bonus_ids; not stored here
                    float(dps) if dps is not None else None,
                    float(pct) if pct is not None else None,
                    1 if (sid and sid == tier_set_id) else 0,
                    int(sid) if sid else None,
                    int(cand["enchant_id"]) if cand.get("enchant_id") else None,
                    "/".join(str(g) for g in gem_ids) if gem_ids else None,
                )
            )

    # Effective simc accuracy used for the combination pass: it always converges
    # to target_error, capped at a maximum iteration count (combo / env override),
    # so we record both — target_error is the stop condition, iterations the cap.
    try:
        effective_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        effective_iters = None
    if not effective_iters or effective_iters <= 0:
        effective_iters = int(SIMC_ITERATIONS) if SIMC_ITERATIONS else None
    effective_terr = float(SIMC_TARGET_ERROR)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        # The loop connection runs autocommit=1 (see run_simc_bis); group the
        # delete+meta+items writes into one transaction so readers never see a
        # spec with its old rows deleted but the new ones not yet inserted.
        if not conn.in_transaction:
            conn.start_transaction()
        databaseConnector.delete_simc_bis(conn, cursor, spec_id, season)
        databaseConnector.insert_simc_bis_meta(
            conn, cursor, spec_id, season,
            simc_version=result.get("simc_version"),
            baseline_dps=baseline_dps,
            iterations=effective_iters,
            target_error=effective_terr,
            tier_config=result.get("tier_config"),
            updated_at=now,
        )
        databaseConnector.insert_simc_bis_items_batch(conn, cursor, item_rows)
        databaseConnector.commit_with_retry(conn)
    except Exception as e:
        conn.rollback()
        _log(f"DB error persisting simc BiS for spec {spec_id}: {e}")
        raise


# --------------------------------------------------------------------------
# Spec selection (round-robin cursor)
# --------------------------------------------------------------------------

def simulated_specs(specs):
    out = []
    for spec_id_str, info in specs.items():
        try:
            role = int(info.get("role", 2))
        except Exception:
            role = 2
        if role not in SIMULATED_ROLES:
            continue
        if int(spec_id_str) in SKIPPED_SPEC_IDS:
            continue
        out.append((int(spec_id_str), info))
    return out


def pick_next_spec(conn, cursor, specs, season):
    """Return the (spec_id, info) with the oldest / missing simc run."""
    oldest = None
    for spec_id, info in simulated_specs(specs):
        try:
            ts = databaseConnector.fetch_simc_bis_updated_at(conn, cursor, spec_id, season)
        except Exception:
            ts = None
        # None (never run) sorts first
        key = (ts is not None, ts or datetime.min)
        if oldest is None or key < oldest[0]:
            oldest = (key, spec_id, info)
    if oldest is None:
        return None
    return oldest[1], oldest[2]


# --------------------------------------------------------------------------
# Public entrypoint (wired into collectLeaderboardData.main)
# --------------------------------------------------------------------------

async def run_simc_bis(session, cancel_event=None, stats=None, get_season=None, reporter=None):
    """Continuously simulate per-slot BiS, one spec at a time, round-robin.

    `get_season(conn, cursor)` -> int season id. If omitted, falls back to the
    SIMC_SEASON env var. `session` is accepted for signature parity with the
    other collector tasks (not used directly). `reporter` is the DiscordReporter
    used to surface error conditions (instead of failing silently).
    """
    from contextlib import closing

    specs, classes = load_static()
    item_lookup = load_item_lookup()
    _stat_log(stats, f"simc: starting BiS collector ({len(simulated_specs(specs))} dps/tank specs)")

    # Surface a degraded max-level detection (fell back instead of using the
    # collected seasonInfo value) rather than silently simming at the fallback.
    if not os.environ.get("SIMC_LEVEL"):
        try:
            si = json.loads((STATIC_DIR / "seasonInfo.json").read_text(encoding="utf-8"))
            has_level = bool(si.get("max_character_level"))
        except Exception:
            has_level = False
        if not has_level:
            await _alert(
                reporter, stats, "SimC: max character level not detected",
                f"Could not read `max_character_level` from seasonInfo.json; "
                f"simulating at fallback level {SIMC_LEVEL}. Check the static-data "
                f"collection (wago.tools ContentTuning).",
                level="warning", throttle_key="simc_maxlevel",
            )

    def _cancelled():
        return cancel_event is not None and cancel_event.is_set()

    if not await pull_simc_image(stats):
        await _alert(
            reporter, stats, "SimC: image pull failed",
            f"Could not pull {SIMC_DOCKER_IMAGE}. Will use the cached image if "
            f"present; sims may be on a stale build or fail entirely.",
            level="warning", throttle_key="simc_pull",
        )
    last_pull = asyncio.get_event_loop().time()

    while not _cancelled():
        # refresh the simc image periodically
        if (asyncio.get_event_loop().time() - last_pull) > SIMC_PULL_INTERVAL:
            await pull_simc_image(stats)
            last_pull = asyncio.get_event_loop().time()
        try:
            with closing(databaseConnector.get_connection()) as conn:
                cursor = conn.cursor()
                # The pool default is autocommit=0, so the first SELECT of the
                # read phase (gear popularity from global_aggregated_*) opens a
                # transaction that holds shared MDL on those tables for the
                # entire multi-hour simc run (nothing commits until persist).
                # The daily TRUNCATE+rebuild events then queue an exclusive MDL
                # request behind us, and every later reader (e.g. the page
                # build) piles up behind that pending request -> 1205 lock wait
                # timeouts. autocommit releases MDL per statement; persist()
                # opens an explicit transaction so its delete+insert stays
                # atomic. READ UNCOMMITTED matches the events' isolation.
                conn.autocommit = True
                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                cursor.execute("SET SESSION lock_wait_timeout = 120")
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 30")
                season = None
                if get_season:
                    season = get_season(conn, cursor)
                if season is None:
                    env_season = os.environ.get("SIMC_SEASON")
                    season = int(env_season) if env_season else None
                if season is None:
                    await _alert(
                        reporter, stats, "SimC: no season available",
                        "Could not determine the current season (Blizzard season id "
                        "or SIMC_SEASON). Skipping this cycle.",
                        level="warning", throttle_key="simc_no_season",
                    )
                    await asyncio.sleep(SIMC_SPEC_SLEEP)
                    continue

                picked = pick_next_spec(conn, cursor, specs, season)
                if not picked:
                    await asyncio.sleep(SIMC_SPEC_SLEEP)
                    continue
                spec_id, info = picked
                class_info = classes.get(str(info.get("classID")), {})
                if stats is not None:
                    try:
                        stats.set_status("simc_current", f"{class_info.get('name')}/{info.get('name')}")
                    except Exception:
                        pass

                result, fail_reason = await optimize_spec(
                    spec_id, info, class_info, season, conn, cursor, item_lookup, stats
                )
                if result:
                    persist(conn, cursor, result, item_lookup)
                    if stats is not None:
                        try:
                            await stats.increment("simc_specs_completed")
                        except Exception:
                            pass
                    _stat_log(stats, f"simc: completed spec {spec_id} (baseline {result['baseline_dps']:.0f} dps)")
                else:
                    reason_tail = (fail_reason or "unknown error")[-1000:]
                    await _alert(
                        reporter, stats, "SimC: spec simulation failed",
                        f"No result for spec {spec_id} "
                        f"({class_info.get('name')}/{info.get('name')}).\n```\n{reason_tail}\n```",
                        level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                    )
                    # mark an attempt so we don't hammer a broken spec; write empty meta
                    try:
                        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        if not conn.in_transaction:
                            conn.start_transaction()
                        databaseConnector.delete_simc_bis(conn, cursor, spec_id, season)
                        databaseConnector.insert_simc_bis_meta(
                            conn, cursor, spec_id, season, updated_at=now
                        )
                        databaseConnector.commit_with_retry(conn)
                    except Exception:
                        conn.rollback()
        except Exception as e:
            import traceback
            traceback.print_exc()
            await _alert(
                reporter, stats, "SimC: collector loop error",
                f"{type(e).__name__}: {e}",
                level="error", throttle_key="simc_loop_error",
            )

        await asyncio.sleep(SIMC_SPEC_SLEEP)

    _stat_log(stats, "simc: BiS collector stopping")


# --------------------------------------------------------------------------
# Debug CLI: simulate a single spec without writing to the DB
# --------------------------------------------------------------------------

def _init_pool_from_env():
    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        2,
    )


async def _dry_run_single(spec_id, season):
    """Generate (and write) the .simc input profiles for a spec WITHOUT running
    simc. Lets you eyeball gear lines, bonus_ids, talents and profileset syntax.

    Writes the tier-sweep profile and the pass-0 greedy profile to SIMC_IO_DIR
    and prints them. The greedy profile uses the initial (most-popular) baseline,
    since the real sweep winner needs an actual sim to determine.
    """
    from contextlib import closing
    specs, classes = load_static()
    item_lookup = load_item_lookup()
    info = specs.get(str(spec_id))
    if not info:
        _log(f"unknown spec id {spec_id}")
        return
    class_info = classes.get(str(info.get("classID")), {})
    _init_pool_from_env()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        prep, prep_err = _prepare_spec(spec_id, info, class_info, season, conn, cursor, item_lookup)
    if not prep:
        _log(f"could not prepare spec: {prep_err}")
        return

    header = prep["header"]
    candidates = prep["candidates"]
    baseline = prep["baseline"]
    tier_set_id = prep["tier_set_id"]
    tier_slots = prep["tier_slots"]
    active_slots = prep["active_slots"]

    # candidate count per slot (spot thin slots at a glance)
    print("\n=== candidates per slot (after popularity filter) ===")
    for slot in active_slots:
        cs = candidates.get(slot, [])
        ids = ", ".join(f"{c['item_id']}(n={c['count']},sockets={c.get('socket_count', 0)})" for c in cs)
        print(f"  {slot:10} {len(cs):2}: {ids}")

    print("\n=== enchants (group -> enchant_id, constant across profilesets) ===")
    for grp, eid in sorted((prep.get("enchant_map") or {}).items()):
        print(f"  {grp:10} {eid}")
    print("=== gem ranking (most popular first, fills sockets top-down) ===")
    print(f"  {prep.get('gem_ranking') or []}")

    SIMC_IO_DIR.mkdir(parents=True, exist_ok=True)
    written = []

    # full-set Top-Gear combination profile (tier configs co-optimised), exactly
    # as the real run builds it.
    base_full, ps, index, all_combos, scenarios = build_combinations(
        candidates, baseline, active_slots, tier_set_id, tier_slots,
        item_lookup, SIMC_MAX_COMBINATIONS,
    )
    try:
        combo_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        combo_iters = None
    txt = build_profile(header, base_full or baseline, ps, iterations=combo_iters)
    p = SIMC_IO_DIR / f"dryrun_spec{spec_id}_topgear.simc"
    p.write_text(txt, encoding="utf-8")
    written.append(p)
    from collections import Counter
    by_scen = Counter(label for _, label in all_combos)
    print(f"\n=== TOP-GEAR COMBO PROFILE ({p}) — {len(all_combos)} valid combos, "
          f"{len(ps)} profilesets, tier scenarios {dict(by_scen)} ===\n{txt}")

    print(f"\nWrote {len(written)} profile(s) to {SIMC_IO_DIR}:")
    for p in written:
        print(f"  {p}")


async def _debug_single(spec_id, season, do_persist=False):
    specs, classes = load_static()
    item_lookup = load_item_lookup()
    info = specs.get(str(spec_id))
    if not info:
        _log(f"unknown spec id {spec_id}")
        return
    class_info = classes.get(str(info.get("classID")), {})

    _init_pool_from_env()
    from contextlib import closing
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        result, fail_reason = await optimize_spec(spec_id, info, class_info, season, conn, cursor, item_lookup)
        if result and do_persist:
            persist(conn, cursor, result, item_lookup)
            _log(f"persisted simc_bis rows for spec {spec_id} season {season}")
    if not result:
        _log(f"no result: {fail_reason}")
        return
    print(json.dumps({
        "spec_id": result["spec_id"],
        "baseline_dps": result["baseline_dps"],
        "simc_version": result["simc_version"],
        "tier_set_id": result["tier_set_id"],
        "tier_config": result["tier_config"],
        "combos": result.get("combos"),
        "bis_per_slot": {
            slot: {
                "item_id": ranked[0][0]["item_id"],
                "bonus_list": ranked[0][0]["bonus_list"],
                "dps": ranked[0][1],
                "dps_pct_gain": (
                    (ranked[0][1] - (result.get("slot_baseline_dps", {}).get(slot) or result["baseline_dps"]))
                    / (result.get("slot_baseline_dps", {}).get(slot) or result["baseline_dps"]) * 100.0
                ) if (result.get("slot_baseline_dps", {}).get(slot) or result["baseline_dps"]) else None,
            }
            for slot, ranked in result["per_slot_ranked"].items() if ranked
        },
    }, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=int, required=True, help="spec id to simulate")
    parser.add_argument("--season", type=int, required=True, help="season id")
    parser.add_argument("--persist", action="store_true",
                        help="also write the result to simc_bis_meta/simc_bis_items")
    parser.add_argument("--dry-run", action="store_true",
                        help="generate and print the .simc input profiles without running simc")
    args = parser.parse_args()
    if args.dry_run:
        asyncio.run(_dry_run_single(args.spec, args.season))
    else:
        asyncio.run(_debug_single(args.spec, args.season, do_persist=args.persist))
