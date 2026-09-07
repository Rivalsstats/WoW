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
     an equip limit (each embellishment's own itemLimit — usually the 2-per-character
     category 512, but some consume nothing and some cap at 1 — no duplicate
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
import hashlib
import asyncio
import argparse
import itertools
from datetime import datetime, timezone
from pathlib import Path

import commonUtils
import databaseConnector
# Shared with the page generators so the Titan's Grip exception is defined once
# (re-exported here: generateSimcProfiles imports it from this module).
from commonUtils import DUAL_WIELD_TWOHAND_SPECS


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
# Ceiling for a single simc invocation, which now sims at most one CHUNK of
# profilesets (see SIMC_CHUNK_SIZE / run_simc_bis), not a whole spec. Must stay
# comfortably below the collector's ~daily restart: a chunk killed by the
# restart is simply re-simmed on resume, but a chunk whose own runtime exceeds
# this cap can never complete, so keep chunks small enough to finish well within
# it. 8h leaves wide margin for a 64-profileset chunk on one pinned core.
SIMC_RUN_TIMEOUT = int(os.environ.get("SIMC_RUN_TIMEOUT", str(8 * 60 * 60)))  # seconds per chunk
# Profilesets simmed per collector visit. A heavy spec (hundreds of combos) is
# computed across this many-sized chunks over successive visits, checkpointed to
# simc_bis_progress after each, so it survives restarts and never blows the
# per-invocation timeout. Fast specs (fewer combos than this) still run in one.
SIMC_CHUNK_SIZE = int(os.environ.get("SIMC_CHUNK_SIZE", "64"))
# Applied to every sibling simc container we launch so it can be found and torn
# down independently of our own process state (e.g. when watchtower replaces this
# collector container, these siblings aren't tracked/updated by watchtower at all).
SIMC_CONTAINER_LABEL = {"mythistone.role": "simc-sim"}
SIMC_CANDIDATES_PER_SLOT = int(os.environ.get("SIMC_CANDIDATES_PER_SLOT", "10"))
# Top-Gear combination budget: hard cap on the number of full-set profilesets we
# evaluate per spec. The per-slot candidate "bag" is trimmed (least-popular items
# first) until its cartesian product fits this cap. One simc invocation handles
# them all as profilesets. This is now a real budget: it used to sit at 2000 while
# most enumerated sets were discarded as illegal (a spec delivered ~500), so
# raising it mostly bought pruned combos. With the equip limits modelled correctly
# nearly every enumerated set survives, and the cap directly sets how long a spec
# takes — roughly cap/SIMC_CHUNK_SIZE simc invocations per rotation.
SIMC_MAX_COMBINATIONS = int(os.environ.get("SIMC_MAX_COMBINATIONS", "500"))
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

# Full Blizzard inventoryType -> Blizzard slot key resolution, used to place the
# item ids of an aggregated_tier_set_comps `comp` (which can span any equippable
# slot, not just the five tier armour slots: a multi-set comp mixes a class tier
# set with, say, a two-piece ring or trinket set) onto concrete slots. Paired
# slots (finger / trinket) are filled left-to-right as their items are seen.
_INVTYPE_SINGLE_SLOT = {
    1: "HEAD", 2: "NECK", 3: "SHOULDER", 16: "BACK", 5: "CHEST", 20: "CHEST",
    9: "WRIST", 10: "HANDS", 6: "WAIST", 7: "LEGS", 8: "FEET",
}
_INVTYPE_PAIR_SLOTS = {11: ("FINGER_1", "FINGER_2"), 12: ("TRINKET_1", "TRINKET_2")}
_INVTYPE_MAIN_HAND = {13, 15, 17, 21, 25, 26}   # one-hand / two-hand / ranged / main hand
_INVTYPE_OFF_HAND = {14, 22, 23}                # off-hand / shield / held in off-hand


def _invtype_to_slot(inv_type, pair_next):
    """Resolve a Blizzard inventoryType to a concrete slot key in ALL_SLOTS.

    `pair_next` (inv_type -> next index) is mutated so a comp wearing two rings /
    two trinkets fills FINGER_1 then FINGER_2 (TRINKET_1 then TRINKET_2) rather
    than colliding on one slot. Returns None for an inventoryType with no gear
    slot (a profession tool, an unknown value)."""
    if inv_type in _INVTYPE_SINGLE_SLOT:
        return _INVTYPE_SINGLE_SLOT[inv_type]
    if inv_type in _INVTYPE_PAIR_SLOTS:
        slots = _INVTYPE_PAIR_SLOTS[inv_type]
        i = pair_next.get(inv_type, 0)
        if i >= len(slots):
            return None
        pair_next[inv_type] = i + 1
        return slots[i]
    if inv_type in _INVTYPE_MAIN_HAND:
        return "MAIN_HAND"
    if inv_type in _INVTYPE_OFF_HAND:
        return "OFF_HAND"
    return None


# Two-hand / ranged inventory types: when the main hand is one of these the
# off-hand slot does not exist and must be skipped.
TWO_HAND_INVTYPES = {17, 15, 25, 26}

# Below this many Top-50-covered slots the verified-loadout data is too thin to
# reseed from (early season, or a spec barely represented in the top-50): the
# baseline falls back to the most-popular set, no Top-50 items are unioned into
# the candidate pool, and no whole player sets are injected (see _prepare_spec).
MIN_TOP50_SLOTS = 8

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


async def _oneshot_alert(reporter, stats, key, title, message, level="warning"):
    """Log and (best-effort) push a one-shot alert: sent once per key until the
    condition recovers (clear_oneshot_alert) or the process restarts. Console
    logging is gated on the same one-shot so an expected idle state (e.g. a
    season with no data during the pre-season gap) doesn't spam the logs either.
    Without a reporter there is no state holder, so it logs each call."""
    if reporter is None:
        _stat_log(stats, f"simc ALERT[{level}] {title}: {message}")
        return
    try:
        sent = await reporter.send_oneshot_alert(key, title, message, level=level)
    except Exception as e:
        _log(f"failed to send discord alert: {e}")
        return
    if sent:
        _stat_log(stats, f"simc ALERT[{level}] {title}: {message}")


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


# Fallback embellishment item-limit category/quantity (Blizzard crafting
# category 512, the classic "two embellishments per character" cap). Only used
# for an embellishment whose reagent we can't resolve — the real limit comes from
# the reagent itself, see load_embellishment_limits.
EMBELLISH_LIMIT_CATEGORY = 512
EMBELLISH_LIMIT_QUANTITY = 2


_EMBELLISH_LIMITS = None


def load_embellishment_limits():
    """Embellishment bonus_id (str) -> (limit_category, limit_quantity), or None
    for the embellishments that consume no budget at all.

    embellishments.json maps embellishment bonus_id -> reagent item_id, and it is
    the REAGENT's own itemLimit (crafting.json) that the game enforces. Most
    reagents share {category: 512, quantity: 2} — the familiar two-embellishment
    cap — but not all: a handful (Lucky Keychain, Griftah's powders, Reserve
    Parachute, ...) carry no itemLimit and stack freely with anything, and a few
    sit in a stricter category of their own (697, quantity 1). Treating every
    embellishment as 512/2 gets both ends wrong: it prunes legal sets that wear a
    free embellishment — enough of them and a spec has NO legal combination left
    and never sims at all — and it lets two of a one-per-character embellishment
    be simmed together. An unresolvable reagent falls back to the 512/2 cap:
    over-constraining is the safer error, and it is logged.

    Both source files are required: without them every set would silently be
    judged legal, which is how illegal, DPS-inflated gear reaches the site — so a
    missing file raises rather than degrading quietly.
    """
    global _EMBELLISH_LIMITS
    if _EMBELLISH_LIMITS is not None:
        return _EMBELLISH_LIMITS
    data = json.loads((STATIC_DIR / "embellishments.json").read_text(encoding="utf-8"))
    craft = json.loads((STATIC_DIR / "crafting.json").read_text(encoding="utf-8"))
    reagents = {}
    for r in craft.get("reagents") or []:
        if isinstance(r, dict) and r.get("itemId") is not None:
            reagents[int(r["itemId"])] = r

    out = {}
    unresolved = []
    for bonus_id, reagent_id in data.items():
        reagent = reagents.get(int(reagent_id))
        if reagent is None:
            unresolved.append(reagent_id)
            out[str(bonus_id)] = (EMBELLISH_LIMIT_CATEGORY, EMBELLISH_LIMIT_QUANTITY)
            continue
        lim = reagent.get("itemLimit") or {}
        cat = lim.get("category")
        out[str(bonus_id)] = (cat, lim.get("quantity")) if cat is not None else None
    if unresolved:
        _log(f"{len(unresolved)} embellishment reagent(s) missing from crafting.json "
             f"(e.g. {unresolved[:5]}); assuming the {EMBELLISH_LIMIT_QUANTITY}-embellishment "
             f"cap for them")
    _EMBELLISH_LIMITS = out
    return _EMBELLISH_LIMITS


_BONUS_SOCKET_COUNTS = None


def load_bonus_socket_counts():
    """bonus_id (str) -> number of sockets that bonus grants (bonuses.json).

    Required, like load_embellishment_limits' sources: an empty table means no
    candidate is ever found to have a socket, so no gem is applied anywhere and
    every sim runs a socket short. That deflates baseline_dps and with it the
    cross-spec tierlist built from it — silently, and identically for every spec,
    so nothing downstream looks wrong. A missing or unreadable file raises.
    """
    global _BONUS_SOCKET_COUNTS
    if _BONUS_SOCKET_COUNTS is None:
        data = json.loads((STATIC_DIR / "bonuses.json").read_text(encoding="utf-8"))
        _BONUS_SOCKET_COUNTS = {
            str(k): int(v.get("socket", 0))
            for k, v in data.items()
            if isinstance(v, dict) and v.get("socket")
        }
    return _BONUS_SOCKET_COUNTS


_ENCHANT_STATIC = None


def load_enchant_static():
    """(valid_enchant_ids, gem_lookup) from enchantments.json.

    valid_enchant_ids: set of int enchantment ids the static data knows — used
    to drop stale/bogus ids the same way the spec page's fetch_enchant_info
    does. gem_lookup: gem item_id (int) -> {limit_category, limit_quantity}
    for entries with slot == "socket" (itemLimitCategory caps unique gems).

    Required. Both returns are used as allow-lists — fetch_enchant_map keeps only
    ids in valid_enchant_ids and fetch_gem_ranking only gems in gem_lookup — so
    empty tables don't mean "unknown", they mean every enchant and every gem is
    dropped and the sims run bare. A missing or unreadable file raises rather
    than quietly producing that.

    Note this is about the FILE being absent. Individual ids absent from a file
    that did load are the intended old-enchant noise filter, and stay a drop.
    """
    global _ENCHANT_STATIC
    if _ENCHANT_STATIC is None:
        valid, gems = set(), {}
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


def _make_candidate(item_id, bonus_list, count, item_lookup,
                    embellish_limits=None, socket_bonus_counts=None):
    """Build one full candidate dict for an equipped item variant (id + bonus_list).

    This is the single candidate shape every producer emits — the popularity pool
    (gather_candidates), the Top-50 per-slot picks (top50_per_slot_gear), the
    injected whole player sets and the resolved tier combos — so all of them flow
    through set_is_valid / legalize_set / gear_line identically. Keeping the shape
    in one place is why a Top-50 or tier item legalizes and sims exactly like a
    popular one.

    Sockets are inherent + bonus, via the one shared commonUtils helper the spec
    page's convert_slots also uses (see the item-socket-count skill), so the
    simmed item's gem count matches the one shown there. Only the embellishments
    that actually consume an equip budget constrain the set; several consume
    nothing (see load_embellishment_limits)."""
    if embellish_limits is None:
        embellish_limits = load_embellishment_limits()
    if socket_bonus_counts is None:
        socket_bonus_counts = load_bonus_socket_counts()
    meta = item_lookup.get(int(item_id), {})
    # bonus_list is a comma-separated string (e.g. "8791,12384,..."); split into
    # ids before testing membership — iterating the raw string would walk it
    # character-by-character and never match an embellishment id.
    bonus_ids = (
        [b.strip() for b in str(bonus_list).split(",") if b.strip()]
        if bonus_list else []
    )
    emb_hits = [b for b in bonus_ids if b in embellish_limits]
    emb_limits = [embellish_limits[b] for b in emb_hits if embellish_limits[b]]
    socket_count = commonUtils.count_item_sockets(
        bonus_ids, socket_bonus_counts, meta.get("socketInfo")
    )
    return {
        "item_id": int(item_id),
        "count": int(count or 0),
        "bonus_list": bonus_list or None,
        "simc_bonus": bonus_to_simc(bonus_list),
        "item_set_id": meta.get("itemSetId"),
        "inv_type": meta.get("inventoryType"),
        "unique_equipped": bool(meta.get("uniqueEquipped")),
        "item_limit": meta.get("itemLimit"),
        "has_embellishment": bool(emb_hits),
        "embellish_limits": emb_limits,
        "socket_count": socket_count,
    }


def top50_per_slot_gear(loadouts, item_lookup):
    """slot -> the Top-50 verified players' most-common equipped item, as a full
    candidate dict (see _make_candidate).

    The per-slot vote is the same one the tierlist "Top 50" bar uses
    (generateSimcProfiles._top50_gear, which now calls this): each top-50 player
    contributes one loadout per dungeon, the most-common item wins (ties toward
    the higher count then lower id), and the most-common bonus set for that item
    wins (ties by the string) — both deterministic across runs regardless of dict
    order. Emitting the full candidate shape (not the lighter display subset) is
    what lets these picks reseed the baseline and be unioned into the candidate
    pool, where they must legalize like any pool candidate."""
    embellish_limits = load_embellishment_limits()
    socket_bonus_counts = load_bonus_socket_counts()
    slot_item_counts = {}          # slot -> item_id -> count
    slot_item_bonus = {}           # slot -> item_id -> bonus_str -> count
    for lo in loadouts or []:
        for it in lo.get("items", []) or []:
            slot = it.get("slot")
            item_id = it.get("item_id")
            if slot not in DB_TO_SIMC_SLOT or not item_id:
                continue
            item_id = int(item_id)
            bonus_str = it.get("bonus_ids") or ""
            slot_item_counts.setdefault(slot, {})
            slot_item_counts[slot][item_id] = slot_item_counts[slot].get(item_id, 0) + 1
            slot_item_bonus.setdefault(slot, {}).setdefault(item_id, {})
            slot_item_bonus[slot][item_id][bonus_str] = (
                slot_item_bonus[slot][item_id].get(bonus_str, 0) + 1
            )

    gear = {}
    for slot in ALL_SLOTS:
        counts = slot_item_counts.get(slot)
        if not counts:
            continue
        item_id = max(counts, key=lambda i: (counts[i], -i))
        bonus_counts = slot_item_bonus[slot][item_id]
        bonus_str = max(bonus_counts, key=lambda b: (bonus_counts[b], b))
        gear[slot] = _make_candidate(
            item_id, bonus_str, counts[item_id], item_lookup,
            embellish_limits, socket_bonus_counts,
        )
    return gear


def _top50_item_bonus(loadouts):
    """item_id -> the Top-50 players' most-common bonus_ids string for that item
    (deterministic tie-break by the string). Used to give a resolved tier piece
    the current bonus_ids (and thus item level) the top players actually wore."""
    counts = {}   # item_id -> {bonus_str: count}
    for lo in loadouts or []:
        for it in lo.get("items") or []:
            iid = it.get("item_id")
            if not iid:
                continue
            b = it.get("bonus_ids") or ""
            counts.setdefault(int(iid), {})
            counts[int(iid)][b] = counts[int(iid)].get(b, 0) + 1
    out = {}
    for iid, bc in counts.items():
        best = max(bc, key=lambda b: (bc[b], b))
        out[iid] = best or None
    return out


def gather_candidates(conn, cursor, spec_id, season, item_lookup, top50_gear=None):
    """slot -> ordered list of candidate dicts (most-popular first).

    Each candidate is the full shape from _make_candidate.

    Rare/stale items are dropped: the aggregated pool occasionally surfaces old
    expansions' items (e.g. a Legion ring) that get current-season bonus_ids
    applied and produce nonsense in simc. We keep only candidates whose equip
    count is at least SIMC_MIN_CANDIDATE_FRACTION of the slot's most-popular item
    (the top item always passes).

    `top50_gear` (from top50_per_slot_gear) unions the Top-50 verified players'
    current per-slot item into each slot's bag: it is appended (deduped by
    item_id+bonus_list via _same_cand) even when population popularity has not
    caught up to it yet, so it BYPASSES the SIMC_MIN_CANDIDATE_FRACTION floor —
    the whole point is to keep the clean current gear that the stale pool would
    otherwise drop early in a season. When a slot has no popularity rows at all
    the Top-50 pick still seeds the bag so the reseeded baseline can use it."""
    embellish_limits = load_embellishment_limits()
    socket_bonus_counts = load_bonus_socket_counts()
    out = {}
    group_cache = {}  # slot_group -> group rows, so each pair's query runs once
    for slot in ALL_SLOTS:
        rows = fetch_slot_rows(conn, cursor, spec_id, season, slot, group_cache)
        cands = []
        if rows:
            top_count = max((int(r.get("count", 0)) for r in rows), default=0)
            floor = top_count * SIMC_MIN_CANDIDATE_FRACTION
            for r in rows[:SIMC_CANDIDATES_PER_SLOT]:
                count = int(r.get("count", 0))
                if count < floor:
                    continue
                item_id = int(r["item"])
                bonus_list = (r.get("bonus") or {}).get("ids") if r.get("bonus") else None
                cands.append(_make_candidate(
                    item_id, bonus_list, count, item_lookup,
                    embellish_limits, socket_bonus_counts,
                ))
        # Union the Top-50 current pick for this slot (bypasses the popularity
        # floor). Appended after the popularity picks so trim_bag drops it last
        # among equally-unpopular tails; deterministic (one pick per slot).
        if top50_gear and slot in top50_gear:
            tc = top50_gear[slot]
            if not any(_same_cand(tc, c) for c in cands):
                cands.append(tc)
        if cands:
            out[slot] = cands
    return out


# --------------------------------------------------------------------------
# Enchants & gems (held constant, sourced from the top-50 player loadouts)
# --------------------------------------------------------------------------

def fetch_enchant_map(conn, cursor, spec_id, season):
    """Enchant group -> most popular RELEVANT enchantment_id.

    Primary source is the top-50 player loadouts; groups with no top-50 data
    fall back to the global aggregation (same source as the spec page's enchant
    dropdowns). Candidates pass through the SAME shared predicate the spec and
    item pages use, commonUtils.is_enchant_relevant (catalog membership + current
    expansion + equipRequirements slot fit), so the sim can never enchant with an
    old-expansion or slot-incompatible enchant the pages hide -- those are what
    simc rejects (e.g. an old enchant capped below the current item's item level).
    Ties break toward the higher count then the lower id so the pick, and thus the
    profile text and its resume signature, are stable across runs.
    """
    catalog = commonUtils.load_enchant_catalog()
    current_expansion = commonUtils.current_expansion_id()

    def _relevant(eid, grp):
        return commonUtils.is_enchant_relevant(catalog.get(int(eid)), current_expansion, grp)

    merged = {}  # group -> {enchant_id: count}
    try:
        raw = databaseConnector.fetch_top50_enchant_ranking(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"could not fetch top-50 enchants for spec {spec_id}: {e}")
        raw = {}
    for sg, pairs in raw.items():
        grp = enchant_group(sg)
        for eid, cnt in pairs:
            if eid is not None and _relevant(eid, grp):
                merged.setdefault(grp, {})
                merged[grp][eid] = merged[grp].get(eid, 0) + cnt

    out = {grp: max(counts.items(), key=lambda kv: (kv[1], -int(kv[0])))[0]
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
            if eid is not None and _relevant(int(eid), grp):
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
    """Yield (category, max_quantity) limit contributions for a candidate: the
    item's own itemLimit (unique-equipped categories, alchemist stones, ...) plus
    the real itemLimit of every embellishment it carries — which is per
    embellishment, not a blanket 512/2 (see load_embellishment_limits)."""
    out = []
    lim = cand.get("item_limit")
    if lim and lim.get("category") is not None:
        out.append((lim["category"], lim.get("quantity")))
    for cat, qty in cand.get("embellish_limits") or []:
        out.append((cat, qty))
    return out


def _consumes_limit(cand):
    """True if the candidate spends an equip-limit budget shared with other slots
    (an embellishment, an alchemist-stone-style itemLimit category). Such a pick
    can only ever be worn alongside a limited number of its peers."""
    return bool(cand) and bool(candidate_limit_categories(cand))


def set_violations(chosen):
    """Every equip limit a full equipped set breaks.

    chosen: dict slot -> candidate. Returns a list of
    {"kind": "unique"|"category", "key": item_id|category, "limit": qty,
    "slots": [slot, ...]} — the slots being the picks that contribute to it.
    Covers unique-equipped (no duplicate of the same unique item across slots)
    and per-category itemLimit quantities (the embellishment cap,
    alchemist-stone-style unique categories, etc.)."""
    unique_slots = {}   # item_id -> [slot, ...]
    cat_slots = {}      # category -> [slot, ...]
    cat_limit = {}
    for slot, cand in chosen.items():
        if not cand:
            continue
        if cand.get("unique_equipped"):
            unique_slots.setdefault(cand["item_id"], []).append(slot)
        for cat, qty in candidate_limit_categories(cand):
            cat_slots.setdefault(cat, []).append(slot)
            if qty is not None:
                cat_limit[cat] = qty if cat not in cat_limit else min(cat_limit[cat], qty)
    out = []
    for iid, slots in unique_slots.items():
        if len(slots) > 1:
            out.append({"kind": "unique", "key": iid, "limit": 1, "slots": slots})
    for cat, slots in cat_slots.items():
        q = cat_limit.get(cat)
        if q is not None and len(slots) > q:
            out.append({"kind": "category", "key": cat, "limit": q, "slots": slots})
    return out


def set_is_valid(chosen):
    """True if a full equipped set respects every equip limit (see set_violations)."""
    return not set_violations(chosen)


def _violation_label(v):
    """Human-readable name of a violated constraint, for logs and alerts."""
    if v["kind"] == "unique":
        return f"unique-equipped item {v['key']}"
    if v["key"] == EMBELLISH_LIMIT_CATEGORY:
        return f"the {v['limit']}-embellishment cap"
    return f"itemLimit category {v['key']} (max {v['limit']})"


def _violation_reason(chosen, violations):
    """One-line explanation of why a set is illegal, naming slots and item ids."""
    parts = []
    for v in violations:
        items = ", ".join(
            f"{s} {chosen[s]['item_id']}" for s in sorted(v["slots"]) if chosen.get(s)
        )
        parts.append(f"{len(v['slots'])} equipped items break {_violation_label(v)}: {items}")
    return "; ".join(parts)


def _hits_violation(cand, v):
    """True if `cand` contributes to the constraint violated in `v`."""
    if not cand:
        return False
    if v["kind"] == "unique":
        return bool(cand.get("unique_equipped")) and cand["item_id"] == v["key"]
    return any(cat == v["key"] for cat, _ in candidate_limit_categories(cand))


def legalize_set(chosen, candidates, locked=()):
    """Demote offending picks until an equipped set respects every equip limit.

    Candidate bags are ranked per slot independently, so a set assembled from them
    can break limits no single slot can see: more than EMBELLISH_LIMIT_QUANTITY
    embellishments (illegal in-game — only two ever apply, and simc would apply
    them all and inflate the set's DPS), the same unique-equipped ring twice, and
    so on. For each violation we demote the LEAST popular offending slot — never a
    `locked` one (the tier pieces a scenario deliberately pins) — to that slot's
    most-popular candidate which doesn't contribute to the violated constraint,
    and re-check. Mutates and returns (chosen, unresolved), `unresolved` listing
    the violations no demotion could fix.
    """
    locked = set(locked)
    stuck = set()
    # Each successful demotion removes one contributor from the violated
    # constraint, but can introduce a different one; bound the loop so a pair of
    # constraints that keep pushing each other around can never spin forever.
    for _ in range(4 * len(ALL_SLOTS)):
        pending = [v for v in set_violations(chosen) if (v["kind"], v["key"]) not in stuck]
        if not pending:
            break
        v = pending[0]
        # least popular first: demote the pick the fewest players actually equip
        offenders = sorted((s for s in v["slots"] if s not in locked),
                           key=lambda s: chosen[s].get("count") or 0)
        for slot in offenders:
            alt = next((c for c in candidates.get(slot, []) if not _hits_violation(c, v)), None)
            if alt is not None:
                _log(f"legalize: {slot} {chosen[slot]['item_id']} -> {alt['item_id']} "
                     f"to respect {_violation_label(v)}")
                chosen[slot] = alt
                break
        else:
            stuck.add((v["kind"], v["key"]))
    return chosen, set_violations(chosen)


def legalize_baseline_embellishments(baseline, candidates):
    """Demote excess picks so the popular baseline respects every equip limit.

    The popular baseline takes the most-popular item per slot independently, which
    can equip more than EMBELLISH_LIMIT_QUANTITY embellishments (or the same
    unique-equipped item twice). Thin wrapper over legalize_set, which keeps the
    most-popular offending picks and swaps the rest. Mutates and returns `baseline`.
    """
    baseline, unresolved = legalize_set(baseline, candidates)
    for v in unresolved:
        _log(f"baseline: no legal candidate in {sorted(v['slots'])} for "
             f"{_violation_label(v)}; the popular set stays illegal")
    return baseline


def _combo_count(opts):
    """Cartesian size of a per-slot option bag (slot -> list of candidates)."""
    n = 1
    for v in opts.values():
        n *= len(v)
        if n > 10 ** 18:
            return n  # effectively unbounded; caller will trim
    return n


def promote_limit_free_alternative(bag):
    """Move each slot's most-popular limit-free candidate up to index 1 whenever
    the slot's top pick spends an equip-limit budget.

    trim_bag shrinks bags from the tail, so without this the one alternative that
    could keep a set legal is usually trimmed away long before the limited head
    is — see trim_bag's protection rule. Relative popularity order is otherwise
    preserved. Mutates and returns `bag`."""
    for slot, cands in bag.items():
        if not cands or not _consumes_limit(cands[0]):
            continue
        i = next((k for k, c in enumerate(cands) if not _consumes_limit(c)), None)
        if i is None or i == 1:
            continue
        bag[slot] = [cands[0], cands[i]] + [c for k, c in enumerate(cands) if k not in (0, i)]
    return bag


def trim_bag(opts, cap):
    """Trim the least-popular candidate from the bag's largest slot until the
    cartesian product fits `cap`. Candidates are most-popular first, so popping
    the tail drops the least-equipped option. Every slot keeps >= 1 candidate.

    A slot whose head spends an equip-limit budget is protected from collapsing
    onto that single pick while a limit-free alternative is still in its bag:
    collapsing it FORCES the limited item into every combination, and three such
    forced slots make every set illegal (the embellishment cap is 2) — the spec
    would then produce no valid combination at all. Protected slots keep
    [limited head, best limit-free alternative] so the sim decides which limited
    items are worth wearing. The cap still wins: protection is dropped once
    nothing else can be trimmed."""
    def _protected(slot):
        v = opts[slot]
        return len(v) == 2 and _consumes_limit(v[0]) and not _consumes_limit(v[1])

    while _combo_count(opts) > cap:
        slot = max((s for s, v in opts.items() if len(v) > 1 and not _protected(s)),
                   key=lambda s: len(opts[s]), default=None)
        if slot is None:   # everything left is protected — the cap takes priority
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


def _drop_two_hand_offhand(gear, spec_id, item_lookup):
    """Drop the OFF_HAND slot when the main hand is a two-hander / ranged weapon,
    except for Titan's Grip Fury (DUAL_WIELD_TWOHAND_SPECS), which wields a
    two-hander in the off-hand too. Mutates `gear`. Shared by the seed baseline,
    the injected player sets and the resolved tier combos so all three honour the
    same handedness rule."""
    mh = gear.get("MAIN_HAND")
    if (mh and spec_id not in DUAL_WIELD_TWOHAND_SPECS
            and item_lookup.get(mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES):
        gear.pop("OFF_HAND", None)


def _ordered_gear(gear):
    """Return `gear` as a new dict in fixed ALL_SLOTS order, so the generated
    .simc text (and thus the resume signature) is identical every run regardless
    of the order slots were assembled in."""
    return {s: gear[s] for s in ALL_SLOTS if s in gear}


def _set_dedup_key(gear):
    """Canonical (slot, item_id, bonus_list) key for a full set, in ALL_SLOTS
    order — the deterministic key used to dedup combos."""
    return tuple(
        (s, gear[s]["item_id"], gear[s].get("bonus_list"))
        for s in ALL_SLOTS if gear.get(s)
    )


def build_injected_sets(loadouts, candidates, item_lookup, spec_id,
                        enchant_map, gem_ranking):
    """Each distinct Top-50 player's whole verified gearset, as a candidate set.

    Coherent known-good sets are evaluated as-is rather than only reachable
    through many simultaneous per-slot swaps of the capped cartesian product.
    Loadouts are grouped by player (character_id); one representative loadout per
    player is chosen deterministically (lowest rank, then lowest map id). Each
    item maps to a slot candidate — reusing the pool candidate when
    item_id+bonus_list matches (so it carries the pool's popularity count) else a
    fresh _make_candidate. Enchants/gems are filled the same constant way the
    baseline gets them (apply_enchants_and_gems), so an injected set is not
    unfairly simmed bare against the enchanted enumerated combos. Identical full
    sets are deduped; the result is ordered by rank. Sets are NOT run through the
    main-hand handedness filter (they were coherent in game); only the
    two-hand off-hand rule is applied defensively."""
    embellish_limits = load_embellishment_limits()
    socket_bonus_counts = load_bonus_socket_counts()

    by_player = {}
    for lo in loadouts or []:
        cid = lo.get("character_id")
        if cid is None:
            continue
        rank = lo.get("rank")
        rank = int(rank) if rank is not None else 10 ** 9
        map_id = lo.get("map_challenge_mode_id")
        map_id = int(map_id) if map_id is not None else 10 ** 9
        key = (rank, map_id)
        prev = by_player.get(cid)
        if prev is None or key < prev[0]:
            by_player[cid] = (key, lo)

    reps = sorted(by_player.items(), key=lambda kv: (kv[1][0][0], kv[1][0][1], kv[0]))

    sets = []
    seen = set()
    for cid, (key, lo) in reps:
        gear = {}
        for it in lo.get("items") or []:
            slot = it.get("slot")
            iid = it.get("item_id")
            if slot not in DB_TO_SIMC_SLOT or not iid:
                continue
            iid = int(iid)
            bonus_list = it.get("bonus_ids") or None
            bag = candidates.get(slot) or []
            match = next(
                (c for c in bag
                 if c.get("item_id") == iid and (c.get("bonus_list") or None) == bonus_list),
                None,
            )
            gear[slot] = dict(match) if match is not None else _make_candidate(
                iid, bonus_list, 0, item_lookup, embellish_limits, socket_bonus_counts
            )
        if not gear:
            continue
        _drop_two_hand_offhand(gear, spec_id, item_lookup)
        gear = _ordered_gear(gear)
        # Constant enchants/gems over the equipped set (own per-category budget).
        apply_enchants_and_gems({s: [c] for s, c in gear.items()},
                                enchant_map, gem_ranking, item_lookup)
        dk = _set_dedup_key(gear)
        if dk in seen:
            continue
        seen.add(dk)
        sets.append({"rank": key[0], "label": f"top50:r{key[0]}", "gear": gear})
    return sets


def _pick_comp_bonus(item_id, slot, top50_item_bonus, candidates, baseline):
    """Bonus list for a tier-comp piece, preferring the Top-50 players' most-common
    bonus for that exact item, else a pool candidate's bonus for it, else the
    slot's baseline bonus. bonus_ids drive the item level, so getting them wrong
    skews DPS (see the edge-case note in the plan)."""
    b = (top50_item_bonus or {}).get(int(item_id))
    if b:
        return b
    for c in candidates.get(slot, []) or []:
        if c.get("item_id") == int(item_id) and c.get("bonus_list"):
            return c.get("bonus_list")
    base = baseline.get(slot)
    if base and base.get("bonus_list"):
        return base.get("bonus_list")
    return None


def build_tier_comps(rows, candidates, baseline, item_lookup, tier_item_to_set,
                     top50_item_bonus, spec_id, enchant_map, gem_ranking):
    """Resolve every aggregated_tier_set_comps row into a whole override set.

    Each `comp` is a canonical ascending comma list of the tier item ids a member
    wore at 2pc+ (a multi-set comp mixes several sets' pieces). Each item is
    placed on its slot (via inventoryType), the exact comp pieces are equipped,
    and every remaining tier-eligible ARMOUR slot the comp does not name is filled
    with the best pool off-piece that belongs to NONE of the comp's sets — so simc
    applies exactly that comp's set bonuses and no more. Non-tier slots inherit the
    seed. Items absent from item_lookup are skipped; if that drops the comp below
    2pc of every resolvable set the whole comp is skipped. Enchants/gems are filled
    like the baseline. Ordered by (total_runs desc, comp); identical sets deduped.

    Returns a list of {comp, total_runs, label, gear, locked_slots} where
    locked_slots are the comp pieces (kept pinned through legalize_set)."""
    embellish_limits = load_embellishment_limits()
    socket_bonus_counts = load_bonus_socket_counts()
    tier_armour_slots = set(TIER_INVTYPE_TO_SLOT.values())

    ordered = sorted(rows or [], key=lambda r: (-int(r[1] or 0), str(r[0])))
    out = []
    seen = set()
    for row in ordered:
        comp = str(row[0])
        total_runs = int(row[1] or 0)
        ids = []
        for tok in comp.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                ids.append(int(tok))
            except ValueError:
                continue
        if not ids:
            continue

        worn = {}
        comp_set_ids = set()
        pair_next = {}
        for iid in ids:
            meta = item_lookup.get(iid)
            if not meta:
                _log(f"tier comp {comp}: item {iid} absent from item_lookup, skipping the item")
                continue
            slot = _invtype_to_slot(meta.get("inventoryType"), pair_next)
            if slot is None:
                _log(f"tier comp {comp}: item {iid} inventoryType "
                     f"{meta.get('inventoryType')!r} maps to no slot, skipping the item")
                continue
            set_id = tier_item_to_set.get(iid)
            if set_id is None:
                set_id = meta.get("itemSetId")
            if set_id is not None:
                comp_set_ids.add(set_id)
            bonus_list = _pick_comp_bonus(iid, slot, top50_item_bonus, candidates, baseline)
            worn[slot] = _make_candidate(
                iid, bonus_list, 0, item_lookup, embellish_limits, socket_bonus_counts
            )

        # Still >= 2pc of at least one resolvable set after filtering unusable items?
        set_counts = {}
        for c in worn.values():
            sid = c.get("item_set_id")
            if sid is not None:
                set_counts[sid] = set_counts.get(sid, 0) + 1
        if not any(v >= 2 for v in set_counts.values()):
            _log(f"tier comp {comp}: fewer than 2 resolvable set pieces after "
                 f"filtering, skipping the comp")
            continue

        gear = {s: dict(baseline[s]) for s in ALL_SLOTS if s in baseline}
        gear.update(worn)
        # Fill the tier armour slots the comp does not name with a non-comp-set
        # off-piece so the exact pc-count is preserved (a seed piece there could be
        # a member of the comp's set and silently bump 2pc to 4pc).
        for s in tier_armour_slots:
            if s in worn:
                continue
            off = next(
                (c for c in (candidates.get(s) or [])
                 if c.get("item_set_id") not in comp_set_ids),
                None,
            )
            if off is not None:
                gear[s] = dict(off)
            elif s in gear and gear[s].get("item_set_id") in comp_set_ids:
                _log(f"tier comp {comp}: no non-set off-piece candidate for {s}; "
                     f"the seed piece there may inflate the set bonus")

        _drop_two_hand_offhand(gear, spec_id, item_lookup)
        gear = _ordered_gear(gear)
        apply_enchants_and_gems({s: [c] for s, c in gear.items()},
                                enchant_map, gem_ranking, item_lookup)
        dk = _set_dedup_key(gear)
        if dk in seen:
            continue
        seen.add(dk)
        out.append({
            "comp": comp,
            "total_runs": total_runs,
            "label": f"tier:{comp}",
            "gear": gear,
            "locked_slots": sorted(worn),
        })
    return out


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
        "fight_style=Patchwerk",
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
    # Fixed ALL_SLOTS order so the base actor's gear lines (and thus the resume
    # signature) never depend on the order the baseline dict was assembled in.
    for slot in ALL_SLOTS:
        cand = baseline_gear.get(slot)
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


def _enumerate_tier_scenarios(candidates, baseline, active_slots, tier_set_id,
                              tier_slots, item_lookup, cap, slot_bag, add_combo):
    """Legacy detect_tier-driven enumeration, used only when no real tier combos
    are available (see build_combinations' `tier_comps` fallback).

    Enumerates "wear the full set" plus, with >=5 tier slots, "drop one slot to an
    off-piece" (always keeping >=4pc), letting full-set DPS settle the
    tier-vs-off-piece choice. Appends each legal combo via `add_combo(full, label)`
    (which dedups and enforces the overall cap). Returns (used_labels, blockers)."""
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

    normal_slots = [s for s in active_slots if s not in tiered_slots]
    normal_bag = {s: slot_bag(s, candidates.get(s, [])) for s in normal_slots if candidates.get(s)}
    normal_bag = {s: v for s, v in normal_bag.items() if v}

    per_scenario_cap = max(1, cap // len(scenarios))
    used_labels = []
    blockers = {}     # why -> [scenario label, ...]
    for label, kept_tier, dropped in scenarios:
        bag = {s: list(v) for s, v in normal_bag.items()}
        if dropped:
            off = [c for c in candidates.get(dropped, []) if c.get("item_set_id") != tier_set_id]
            if not off:
                continue   # nothing to drop to; "all" already covers wearing it
            bag[dropped] = slot_bag(dropped, off)
        promote_limit_free_alternative(bag)
        trim_bag(bag, per_scenario_cap)
        fixed_slots = {s: v[0] for s, v in bag.items() if len(v) == 1}
        vary = {s: v for s, v in bag.items() if len(v) > 1}
        scen_fixed = dict(baseline)        # seed per slot ...
        scen_fixed.update(kept_tier)       # ... tier slots wear the set ...
        if dropped:
            scen_fixed.pop(dropped, None)   # ... except the dropped slot (from bag)
        scen_fixed.update(fixed_slots)
        # The trim above pins every collapsed slot to its raw most-popular item,
        # which silently undoes the demotions legalize_baseline_embellishments
        # made on the baseline. Left alone, a core carrying three embellishments
        # makes EVERY enumerated set illegal, so legalize the pinned part (tier
        # pieces locked) before enumerating — the varying slots are excluded
        # because their pick comes from the product below, where
        # enumerate_valid_combos already prunes illegal sets.
        core = {s: c for s, c in scen_fixed.items() if s not in vary}
        core, unresolved = legalize_set(core, candidates, locked=set(kept_tier))
        scen_fixed.update(core)
        combos = enumerate_valid_combos(scen_fixed, vary, per_scenario_cap)
        if not combos:
            heads = {**scen_fixed, **{s: v[0] for s, v in vary.items()}}
            why = (_violation_reason(heads, unresolved or set_violations(heads))
                   or "no legal combination")
            blockers.setdefault(why, []).append(label)
        for chosen in combos:
            add_combo({**scen_fixed, **chosen}, label)
        used_labels.append(label)
    return used_labels, blockers


def _enumerate_single_base(candidates, baseline, active_slots, item_lookup, cap,
                           slot_bag, add_combo):
    """Per-slot cartesian enumeration around a single base (no tier scenario
    split): used when real tier combos are simmed explicitly, so tier
    configuration is handled by those combos rather than a detect_tier sweep.

    Appends each legal combo via `add_combo`. Returns (used_labels, blockers)."""
    bag = {s: slot_bag(s, candidates.get(s, [])) for s in active_slots if candidates.get(s)}
    bag = {s: v for s, v in bag.items() if v}
    promote_limit_free_alternative(bag)
    trim_bag(bag, cap)
    fixed_slots = {s: v[0] for s, v in bag.items() if len(v) == 1}
    vary = {s: v for s, v in bag.items() if len(v) > 1}
    scen_fixed = dict(baseline)
    scen_fixed.update(fixed_slots)
    core = {s: c for s, c in scen_fixed.items() if s not in vary}
    core, unresolved = legalize_set(core, candidates)
    scen_fixed.update(core)
    combos = enumerate_valid_combos(scen_fixed, vary, cap)
    blockers = {}
    if not combos:
        heads = {**scen_fixed, **{s: v[0] for s, v in vary.items()}}
        why = (_violation_reason(heads, unresolved or set_violations(heads))
               or "no legal combination")
        blockers.setdefault(why, []).append("enum")
    for chosen in combos:
        add_combo({**scen_fixed, **chosen}, "enum")
    return ["enum"], blockers


def build_combinations(candidates, baseline, active_slots, tier_set_id, tier_slots,
                       item_lookup, cap, injected_sets=None, tier_comps=None):
    """Build the Top-Gear-style full-set combinations to sim for a spec.

    Each combination is a complete legal equipped set (equip limits enforced).
    The base actor is seeded from the legalized Top-50 baseline; the high-value
    coherent sets are guaranteed a place before the per-slot search fills the rest
    of the budget:

      1. the seed baseline itself (all_combos[0]);
      2. each injected whole Top-50 player set (legalized; the main-hand
         handedness filter is NOT applied — they were coherent in game);
      3. each resolved tier-set combo (legalized with its comp pieces locked);
      4. per-slot cartesian enumeration filling the remaining budget.

    A set that cannot be legalized is dropped with a log rather than simmed
    illegal. Combos are deduped by a canonical per-slot key. The reserved
    guaranteed block (1-3) is hard-capped at cap//2 so the enumerated search is
    never fully starved, and the seed always survives that clamp.

    When `tier_comps` is empty the per-slot search falls back to the legacy
    detect_tier scenario sweep (step 4 becomes the "all"/"drop:" enumeration);
    otherwise tier configuration is settled by the explicit combos in step 3.

    Returns (base_full, profilesets, index, all_combos, scenarios, reason) — the
    same tuple shape as before:
      base_full   : dict slot->cand seeding the simc base actor (the seed baseline)
      profilesets : list of (name, [(slot, cand), ...]) overrides vs base_full
      index       : name -> (full_set_dict, config_label)
      all_combos  : list of (full_set_dict, config_label)
      scenarios   : list of config labels explored
      reason      : None on success, else why no legal combination exists
    """
    injected_sets = injected_sets or []
    tier_comps = tier_comps or []

    # Main hand pinned to the baseline's handedness for the per-slot search. A
    # one-hand baseline never pulls in a two-hander (and vice versa); the off-hand
    # only rides along when the baseline kept one — i.e. for 1H specs and Titan's
    # Grip Fury, but not for plain two-hand specs.
    base_mh = baseline.get("MAIN_HAND")
    base_mh_2h = bool(base_mh and item_lookup.get(base_mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES)

    def slot_bag(slot, cands):
        if slot == "MAIN_HAND":
            cands = [c for c in cands
                     if (item_lookup.get(c["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES) == base_mh_2h]
            if not cands and base_mh:
                cands = [base_mh]
        return list(cands)

    all_combos = []   # list of (full_set_dict, config_label)
    seen = set()

    def add_combo(full, label):
        key = _set_dedup_key(full)
        if key in seen:
            return False
        seen.add(key)
        all_combos.append((full, label))
        return True

    # ---- 1. seed baseline (always all_combos[0]) ----
    base_full = {s: baseline[s] for s in ALL_SLOTS if s in baseline}
    base_full, _base_unresolved = legalize_set(base_full, candidates)
    add_combo(base_full, "seed")

    # ---- 2 + 3. guaranteed coherent sets, hard-capped at cap//2 ----
    reserved_cap = max(1, cap // 2)
    for inj in injected_sets:                     # caller pre-sorts by rank
        if len(all_combos) >= reserved_cap:
            break
        full = {s: c for s, c in inj["gear"].items()}
        full, unresolved = legalize_set(full, candidates)
        if unresolved:
            _log(f"dropping injected set {inj.get('label')}: cannot legalize "
                 f"({_violation_reason(full, unresolved)})")
            continue
        add_combo(_ordered_gear(full), inj["label"])
    for tc in tier_comps:                         # caller pre-sorts by total_runs desc
        if len(all_combos) >= reserved_cap:
            break
        full = {s: c for s, c in tc["gear"].items()}
        full, unresolved = legalize_set(full, candidates, locked=set(tc.get("locked_slots") or ()))
        if unresolved:
            _log(f"dropping tier comp {tc.get('comp')}: cannot legalize "
                 f"({_violation_reason(full, unresolved)})")
            continue
        add_combo(_ordered_gear(full), tc["label"])

    # ---- 4. fill the remaining budget with the per-slot search ----
    enum_cap = max(1, cap - len(all_combos))
    if tier_comps:
        used_labels, blockers = _enumerate_single_base(
            candidates, baseline, active_slots, item_lookup, enum_cap, slot_bag, add_combo
        )
    else:
        used_labels, blockers = _enumerate_tier_scenarios(
            candidates, baseline, active_slots, tier_set_id, tier_slots,
            item_lookup, enum_cap, slot_bag, add_combo
        )
    used_labels = list(dict.fromkeys(
        [lbl for _, lbl in all_combos] + used_labels
    ))

    if not all_combos:
        reason = "; ".join(f"{why} ({', '.join(labels)})"
                           for why, labels in blockers.items())
        return None, [], {}, [], used_labels, reason or "no legal gear combination"

    # Seed the base actor with the first combo (the seed baseline); express every
    # other combo as a profileset overriding only the slots that differ from it.
    base_full, _ = all_combos[0]
    profilesets = []
    index = {}
    for i, (full, label) in enumerate(all_combos[1:], start=1):
        overrides = [(s, full[s]) for s in ALL_SLOTS
                     if full.get(s) and not _same_cand(full.get(s), base_full.get(s))]
        if not overrides:
            continue
        name = f"g{i}"
        profilesets.append((name, overrides))
        index[name] = (full, label)
    return base_full, profilesets, index, all_combos, used_labels, None


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

    # Top-50 verified loadouts: the clean, current gear used both to reseed the
    # baseline and to inject whole coherent player sets (see build_injected_sets).
    try:
        loadouts = databaseConnector.fetch_top50_loadouts(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"could not fetch top-50 loadouts for spec {spec_id}: {e}")
        loadouts = []
    top50_gear = top50_per_slot_gear(loadouts, item_lookup)
    # Below MIN_TOP50_SLOTS the verified data is too thin to reseed from (early
    # season): fall back to the most-popular set, union nothing, inject nothing.
    use_top50 = len(top50_gear) >= MIN_TOP50_SLOTS

    candidates = gather_candidates(
        conn, cursor, spec_id, season, item_lookup,
        top50_gear if use_top50 else None,
    )
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
    _stat_log(stats, f"simc: spec {spec_id} ({class_name}/{spec_name}) "
                     f"tier_set={tier_set_id} slots={sorted(tier_slots)} "
                     f"top50_seed={use_top50}")

    # ---- baseline ----
    if use_top50:
        # Reseed from the Top-50 current gear. Reference the candidate objects that
        # were unioned into the pool (so they carry the enchants/gems apply_* just
        # attached, and legalize/enumeration see the same object). A slot the Top-50
        # vote did not cover (a player left it bare, or an armory row failed to
        # parse) still takes the most-popular pool item, so a slot the pool CAN
        # fill is never dropped from the whole profile (e.g. shoulders vanishing).
        baseline = {}
        for slot in ALL_SLOTS:
            bag = candidates.get(slot) or []
            tc = top50_gear.get(slot)
            if tc is not None:
                match = next((c for c in bag if _same_cand(c, tc)), None)
                baseline[slot] = match if match is not None else tc
            elif bag:
                baseline[slot] = bag[0]
    else:
        # Fallback: most-popular item per slot (unchanged pre-Top-50 behaviour).
        baseline = {slot: cands[0] for slot, cands in candidates.items()}

    # drop off_hand if main hand is a two-hander / ranged weapon — but not for
    # Titan's Grip Fury, which wields a two-hander in the off-hand too.
    _drop_two_hand_offhand(baseline, spec_id, item_lookup)

    # The per-slot picks above ignore cross-slot equip limits; the most common item
    # in three+ slots can be embellished. Keep the baseline legal (<=2
    # embellishments) so it isn't simmed with illegal, DPS-inflating stats.
    legalize_baseline_embellishments(baseline, candidates)
    baseline = {s: baseline[s] for s in ALL_SLOTS if s in baseline}

    active_slots = [s for s in ALL_SLOTS if s in baseline]

    # ---- injected whole Top-50 player sets ----
    injected_sets = []
    if use_top50:
        injected_sets = build_injected_sets(
            loadouts, candidates, item_lookup, spec_id,
            enchant_map, gem_ranking,
        )

    # ---- resolved tier-set combos (the exact combos the spec page lists) ----
    tier_item_to_set, _tier_set_meta = commonUtils.load_tier_sets(str(STATIC_DIR))
    # Top-50 players' most-common bonus per item id, so a resolved tier piece wears
    # the current bonus_ids (item level) the top players actually used.
    top50_item_bonus = _top50_item_bonus(loadouts)
    try:
        tier_rows = databaseConnector.fetch_tier_set_comps(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"could not fetch tier-set comps for spec {spec_id}: {e}")
        tier_rows = []
    tier_comps = build_tier_comps(
        tier_rows, candidates, baseline, item_lookup, tier_item_to_set,
        top50_item_bonus, spec_id, enchant_map, gem_ranking,
    )
    _stat_log(stats, f"simc: spec {spec_id} injected_sets={len(injected_sets)} "
                     f"tier_comps={len(tier_comps)}")

    return {
        "header": header,
        "candidates": candidates,
        "baseline": baseline,
        "tier_set_id": tier_set_id,
        "tier_slots": tier_slots,
        "active_slots": active_slots,
        "injected_sets": injected_sets,
        "tier_comps": tier_comps,
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
    return await simulate_prepared(spec_id, season, prep, item_lookup, stats)


# The subset of a prep dict that _build_run consumes. Persisted as JSON with an
# in-progress run so a restart resumes against the run's ORIGINAL inputs: the
# candidate bags come from popularity aggregations that are rebuilt nightly, so
# re-preparing after the ~daily container restart would usually change the
# generated profile, mismatch the run signature, and discard all banked chunks —
# the heaviest specs (the whole reason for chunking) would then never finish.
_PREP_SNAPSHOT_KEYS = ("header", "candidates", "baseline", "tier_set_id",
                       "tier_slots", "active_slots", "injected_sets", "tier_comps")


def _snapshot_prep(prep):
    """Serialize the build-relevant part of a prep dict to compact JSON."""
    data = {}
    for k in _PREP_SNAPSHOT_KEYS:
        v = prep.get(k)
        if isinstance(v, set):
            v = sorted(v)
        data[k] = v
    return json.dumps(data, separators=(",", ":"))


def _load_prep_snapshot(text):
    """Parse a stored prep snapshot; None if missing/corrupt/incomplete."""
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    if not all(k in data for k in _PREP_SNAPSHOT_KEYS):
        return None
    return data


def _build_run(prep, item_lookup):
    """Build the deterministic combination set for a prepared spec.

    Returns a dict describing the whole run — base actor, profileset list,
    name->combo index, config labels, iteration cap, and a `signature` (SHA-256
    of the exact full .simc text). The signature is what lets progress be
    resumed safely: any change to the candidate set, gear, or iteration settings
    changes the generated profile and therefore the signature, so stale
    checkpoints are discarded rather than mixed into a new run. The simc *build*
    is deliberately not part of the signature, so the 6-hourly image pulls don't
    invalidate a run mid-flight.

    Returns (build_dict, None), or (None, reason) when the spec has no legal
    combination — `reason` naming the equip limit and the slots/items that break
    it, so the alert says what is actually wrong instead of "no valid combos".
    """
    header = prep["header"]
    candidates = prep["candidates"]
    baseline = prep["baseline"]
    tier_set_id = prep["tier_set_id"]
    # detect_tier returns a SET; its iteration order seeds tier_pieces and thus
    # the scenario/profileset order in the generated text. Python randomises
    # string hashing per process, so an unsorted set would change the signature
    # across restarts even with identical data — sort for determinism (also
    # normalises the list form a JSON prep snapshot restores).
    tier_slots = sorted(prep["tier_slots"])
    active_slots = prep["active_slots"]
    # Both come from _prepare_spec as already-ordered lists (injected by rank, tier
    # by total_runs desc) and are round-tripped verbatim through the JSON prep
    # snapshot, so a resume rebuilds the identical combos and the same signature.
    injected_sets = prep.get("injected_sets") or []
    tier_comps = prep.get("tier_comps") or []

    # ---- Top-Gear-style full-set combinations ----
    # Evaluate whole-set combinations rather than optimising one slot at a time,
    # pruning any set that breaks an equip limit. This captures cross-slot
    # interactions and keeps the recommended set legal (<=2 embellishments, no
    # duplicate unique-equipped item, itemLimit categories respected). The
    # high-value coherent sets (the Top-50 seed, each injected player set and each
    # real tier combo) are simmed as intact sets alongside the per-slot search
    # (see build_combinations).
    try:
        combo_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        combo_iters = None
    if combo_iters is not None and combo_iters <= 0:
        combo_iters = None

    base_full, profilesets, index, all_combos, scenarios, reason = build_combinations(
        candidates, baseline, active_slots, tier_set_id, tier_slots,
        item_lookup, SIMC_MAX_COMBINATIONS, injected_sets, tier_comps,
    )
    if not all_combos:
        return None, reason or "no legal gear combination"

    full_text = build_profile(header, base_full, profilesets, iterations=combo_iters)
    signature = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
    return {
        "header": header,
        "candidates": candidates,
        "active_slots": active_slots,
        "tier_set_id": tier_set_id,
        "base_full": base_full,
        "base_label": all_combos[0][1],
        "profilesets": profilesets,          # [(name, [(slot, cand), ...]), ...]
        "index": index,                      # name -> (full_set, config_label)
        "scenarios": scenarios,
        "combo_iters": combo_iters,
        "n_combos": len(all_combos),
        "signature": signature,
    }, None


def _assemble_result(spec_id, season, build, means, baseline_dps, simc_version):
    """Turn profileset DPS means into the final per-slot-ranked result dict.

    `means` is name->mean_dps for every profileset (from one monolithic run, or
    reassembled from checkpoint chunks — the source doesn't matter). Returns
    (result_dict, None) or (None, error_str)."""
    base_full = build["base_full"]
    base_label = build["base_label"]
    index = build["index"]
    active_slots = build["active_slots"]
    candidates = build["candidates"]
    tier_set_id = build["tier_set_id"]

    # Reassemble every simmed combo as (full set, dps, config_label).
    combo_results = [(base_full, baseline_dps, base_label)]
    for name, dps in means.items():
        entry = index.get(name)
        if entry is None:
            continue   # name not in the current build (defensive; signature guards this)
        full, label = entry
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


async def simulate_prepared(spec_id, season, prep, item_lookup, stats=None):
    """Run a whole spec in one simc invocation (no checkpointing).

    Used by the debug CLI. The production collector uses the chunked, resumable
    path in run_simc_bis instead. Touches no DB; all reads happen in
    _prepare_spec.
    """
    build, build_err = _build_run(prep, item_lookup)
    if build is None:
        msg = f"spec {spec_id} produced no valid gear combinations: {build_err}"
        _stat_log(stats, f"simc: {msg}")
        return None, msg

    _stat_log(stats, f"simc: spec {spec_id} evaluating {build['n_combos']} full-set combos "
                     f"across {len(build['scenarios'])} combo group(s)")
    profile_text = build_profile(build["header"], build["base_full"], build["profilesets"],
                                 iterations=build["combo_iters"])
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
    return _assemble_result(spec_id, season, build, means, baseline_dps, simc_version)


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


def _write_failure_meta(spec_id, season):
    """Record a failed/timed-out attempt (empty BiS meta with a fresh timestamp)
    so pick_next_spec's round-robin doesn't immediately re-pick the broken spec.

    Uses its own short-lived, validated connection: the sim that just failed may
    have run for hours, so any connection checked out before it would be dead by
    now (that was the original bug). Best-effort — a failure here must not crash
    the collector loop."""
    from contextlib import closing
    try:
        with closing(databaseConnector.get_live_connection()) as conn:
            cursor = conn.cursor()
            databaseConnector.configure_read_session(conn, cursor)
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            if not conn.in_transaction:
                conn.start_transaction()
            databaseConnector.delete_simc_bis(conn, cursor, spec_id, season)
            databaseConnector.insert_simc_bis_meta(
                conn, cursor, spec_id, season, updated_at=now
            )
            databaseConnector.commit_with_retry(conn)
    except Exception as e:
        _log(f"simc: could not write failure meta for spec {spec_id}: {e}")


# --------------------------------------------------------------------------
# Checkpoint / resume (chunked profileset runs)
#
# A spec's profilesets are simmed in chunks of SIMC_CHUNK_SIZE across successive
# collector visits, each chunk checkpointed to simc_bis_progress[_meta] so a
# heavy spec survives the container's ~daily restart instead of restarting from
# zero. Each helper below opens its own short-lived, validated connection so no
# connection is ever held across the (long) sim — see [[dont-hold-pooled-conn]].
# --------------------------------------------------------------------------

def _persist_progress_chunk(spec_id, season, signature, total, reset, snapshot,
                            chunk_means, baseline_dps, simc_version, now):
    """Write one chunk's profileset means (upsert) and refresh the run header.

    `reset` drops any stale checkpoint (signature changed) first. `snapshot` is
    the JSON prep snapshot; COALESCE in the upsert keeps the stored one when None
    is passed. A successful chunk clears the failed flag. baseline_dps /
    simc_version are captured from whichever chunk ran (every chunk sims the base
    actor). Its own transaction, own connection."""
    from contextlib import closing
    with closing(databaseConnector.get_live_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        if not conn.in_transaction:
            conn.start_transaction()
        if reset:
            databaseConnector.delete_simc_progress(conn, cursor, spec_id, season)
        databaseConnector.upsert_simc_progress_meta(
            conn, cursor, spec_id, season, signature, total,
            baseline_dps, simc_version, started_at=now, last_attempt_at=now,
            failed=False, prep_snapshot=snapshot,
        )
        rows = [(spec_id, season, name, float(dps), now) for name, dps in chunk_means.items()]
        databaseConnector.insert_simc_progress_rows(conn, cursor, rows)
        databaseConnector.commit_with_retry(conn)


def _touch_progress_attempt(spec_id, season, signature, total, reset, snapshot, now):
    """Mark a FAILED attempt on a run without recording results (chunk sim
    errored/timed out): sets the failed flag and bumps last_attempt_at so
    _select_target_spec sends this spec to the back of the queue instead of
    immediately retrying the same failing chunk. Stores the snapshot too (a
    first-chunk failure must still leave a resumable header). Preserves any
    existing baseline/version (COALESCE in the upsert)."""
    from contextlib import closing
    try:
        with closing(databaseConnector.get_live_connection()) as conn:
            cursor = conn.cursor()
            databaseConnector.configure_read_session(conn, cursor)
            if not conn.in_transaction:
                conn.start_transaction()
            if reset:
                databaseConnector.delete_simc_progress(conn, cursor, spec_id, season)
            databaseConnector.upsert_simc_progress_meta(
                conn, cursor, spec_id, season, signature, total,
                None, None, started_at=now, last_attempt_at=now,
                failed=True, prep_snapshot=snapshot,
            )
            databaseConnector.commit_with_retry(conn)
    except Exception as e:
        _log(f"simc: could not touch progress attempt for spec {spec_id}: {e}")


def _finalize_run(spec_id, season, build, all_means, baseline_dps, simc_version, item_lookup):
    """Assemble the final BiS from every chunk's means, persist it, and clear the
    checkpoint. Returns (True, result) or (False, error_str). The persist and the
    checkpoint-clear run on one connection; if the process dies between them the
    leftover progress rows just re-finalise (idempotently) on the next visit."""
    from contextlib import closing
    result, err = _assemble_result(spec_id, season, build, all_means, baseline_dps, simc_version)
    if not result:
        return False, err
    with closing(databaseConnector.get_live_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        persist(conn, cursor, result, item_lookup)   # writes simc_bis_meta + items (own txn)
        # Clear the checkpoint in its own committed transaction. execute_with_retry
        # doesn't commit on its own, and persist() left autocommit ambiguous, so be
        # explicit — otherwise closing the pooled connection rolls the delete back.
        if not conn.in_transaction:
            conn.start_transaction()
        databaseConnector.delete_simc_progress(conn, cursor, spec_id, season)
        databaseConnector.commit_with_retry(conn)
    return True, result


def _clear_progress(spec_id, season):
    """Best-effort drop of a spec's checkpoint (e.g. after an unassemblable run)."""
    from contextlib import closing
    try:
        with closing(databaseConnector.get_live_connection()) as conn:
            cursor = conn.cursor()
            databaseConnector.configure_read_session(conn, cursor)
            databaseConnector.delete_simc_progress(conn, cursor, spec_id, season)
    except Exception as e:
        _log(f"simc: could not clear progress for spec {spec_id}: {e}")


def _select_target_spec(conn, cursor, specs, season):
    """Pick the spec to work on next (oldest queue position first).

    Queue position per spec:
      * unfailed in-progress run -> its started_at. The run began when the spec
        was the stalest of all, so that old timestamp keeps it at the front —
        a run interrupted by the ~daily container restart is resumed
        immediately instead of waiting a full rotation.
      * failed in-progress run   -> its last_attempt_at. A genuinely failing
        chunk sends the spec to the back of the queue so it can't monopolise
        the loop; its banked chunks are retried when it comes around again.
      * no run in progress       -> its completed simc_bis_meta.updated_at.
    None (never run) sorts first."""
    activity = databaseConnector.fetch_simc_progress_activity(conn, cursor, season)
    best = None
    for spec_id, info in simulated_specs(specs):
        prog = activity.get(int(spec_id))
        if prog is not None:
            ts = prog["last_attempt_at"] if prog["failed"] else prog["started_at"]
        else:
            try:
                ts = databaseConnector.fetch_simc_bis_updated_at(conn, cursor, spec_id, season)
            except Exception:
                ts = None
        key = (ts is not None, ts or datetime.min)
        if best is None or key < best[0]:
            best = (key, spec_id, info)
    if best is None:
        return None
    return best[1], best[2]


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

async def run_simc_bis(session, cancel_event=None, stats=None, get_season=None,
                       reporter=None, pause_check=None, write_gate=None):
    """Continuously simulate per-slot BiS, one spec at a time, round-robin.

    `get_season(conn, cursor)` -> int season id. If omitted, falls back to the
    SIMC_SEASON env var. `session` is accepted for signature parity with the
    other collector tasks (not used directly). `reporter` is the DiscordReporter
    used to surface error conditions (instead of failing silently). `pause_check`
    is an optional awaitable called between specs; it blocks while a season-rollover
    wipe is pending so this task doesn't write into tables about to be cleared.

    `write_gate` is the collector's WriteGate. `pause_check` alone is not enough:
    it is uncounted, so between two specs the gate could report quiesced while a
    simc_bis_* write is still landing and ev_season_wipe truncates underneath it.
    Every write below is therefore held inside a counted begin()/end() section —
    only the writes, never a sim, which can run for hours.
    """
    from contextlib import asynccontextmanager, closing

    @asynccontextmanager
    async def _gated():
        if write_gate is None:
            yield
            return
        await write_gate.begin()
        try:
            yield
        finally:
            write_gate.end()

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
        if pause_check is not None:
            await pause_check()  # hold between specs while a season wipe runs
        # refresh the simc image periodically
        if (asyncio.get_event_loop().time() - last_pull) > SIMC_PULL_INTERVAL:
            await pull_simc_image(stats)
            last_pull = asyncio.get_event_loop().time()
        try:
            # --- Read phase: short-lived connection, released BEFORE the sims ---
            # A spec is simmed in chunks of SIMC_CHUNK_SIZE profilesets, run
            # BACK-TO-BACK until the spec completes (checkpointing each chunk), so
            # a heavy spec finishes in ~one continuous stretch, survives the
            # ~daily container restart losing at most one chunk, and never blows
            # the per-chunk timeout. An interrupted run resumes from its stored
            # prep snapshot, NOT from re-prepared data: the candidate bags come
            # from nightly-rebuilt popularity aggregations, so re-preparing after
            # a restart would usually change the profile, mismatch the signature,
            # and throw all banked chunks away. get_live_connection()
            # pings/reconnects on checkout, reviving stale pooled connections.
            prep = prep_err = None
            build = build_err = None
            snapshot = None
            spec_id = info = class_info = None
            season = None
            done = {}
            reset_progress = False
            stored_baseline = None
            stored_version = None
            with closing(databaseConnector.get_live_connection()) as conn:
                cursor = conn.cursor()
                # autocommit read phase (see configure_read_session); writers open
                # their own explicit transactions so their writes stay atomic.
                databaseConnector.configure_read_session(conn, cursor)
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

                # Pre-season gap: the season has been resolved (Blizzard flipped
                # it) but no runs/gear have been collected + aggregated yet, so
                # every spec would fail gather_candidates with "no candidate
                # items". Emit a single "no data yet" alert instead of one failure
                # per spec, and idle until data appears. The one-shot re-arms on a
                # collector restart (in-memory) and on recovery below.
                no_data_key = f"simc_no_season_data_{season}"
                if not databaseConnector.simc_season_has_gear_data(conn, cursor, season):
                    await _oneshot_alert(
                        reporter, stats, no_data_key,
                        f"SimC: no gear data for season {season} yet",
                        f"Season {season} has no aggregated gear data yet (pre-season "
                        "gap). Pausing BiS simulations until data is collected. This "
                        "alert repeats only on a collector restart.",
                        level="warning",
                    )
                    await asyncio.sleep(SIMC_SPEC_SLEEP)
                    continue
                # Season is ready: re-arm the one-shot so a later empty season (same
                # process) alerts again.
                if reporter is not None:
                    reporter.clear_oneshot_alert(no_data_key)

                picked = _select_target_spec(conn, cursor, specs, season)
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

                # Resume path: rebuild the run from the checkpoint's snapshot. The
                # signature then only mismatches when the CODE that generates
                # profiles changed (a legitimate reset), not when popularity data
                # drifted overnight.
                pmeta = databaseConnector.fetch_simc_progress_meta(conn, cursor, spec_id, season)
                if pmeta is not None:
                    snap_prep = _load_prep_snapshot(pmeta.get("prep_snapshot"))
                    snap_build = _build_run(snap_prep, item_lookup)[0] if snap_prep else None
                    if snap_build is not None and snap_build["signature"] == pmeta.get("run_signature"):
                        build = snap_build
                        snapshot = pmeta.get("prep_snapshot")
                        done = databaseConnector.fetch_simc_progress_means(conn, cursor, spec_id, season)
                        stored_baseline = pmeta.get("baseline_dps")
                        stored_version = pmeta.get("simc_version")
                        _stat_log(stats, f"simc: spec {spec_id} resuming from checkpoint "
                                         f"({len(done)}/{pmeta.get('total_profilesets')} profilesets banked)")
                    else:
                        # Unusable checkpoint (corrupt snapshot or profile-gen code
                        # changed): start a fresh run, dropping the old rows on the
                        # first write below.
                        reset_progress = True

                if build is None:
                    prep, prep_err = _prepare_spec(
                        spec_id, info, class_info, season, conn, cursor, item_lookup, stats
                    )
                    if prep:
                        build, build_err = _build_run(prep, item_lookup)
                        if build is not None:
                            snapshot = _snapshot_prep(prep)
            # connection released here — the sims below hold no DB connection

            spec_label = f"{class_info.get('name')}/{info.get('name')}" if class_info else str(spec_id)

            # --- Prep / build failures: mark an attempt and move on ---
            if build is None:
                msg = prep_err or build_err or f"spec {spec_id} produced no valid gear combinations"
                await _alert(
                    reporter, stats, "SimC: spec simulation failed",
                    f"No result for spec {spec_id} ({spec_label}).\n```\n{(msg or 'unknown error')[-1000:]}\n```",
                    level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                )
                async with _gated():
                    _write_failure_meta(spec_id, season)
                    if reset_progress:
                        _clear_progress(spec_id, season)
                await asyncio.sleep(SIMC_SPEC_SLEEP)
                continue

            total = len(build["profilesets"])

            # --- Sim phase: back-to-back chunks, no DB connection held ---
            while not _cancelled():
                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                remaining = [ps for ps in build["profilesets"] if ps[0] not in done]

                if not remaining:
                    # All profilesets banked (or the spec has none). If a previous
                    # chunk already measured the base actor, finalize directly;
                    # otherwise (0-profileset spec, or a crash landed exactly
                    # between the last bank and finalize with no stored baseline)
                    # run one base-only sim for the reference DPS.
                    baseline_dps, simc_version = stored_baseline, stored_version
                    if baseline_dps is None:
                        profile_text = build_profile(build["header"], build["base_full"], [],
                                                     iterations=build["combo_iters"])
                        result_json, run_err = await run_simc(profile_text, f"spec{spec_id}_base")
                        if not result_json:
                            if not _cancelled():
                                await _alert(
                                    reporter, stats, "SimC: spec simulation failed",
                                    f"Base sim failed for spec {spec_id} ({spec_label}).\n"
                                    f"```\n{(run_err or 'unknown error')[-1000:]}\n```",
                                    level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                                )
                                async with _gated():
                                    _touch_progress_attempt(spec_id, season, build["signature"],
                                                            total, reset_progress, snapshot, now)
                            break
                        baseline_dps = parse_baseline_dps(result_json)
                        simc_version = parse_simc_version(result_json)
                    async with _gated():
                        ok, fin = _finalize_run(spec_id, season, build, done,
                                                baseline_dps, simc_version, item_lookup)
                    if ok:
                        if stats is not None:
                            try:
                                await stats.increment("simc_specs_completed")
                            except Exception:
                                pass
                        _stat_log(stats, f"simc: completed spec {spec_id} ({spec_label}) — "
                                         f"baseline {fin['baseline_dps']:.0f} dps, {total} profilesets")
                    else:
                        await _alert(
                            reporter, stats, "SimC: spec simulation failed",
                            f"No result for spec {spec_id} ({spec_label}).\n```\n{(fin or 'unknown error')[-1000:]}\n```",
                            level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                        )
                        async with _gated():
                            _write_failure_meta(spec_id, season)
                            _clear_progress(spec_id, season)
                    break

                chunk = remaining[:SIMC_CHUNK_SIZE]
                done_n = len(done)
                _stat_log(stats, f"simc: spec {spec_id} ({spec_label}) simming profilesets "
                                 f"{done_n + 1}-{done_n + len(chunk)}/{total}"
                                 + (" [restarting stale progress]" if reset_progress else ""))
                profile_text = build_profile(build["header"], build["base_full"], chunk,
                                             iterations=build["combo_iters"])
                token = f"spec{spec_id}_chunk{done_n // max(1, SIMC_CHUNK_SIZE)}"
                result_json, run_err = await run_simc(profile_text, token)
                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                if not result_json:
                    # Distinguish shutdown from genuine failure: on SIGTERM the
                    # entrypoint force-removes the sibling container, which
                    # surfaces here as a failed run. Leave the checkpoint
                    # untouched (failed=0, old started_at) so the run is resumed
                    # FIRST after the restart; only a real failure flags the spec
                    # and sends it to the back of the queue.
                    if _cancelled():
                        break
                    await _alert(
                        reporter, stats, "SimC: spec simulation failed",
                        f"Chunk failed for spec {spec_id} ({spec_label}).\n```\n{(run_err or 'unknown error')[-1000:]}\n```",
                        level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                    )
                    async with _gated():
                        _touch_progress_attempt(spec_id, season, build["signature"], total,
                                                reset_progress, snapshot, now)
                    break

                baseline_dps = parse_baseline_dps(result_json)
                simc_version = parse_simc_version(result_json)
                if simc_version and stats is not None:
                    try:
                        stats.set_status("simc_build", simc_version)
                    except Exception:
                        pass
                chunk_means = parse_profileset_means(result_json)
                if not chunk_means:
                    # simc "succeeded" but returned no profileset results — treat
                    # as a failure rather than looping on the same chunk forever.
                    await _alert(
                        reporter, stats, "SimC: spec simulation failed",
                        f"Chunk returned no profileset results for spec {spec_id} ({spec_label}).",
                        level="error", throttle_key=f"simc_spec_fail_{spec_id}",
                    )
                    async with _gated():
                        _touch_progress_attempt(spec_id, season, build["signature"], total,
                                                reset_progress, snapshot, now)
                    break
                if stats is not None:
                    try:
                        await stats.increment("simc_profilesets_run", len(chunk_means))
                    except Exception:
                        pass

                # Bank the chunk before anything else so a crash/restart from here
                # on can only lose work that was never persisted.
                async with _gated():
                    _persist_progress_chunk(spec_id, season, build["signature"], total,
                                            reset_progress, snapshot, chunk_means,
                                            baseline_dps, simc_version, now)
                reset_progress = False   # stale rows dropped on the first write
                done.update(chunk_means)
                stored_baseline, stored_version = baseline_dps, simc_version
                _stat_log(stats, f"simc: spec {spec_id} ({spec_label}) progress "
                                 f"{len(done)}/{total} profilesets")
                # Brief pause between chunks (keeps the loop responsive to
                # cancellation and lets other tasks breathe); the next loop pass
                # sims the following chunk or finalizes.
                await asyncio.sleep(1)
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
    injected_sets = prep.get("injected_sets") or []
    tier_comps = prep.get("tier_comps") or []

    # candidate count per slot (spot thin slots at a glance). The equip-limit
    # flags matter: a slot whose every candidate is embellished/limited can force
    # an illegal set (see legalize_set / trim_bag's protection rule).
    print("\n=== candidates per slot (after popularity filter) ===")
    print("     flags: E=embellished (E-free spends no budget)  U=unique-equipped"
          "  <cat>x<qty>=itemLimit consumed")
    for slot in active_slots:
        cs = candidates.get(slot, [])
        parts = []
        for c in cs:
            flags = []
            if c.get("has_embellishment"):
                flags.append("E" if c.get("embellish_limits") else "E-free")
            if c.get("unique_equipped"):
                flags.append("U")
            flags += [f"{cat}x{qty}" for cat, qty in candidate_limit_categories(c)]
            parts.append(f"{c['item_id']}(n={c['count']},sockets={c.get('socket_count', 0)}"
                         + (f",{'/'.join(flags)}" if flags else "") + ")")
        limit_free = sum(1 for c in cs if not _consumes_limit(c))
        print(f"  {slot:10} {len(cs):2} ({limit_free} limit-free): {', '.join(parts)}")

    base_violations = set_violations(baseline)
    print("\n=== baseline (Top-50 seed if available, else most-popular; after legalization) ===")
    print(f"  embellished slots: {sorted(s for s, c in baseline.items() if c.get('has_embellishment'))}"
          f" — of which budget-consuming: "
          f"{sorted(s for s, c in baseline.items() if c.get('embellish_limits'))}")
    print(f"  legal: {not base_violations}"
          + (f" — {_violation_reason(baseline, base_violations)}" if base_violations else ""))

    # Seed-vs-popular diff: where the reseeded baseline differs from the raw
    # most-popular item per slot (the pre-Top-50 baseline). A non-empty diff is the
    # whole point of the reseed — the clean current item replacing a stale popular
    # one.
    print("\n=== seed vs most-popular per slot (where the Top-50 reseed changed the base) ===")
    diffs = 0
    for slot in active_slots:
        cs = candidates.get(slot) or []
        if not cs:
            continue
        popular = cs[0]
        seed = baseline.get(slot)
        if seed and not _same_cand(seed, popular):
            diffs += 1
            print(f"  {slot:10} popular {popular['item_id']} (n={popular.get('count')}) "
                  f"-> seed {seed['item_id']} (n={seed.get('count')})")
    if not diffs:
        print("  (no difference — seed equals the most-popular set)")

    print("\n=== injected Top-50 player sets (whole coherent sets) ===")
    if injected_sets:
        for inj in injected_sets[:10]:
            g = inj["gear"]
            worn_desc = ", ".join(f"{s}={g[s]['item_id']}" for s in ALL_SLOTS if s in g)
            print(f"  {inj['label']:14} {len(g)} slots: {worn_desc}")
        if len(injected_sets) > 10:
            print(f"  ... and {len(injected_sets) - 10} more")
    else:
        print("  (none — thin Top-50 data or no verified loadouts)")

    print("\n=== resolved tier-set combos (each real 2pc/3pc/4pc config, "
          "ordered by popularity) ===")
    if tier_comps:
        for tc in tier_comps:
            worn = tc.get("locked_slots") or []
            print(f"  {tc['label']}  runs={tc.get('total_runs')}  "
                  f"pieces={[tc['gear'][s]['item_id'] for s in worn if s in tc['gear']]}")
    else:
        print("  (none — no aggregated tier-set comps; falls back to detect_tier scenarios)")

    print("\n=== enchants (group -> enchant_id, constant across profilesets) ===")
    for grp, eid in sorted((prep.get("enchant_map") or {}).items()):
        print(f"  {grp:10} {eid}")
    print("=== gem ranking (most popular first, fills sockets top-down) ===")
    print(f"  {prep.get('gem_ranking') or []}")

    SIMC_IO_DIR.mkdir(parents=True, exist_ok=True)
    written = []

    # full-set Top-Gear combination profile, exactly as the real run builds it
    # (seed base + injected player sets + tier combos + per-slot enumeration).
    base_full, ps, index, all_combos, scenarios, reason = build_combinations(
        candidates, baseline, active_slots, tier_set_id, tier_slots,
        item_lookup, SIMC_MAX_COMBINATIONS, injected_sets, tier_comps,
    )
    try:
        combo_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        combo_iters = None

    # Which slots the search actually varies, read back off the combos: a slot the
    # trim collapsed is pinned in every set, so a bad pin there can never be
    # escaped (the failure mode this diagnostic exists for).
    print("\n=== forced vs varying slots (across every enumerated combo) ===")
    if all_combos:
        for slot in active_slots:
            ids = {full[slot]["item_id"] for full, _ in all_combos if full.get(slot)}
            kind = "VARY  " if len(ids) > 1 else "forced"
            print(f"  {kind} {slot:10} {sorted(ids)}")
        illegal = [i for i, (full, _) in enumerate(all_combos) if not set_is_valid(full)]
        print(f"  illegal combos returned: {len(illegal)} (must be 0)")
        assert not illegal, (
            f"{len(illegal)} illegal combo(s) returned by build_combinations "
            f"(e.g. index {illegal[0]}: "
            f"{_violation_reason(all_combos[illegal[0]][0], set_violations(all_combos[illegal[0]][0]))})"
        )
    else:
        print(f"  NO VALID COMBINATIONS — {reason}")

    txt = build_profile(header, base_full or baseline, ps, iterations=combo_iters)
    p = SIMC_IO_DIR / f"dryrun_spec{spec_id}_topgear.simc"
    p.write_text(txt, encoding="utf-8")
    written.append(p)
    from collections import Counter
    by_scen = Counter(label for _, label in all_combos)
    print(f"\n=== TOP-GEAR COMBO PROFILE ({p}) — {len(all_combos)} valid combos, "
          f"{len(ps)} profilesets, combo labels {dict(by_scen)} ===\n{txt}")

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
