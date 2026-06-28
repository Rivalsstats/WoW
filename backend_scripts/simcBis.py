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

SimulationCraft itself is executed as a short-lived sibling Docker container
(``docker run --rm``) over a shared volume, so watchtower keeps simc patch-current.
Set ``SIMC_BIN`` to run a local binary instead (used for local debugging).

Profilesets are the core mechanism: one baseline set is simulated, then each
combination overrides its (non-locked) gear slots and is evaluated in isolation.
One simc invocation evaluates every combination and emits JSON (``json2``) with
``sim.profilesets.results[]``.
"""

import os
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
SIMC_RUN_TIMEOUT = int(os.environ.get("SIMC_RUN_TIMEOUT", str(6 * 60 * 60)))  # seconds per invocation
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
# Fixed iteration count for the combination pass (Raidbots Top Gear uses 5000):
# a fixed count is generally faster than target_error for large profileset
# batches. Set to empty/0 to fall back to SIMC_ITERATIONS / SIMC_TARGET_ERROR.
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
                }
            )
        if cands:
            out[slot] = cands
    return out


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
    # NOTE(extension point): hold most-common gem/enchant constant here later.
    return ",".join(parts)


def build_header(class_name, spec_name, primary_stat, talents_code):
    token = class_token(class_name)
    race = DEFAULT_RACE.get(token, "orc")
    role = "spell" if (primary_stat or "").upper() == "INTELLECT" else "attack"
    lines = [
        f'{token}="mythistone_{spec_slug(spec_name)}"',
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


def sim_options(iterations=None):
    """simc-wide options. `iterations`, when given, pins a fixed iteration count
    for this run (used by the combination pass); otherwise we fall back to the
    SIMC_ITERATIONS env override or the default target_error."""
    opts = [
        f"threads={SIMC_THREADS}",
        f"profileset_work_threads={SIMC_PROFILESET_WORK_THREADS}",
        "profileset_metric=dps",
        "single_actor_batch=1",
    ]
    if iterations:
        opts.append(f"iterations={iterations}")
    elif SIMC_ITERATIONS:
        opts.append(f"iterations={SIMC_ITERATIONS}")
    else:
        opts.append(f"target_error={SIMC_TARGET_ERROR}")
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
    # handedness so a two-hander is never paired with an off-hand.
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
        msg = f"simc timed out after {SIMC_RUN_TIMEOUT}s for {token}"
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
        msg = f"simc container timed out after {SIMC_RUN_TIMEOUT}s for {token}"
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

    # drop off_hand if main hand is a two-hander / ranged weapon
    mh = baseline.get("MAIN_HAND")
    if mh and (item_lookup.get(mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES):
        baseline.pop("OFF_HAND", None)
    active_slots = [s for s in ALL_SLOTS if s in baseline]

    return {
        "header": header,
        "candidates": candidates,
        "baseline": baseline,
        "tier_set_id": tier_set_id,
        "tier_slots": tier_slots,
        "active_slots": active_slots,
        "talents_code": talents_code,
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
                )
            )

    # Effective simc accuracy used for the combination pass: a fixed iteration
    # count (combo / env override) or the default target_error.
    try:
        effective_iters = int(SIMC_COMBO_ITERATIONS) if SIMC_COMBO_ITERATIONS else None
    except ValueError:
        effective_iters = None
    if not effective_iters or effective_iters <= 0:
        effective_iters = int(SIMC_ITERATIONS) if SIMC_ITERATIONS else None
    effective_terr = None if effective_iters else float(SIMC_TARGET_ERROR)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
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
        if role in SIMULATED_ROLES:
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
        ids = ", ".join(f"{c['item_id']}(n={c['count']})" for c in cs)
        print(f"  {slot:10} {len(cs):2}: {ids}")

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
