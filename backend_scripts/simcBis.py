"""SimulationCraft "best item per slot" collector.

Runs continuously inside the collector container (registered alongside
``run_raiderio_top_loadouts``). For each DPS / Tank spec it:

  1. Builds a baseline ``.simc`` profile from our most-popular loadout
     (most-common item per slot + most-common talent loadout). This baseline
     already wears the current meta tier set.
  2. Detects the tier slots dynamically via Blizzard ``itemSetId``.
  3. Runs a small tier-scenario sweep to decide which slots wear the set.
  4. Iteratively greedy-optimizes every slot by profileset-swapping the
     top-10 candidate items (sourced from our own leaderboard data) one at a
     time, ranking each slot's candidates by simulated DPS.
  5. Persists the per-slot ranked results to ``simc_bis_meta`` /
     ``simc_bis_items`` for the page build to render a "SIM" badge.

SimulationCraft itself is executed as a short-lived sibling Docker container
(``docker run --rm``) over a shared volume, so watchtower keeps simc patch-current.
Set ``SIMC_BIN`` to run a local binary instead (used for local debugging).

Profilesets are the core mechanism: one baseline is simulated, then each
variation overrides one (or, for tier scenarios, several) gear slot(s) and is
evaluated in isolation. One simc invocation evaluates a whole pass of
variations and emits JSON (``json2``) with ``sim.profilesets.results[]``.
"""

import os
import json
import asyncio
import argparse
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
SIMC_CPUS = os.environ.get("SIMC_CPUS")  # optional docker --cpus cap
SIMC_PROFILESET_WORK_THREADS = os.environ.get("SIMC_PROFILESET_WORK_THREADS", "1")
SIMC_ITERATIONS = os.environ.get("SIMC_ITERATIONS")  # e.g. "5000"; if unset, use target_error
SIMC_TARGET_ERROR = os.environ.get("SIMC_TARGET_ERROR", "0.1")
SIMC_RUN_TIMEOUT = int(os.environ.get("SIMC_RUN_TIMEOUT", str(60 * 60)))  # seconds per invocation
SIMC_MAX_PASSES = int(os.environ.get("SIMC_MAX_PASSES", "3"))
# Minimum relative DPS gain for a single-slot swap to be accepted into the next
# greedy baseline (guards against sim-noise-driven drift). 0.002 = 0.2%.
SIMC_IMPROVE_MARGIN = float(os.environ.get("SIMC_IMPROVE_MARGIN", "0.002"))
SIMC_CANDIDATES_PER_SLOT = int(os.environ.get("SIMC_CANDIDATES_PER_SLOT", "10"))
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
    """id -> item dict from equippable-items.json (has inventoryType, itemSetId)."""
    items = json.loads((STATIC_DIR / "equippable-items.json").read_text(encoding="utf-8"))
    return {int(i["id"]): i for i in items if i.get("id") is not None}


def bonus_to_simc(bonus_list):
    """DB bonus_list (comma string) -> simc bonus_id value (slash-separated)."""
    if not bonus_list:
        return None
    ids = [b.strip() for b in str(bonus_list).split(",") if b.strip()]
    return "/".join(ids) if ids else None


# --------------------------------------------------------------------------
# Candidate gathering & tier detection
# --------------------------------------------------------------------------

def gather_candidates(conn, cursor, spec_id, season, item_lookup):
    """slot -> ordered list of candidate dicts (most-popular first).

    Each candidate: {item_id, count, bonus_list, simc_bonus, item_set_id, inv_type}.

    Rare/stale items are dropped: the aggregated pool occasionally surfaces old
    expansions' items (e.g. a Legion ring) that get current-season bonus_ids
    applied and produce nonsense in simc. We keep only candidates whose equip
    count is at least SIMC_MIN_CANDIDATE_FRACTION of the slot's most-popular item
    (the top item always passes).
    """
    out = {}
    for slot in ALL_SLOTS:
        rows = databaseConnector.fetch_top_items_for_slot_with_bonus(
            conn, cursor, spec_id, season, slot
        )
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
            cands.append(
                {
                    "item_id": item_id,
                    "count": count,
                    "bonus_list": bonus_list,
                    "simc_bonus": bonus_to_simc(bonus_list),
                    "item_set_id": meta.get("itemSetId"),
                    "inv_type": meta.get("inventoryType"),
                }
            )
        if cands:
            out[slot] = cands
    return out


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


def rank_candidates(results, margin=None):
    """Rank (candidate, dps) pairs for a slot, highest DPS first.

    Candidates within `margin` of the top DPS are a statistical tie (sim error),
    so among those we surface the most-popular one as rank-1 for a stable badge
    instead of letting sim noise pick between near-identical items.
    """
    if not results:
        return []
    if margin is None:
        margin = SIMC_IMPROVE_MARGIN
    mx = max(d for _, d in results)
    threshold = mx * (1 - margin)
    tied = [r for r in results if r[1] >= threshold]
    rest = [r for r in results if r[1] < threshold]
    tied.sort(key=lambda r: (-(r[0].get("count", 0) or 0), -r[1]))
    rest.sort(key=lambda r: -r[1])
    return tied + rest


def best_tier_candidate(candidates, slot, tier_set_id):
    for cand in candidates.get(slot, []):
        if cand.get("item_set_id") == tier_set_id:
            return cand
    return None


def best_non_tier_candidate(candidates, slot, tier_set_id):
    for cand in candidates.get(slot, []):
        if cand.get("item_set_id") != tier_set_id:
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


def sim_options():
    opts = [
        f"threads={SIMC_THREADS}",
        f"profileset_work_threads={SIMC_PROFILESET_WORK_THREADS}",
        "profileset_metric=dps",
        "single_actor_batch=1",
    ]
    if SIMC_ITERATIONS:
        opts.append(f"iterations={SIMC_ITERATIONS}")
    else:
        opts.append(f"target_error={SIMC_TARGET_ERROR}")
    return opts


def build_profile(header, baseline_gear, profilesets):
    """Assemble the full .simc text.

    baseline_gear: dict slot -> candidate (the current best-known set).
    profilesets: list of (name, [(slot, candidate), ...]) overrides.
    """
    out = []
    out.extend(sim_options())
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


def build_tier_sweep_profilesets(candidates, tier_set_id, tier_slots):
    """Profilesets for the tier-scenario sweep: 'all' (full set) plus one
    'drop:<slot>' per tier slot (4-set + best off-piece in the dropped slot).
    Returns (profilesets, index) where index maps name -> (dropped_slot, overrides)."""
    tier_slots = sorted(tier_slots)
    profilesets = []
    index = {}

    all_overrides = [(s, best_tier_candidate(candidates, s, tier_set_id))
                     for s in tier_slots if best_tier_candidate(candidates, s, tier_set_id)]
    if all_overrides:
        profilesets.append(("tall", all_overrides))
        index["tall"] = ("all", all_overrides)

    if len(tier_slots) >= 5:
        for i, drop in enumerate(tier_slots):
            overrides = []
            for slot in tier_slots:
                cand = (best_non_tier_candidate(candidates, slot, tier_set_id) if slot == drop
                        else best_tier_candidate(candidates, slot, tier_set_id))
                if cand:
                    overrides.append((slot, cand))
            name = f"td{i}"
            profilesets.append((name, overrides))
            index[name] = (drop, overrides)
    return profilesets, index


def build_greedy_profilesets(baseline, candidates, greedy_slots):
    """One single-slot-override profileset per candidate that differs from the
    baseline, across the given slots. Returns (profilesets, index) where index
    maps name -> (slot, candidate)."""
    profilesets = []
    index = {}
    n = 0
    for slot in greedy_slots:
        base_cand = baseline.get(slot)
        for cand in candidates.get(slot, []):
            if base_cand and cand["item_id"] == base_cand["item_id"] and cand["bonus_list"] == base_cand["bonus_list"]:
                continue  # identical to baseline, no need to re-sim
            name = f"c{n}"
            profilesets.append((name, [(slot, cand)]))
            index[name] = (slot, cand)
            n += 1
    return profilesets, index


# --------------------------------------------------------------------------
# Running simc
# --------------------------------------------------------------------------

async def run_simc(profile_text, token):
    """Write the profile, run simc, return the parsed JSON dict (or None).

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
        ok = await _run_simc_local(token, in_path, out_path)
    else:
        ok = await _run_simc_docker(token)
    if not ok:
        return None
    if not out_path.exists():
        _log(f"simc produced no output for {token}")
        return None
    try:
        return json.loads(out_path.read_text(encoding="utf-8"))
    except Exception as e:
        _log(f"failed to parse simc json for {token}: {e}")
        return None


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
        _log(f"simc timed out after {SIMC_RUN_TIMEOUT}s for {token}")
        return False
    if proc.returncode != 0:
        tail = (stdout or b"").decode("utf-8", "replace")[-1500:]
        _log(f"simc exited {proc.returncode} for {token}:\n{tail}")
        return False
    return True


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


async def _run_simc_docker(token):
    """Run simc in a sibling container via the Docker SDK (blocking call run in a
    thread so the event loop is never blocked)."""
    def _run():
        import docker  # imported lazily so local/debug runs don't require the SDK
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
            "remove": True,
            "detach": False,
            "stdout": True,
            "stderr": True,
        }
        if SIMC_CPUS:
            try:
                kwargs["nano_cpus"] = int(float(SIMC_CPUS) * 1e9)
            except Exception:
                pass
        return client.containers.run(**kwargs)

    _log(f"running simc container {SIMC_DOCKER_IMAGE} for {token}")
    try:
        logs = await asyncio.wait_for(asyncio.to_thread(_run), timeout=SIMC_RUN_TIMEOUT)
        return True
    except asyncio.TimeoutError:
        _log(f"simc container timed out after {SIMC_RUN_TIMEOUT}s for {token}")
        return False
    except ModuleNotFoundError:
        _log("simc: the 'docker' Python SDK is not installed. Either `pip install docker` "
             "(with Docker running) or set SIMC_BIN=<path to simc.exe> for a local run.")
        return False
    except Exception as e:
        # docker.errors.ContainerError carries the simc stderr in str(e)
        _log(f"simc container failed for {token}: {str(e)[-1500:]}")
        return False


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

    Returns a dict with header, candidates, baseline, tier info and active_slots,
    or None if the spec can't be prepared. Shared by optimize_spec and --dry-run.
    """
    spec_name = spec_info.get("name")
    class_name = class_info.get("name")
    if not class_token(class_name):
        _stat_log(stats, f"simc: unknown class token for {class_name}, skipping spec {spec_id}")
        return None

    candidates = gather_candidates(conn, cursor, spec_id, season, item_lookup)
    if not candidates:
        _stat_log(stats, f"simc: no candidate items for spec {spec_id}, skipping")
        return None

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
    }


async def optimize_spec(spec_id, spec_info, class_info, season, conn, cursor,
                        item_lookup, stats=None):
    """Run the full optimisation for one spec. Returns a result dict or None."""
    prep = _prepare_spec(spec_id, spec_info, class_info, season, conn, cursor, item_lookup, stats)
    if not prep:
        return None
    header = prep["header"]
    candidates = prep["candidates"]
    baseline = prep["baseline"]
    tier_set_id = prep["tier_set_id"]
    tier_slots = prep["tier_slots"]
    active_slots = prep["active_slots"]

    # ---- Pass A: tier-scenario sweep ----
    tier_config = "none"
    if tier_set_id and len(tier_slots) >= 4:
        tier_config = await _tier_sweep(
            header, baseline, candidates, tier_set_id, tier_slots, stats
        )

    # Lock the tier slots that the sweep assigned to the set: keeping the 4pc
    # intact during greedy (swapping a tier slot to an off-piece would silently
    # break the bonus and tank DPS). Their BiS is simply the tier piece.
    locked_tier_slots = set()
    if tier_set_id:
        for s in tier_slots:
            bc = baseline.get(s)
            if bc and bc.get("item_set_id") == tier_set_id:
                locked_tier_slots.add(s)
    greedy_slots = [s for s in active_slots if s not in locked_tier_slots]

    # ---- Pass B: iterative greedy per slot, never accepting a regression ----
    simc_version = None
    iterations_used = 0
    best_dps = None
    best_baseline = None
    best_ranked = None

    for pass_n in range(SIMC_MAX_PASSES):
        profilesets, index = build_greedy_profilesets(baseline, candidates, greedy_slots)

        profile_text = build_profile(header, baseline, profilesets)
        result = await run_simc(profile_text, f"spec{spec_id}_p{pass_n}")
        if not result:
            break
        baseline_dps = parse_baseline_dps(result)
        if baseline_dps is None:
            break
        simc_version = parse_simc_version(result) or simc_version
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
        iterations_used += 1

        # per-slot ranked results for this pass (baseline item included)
        slot_results = {slot: [(baseline[slot], baseline_dps)] for slot in greedy_slots if baseline.get(slot)}
        for name, dps in means.items():
            slot, cand = index[name]
            slot_results.setdefault(slot, []).append((cand, dps))
        per_slot_ranked = {
            slot: rank_candidates(res)
            for slot, res in slot_results.items()
        }
        # locked tier slots: their BiS is the equipped tier piece
        for slot in locked_tier_slots:
            if baseline.get(slot):
                per_slot_ranked[slot] = [(baseline[slot], baseline_dps)]

        # Regression guard: only keep a pass that did not lose DPS vs the best so far.
        if best_dps is None or baseline_dps >= best_dps:
            best_dps = baseline_dps
            best_baseline = dict(baseline)
            best_ranked = per_slot_ranked
        else:
            _stat_log(stats, f"simc: spec {spec_id} pass {pass_n} regressed ({baseline_dps:.0f} < {best_dps:.0f}); keeping best")
            break

        # Apply only single-slot winners that beat the baseline by the margin.
        changed = False
        for slot in greedy_slots:
            ranked = per_slot_ranked.get(slot) or []
            if not ranked or not baseline.get(slot):
                continue
            top_cand, top_dps = ranked[0]
            if top_cand["item_id"] != baseline[slot]["item_id"] and top_dps > baseline_dps * (1 + SIMC_IMPROVE_MARGIN):
                baseline[slot] = top_cand
                changed = True

        _stat_log(stats, f"simc: spec {spec_id} pass {pass_n} baseline_dps={baseline_dps:.0f} changed={changed}")
        if not changed:
            break

    if not best_ranked or best_dps is None:
        return None

    return {
        "spec_id": spec_id,
        "season": season,
        "baseline_dps": best_dps,
        "simc_version": simc_version,
        "tier_set_id": tier_set_id,
        "tier_config": tier_config,
        "per_slot_ranked": best_ranked,
        "passes": iterations_used,
    }


async def _tier_sweep(header, baseline, candidates, tier_set_id, tier_slots, stats):
    """Decide which tier slots wear the set. Mutates `baseline` in place to the
    winning tier configuration. Returns a short text label of the chosen config."""
    profilesets, index = build_tier_sweep_profilesets(candidates, tier_set_id, tier_slots)
    if not profilesets:
        return "none"

    profile_text = build_profile(header, baseline, profilesets)
    result = await run_simc(profile_text, f"tier_{tier_set_id}")
    if not result:
        return "all"
    means = parse_profileset_means(result)
    if stats is not None:
        try:
            await stats.increment("simc_profilesets_run", len(means))
        except Exception:
            pass
    if not means:
        return "all"

    best_name = max(means, key=lambda k: means[k])
    _, overrides = index[best_name]
    for slot, cand in overrides:
        baseline[slot] = cand
    label = "all" if best_name == "tall" else f"drop:{index[best_name][0]}"
    _stat_log(stats, f"simc: tier sweep chose {label} ({means.get(best_name)})")
    return label


# --------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------

def persist(conn, cursor, result, item_lookup):
    spec_id = result["spec_id"]
    season = result["season"]
    baseline_dps = result["baseline_dps"]
    tier_set_id = result.get("tier_set_id")

    item_rows = []
    for slot, ranked in result["per_slot_ranked"].items():
        for rank, (cand, dps) in enumerate(ranked, start=1):
            pct = ((dps - baseline_dps) / baseline_dps * 100.0) if baseline_dps else None
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

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        databaseConnector.delete_simc_bis(conn, cursor, spec_id, season)
        databaseConnector.insert_simc_bis_meta(
            conn, cursor, spec_id, season,
            simc_version=result.get("simc_version"),
            baseline_dps=baseline_dps,
            iterations=result.get("passes"),
            target_error=float(SIMC_TARGET_ERROR) if not SIMC_ITERATIONS else None,
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

                result = await optimize_spec(
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
                    await _alert(
                        reporter, stats, "SimC: spec simulation failed",
                        f"No result for spec {spec_id} "
                        f"({class_info.get('name')}/{info.get('name')}). Likely a "
                        f"simc run error or empty candidate pool — see collector logs.",
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
        prep = _prepare_spec(spec_id, info, class_info, season, conn, cursor, item_lookup)
    if not prep:
        _log("could not prepare spec (no candidates / unknown class)")
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

    # tier-sweep profile
    if tier_set_id and len(tier_slots) >= 4:
        ps, _ = build_tier_sweep_profilesets(candidates, tier_set_id, tier_slots)
        txt = build_profile(header, baseline, ps)
        p = SIMC_IO_DIR / f"dryrun_spec{spec_id}_tier.simc"
        p.write_text(txt, encoding="utf-8")
        written.append(p)
        print(f"\n=== TIER-SWEEP PROFILE ({p}) ===\n{txt}")

    # pass-0 greedy profile (locked tier slots excluded, as in the real run)
    locked = {s for s in tier_slots if baseline.get(s) and baseline[s].get("item_set_id") == tier_set_id}
    greedy_slots = [s for s in active_slots if s not in locked]
    ps, _ = build_greedy_profilesets(baseline, candidates, greedy_slots)
    txt = build_profile(header, baseline, ps)
    p = SIMC_IO_DIR / f"dryrun_spec{spec_id}_p0.simc"
    p.write_text(txt, encoding="utf-8")
    written.append(p)
    print(f"\n=== GREEDY PASS-0 PROFILE ({p}) — {len(ps)} profilesets, locked tier slots {sorted(locked)} ===\n{txt}")

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
        result = await optimize_spec(spec_id, info, class_info, season, conn, cursor, item_lookup)
        if result and do_persist:
            persist(conn, cursor, result, item_lookup)
            _log(f"persisted simc_bis rows for spec {spec_id} season {season}")
    if not result:
        _log("no result")
        return
    print(json.dumps({
        "spec_id": result["spec_id"],
        "baseline_dps": result["baseline_dps"],
        "simc_version": result["simc_version"],
        "tier_set_id": result["tier_set_id"],
        "tier_config": result["tier_config"],
        "passes": result["passes"],
        "bis_per_slot": {
            slot: {
                "item_id": ranked[0][0]["item_id"],
                "bonus_list": ranked[0][0]["bonus_list"],
                "dps": ranked[0][1],
                "dps_pct_gain": ((ranked[0][1] - result["baseline_dps"]) / result["baseline_dps"] * 100.0) if result["baseline_dps"] else None,
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
