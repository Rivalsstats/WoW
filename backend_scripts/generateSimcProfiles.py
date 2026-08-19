"""Build the SimulationCraft input profiles for the CI-driven DPS tierlist.

Emits one ``.simc`` file per gear set, each containing one actor for every
simulated spec (DPS + tanks; healers are never simmed). The GitHub Actions
matrix then runs each file once per target count (1/3/5/8) — passing
``desired_targets=N`` on the simc CLI — so a single simc invocation sims every
spec in that gear set for that target count under ``single_actor_batch=1``.

Two gear sets, both reusing the collector's existing gear/enchant/gem/talent
pipeline (simcBis.py) so the CI profiles never drift from production:

  * ``popular``  — the most-popular items/enchants/gems + most-popular talent
    loadout, i.e. exactly the baseline set shown on each spec page
    (simcBis._prepare_spec).
  * ``simcbis``  — the rank-1 per-slot items the collector's Top-Gear sweep
    persisted to ``simc_bis_items`` (with their bonus ids, enchants and gems),
    worn with the same popular talents the collector simmed them under.

Actors are named ``spec{spec_id}_{gearset}`` so the page generator can recover
the spec id and gear set from the json2 result alone. Anything that can't be
built for a spec (no popularity data, no persisted simc BiS rows, …) is skipped
and recorded in ``manifest.json`` rather than aborting the whole file.
"""

import os
import sys
import json
import argparse
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import databaseConnector
import simcBis
from simcBis import (
    ALL_SLOTS,
    DB_TO_SIMC_SLOT,
    DUAL_WIELD_TWOHAND_SPECS,
    RAID_BUFF_OVERRIDES,
    TWO_HAND_INVTYPES,
    build_header,
    gear_line,
    bonus_to_simc,
    class_token,
)

GEAR_SETS = ["popular", "simcbis"]


def tierlist_sim_options(target_error):
    """simc-wide options shared by every actor in a tierlist profile.

    ``desired_targets`` and ``max_time`` are placeholders — each matrix job
    overrides them on the simc CLI (desired_targets 1/3/5/8; max_time 180s for
    single-target, 60s for multi-target). No profileset / scale-factor options:
    these files sim a single fixed set per actor, not a Top-Gear sweep.
    """
    return [
        "threads=4",
        "single_actor_batch=1",
        f"target_error={target_error}",
        "max_time=300",
        "fight_style=Patchwerk",
        "desired_targets=5",
        "report_details=0",
        "optimize_expressions=1",
        *RAID_BUFF_OVERRIDES,
    ]


def _actor_block(header, active_slots, gear):
    """Header lines + one gear line per equipped slot for a single actor."""
    lines = list(header)
    lines.append("")
    for slot in active_slots:
        cand = gear.get(slot)
        if cand:
            lines.append(gear_line(slot, cand))
    return lines


def _simcbis_gear(bis_rows, item_lookup, spec_id, enchant_map, gem_ranking):
    """Turn fetch_simc_bis() output into slot -> gear-line candidate dicts.

    Picks the rank-1 item per slot and drops the off-hand when the main hand is
    a two-hander (except for Titan's Grip Fury, which wields a two-hander in
    both hands).

    Enchants/gems are hybrid: the collector's persisted values are used when
    present, otherwise the live popular pipeline (apply_enchants_and_gems)
    backfills them. The simc_bis_items enchant/gem columns were added after most
    specs were last collected, so without this a stale spec would sim with no
    enchants and no gems and lose to the (fully enchanted/gemmed) popular set.

    Returns (gear, active_slots) or (None, []) if nothing usable.
    """
    socket_bonus_counts = simcBis.load_bonus_socket_counts()
    gear = {}
    persisted = {}
    for slot, entries in bis_rows.items():
        if slot not in DB_TO_SIMC_SLOT or not entries:
            continue
        best = min(entries, key=lambda e: e.get("rank", 99))
        bonus_list = best.get("bonus_list")
        bonus_ids = [b.strip() for b in str(bonus_list).split(",") if b.strip()] if bonus_list else []
        # Socket count from the equipped bonus_ids, raised to the item's inherent
        # sockets — mirrors gather_candidates so gems land on the right slots.
        socket_count = sum(socket_bonus_counts.get(b, 0) for b in bonus_ids)
        inherent = len((item_lookup.get(best["item_id"], {}).get("socketInfo") or {}).get("sockets") or [])
        gem_ids = best.get("gem_ids")
        persisted[slot] = {
            "enchant_id": best.get("enchant_id"),
            "gem_ids": [g for g in str(gem_ids).split("/") if g] if gem_ids else None,
        }
        gear[slot] = {
            "item_id": best["item_id"],
            "simc_bonus": bonus_to_simc(bonus_list),
            "socket_count": max(socket_count, inherent),
        }
    if not gear:
        return None, []

    mh = gear.get("MAIN_HAND")
    if (mh and spec_id not in DUAL_WIELD_TWOHAND_SPECS
            and item_lookup.get(mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES):
        gear.pop("OFF_HAND", None)

    # Fill live enchants/gems over the equipped set (correct per-category gem
    # budget), then prefer the collector's persisted values wherever it has them.
    live_cands = {slot: [cand] for slot, cand in gear.items()}
    simcBis.apply_enchants_and_gems(live_cands, enchant_map, gem_ranking, item_lookup)
    for slot, cand in gear.items():
        p = persisted[slot]
        if p["enchant_id"] is not None:
            cand["enchant_id"] = p["enchant_id"]
        if p["gem_ids"]:
            cand["gem_ids"] = p["gem_ids"]

    active_slots = [s for s in ALL_SLOTS if s in gear]
    return gear, active_slots


def build_profiles(season, target_error, only_specs=None):
    """Return ({gearset: [actor_block, ...]}, manifest_specs)."""
    specs, classes = simcBis.load_static()
    item_lookup = simcBis.load_item_lookup()

    actors = {gs: [] for gs in GEAR_SETS}
    manifest_specs = {}

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        for spec_id, info in simcBis.simulated_specs(specs):
            if only_specs and spec_id not in only_specs:
                continue
            class_info = classes.get(str(info.get("classID")), {})
            built, skipped = [], {}

            prep, prep_err = simcBis._prepare_spec(
                spec_id, info, class_info, season, conn, cursor, item_lookup
            )
            if not prep:
                # Without the popular pipeline we have neither a talent loadout
                # nor a class header, so both gear sets are unbuildable.
                reason = prep_err or "prepare failed"
                manifest_specs[spec_id] = {"actors": [], "skipped": {gs: reason for gs in GEAR_SETS}}
                simcBis._log(f"tierlist: spec {spec_id} skipped entirely: {reason}")
                continue

            class_name = class_info.get("name")
            spec_name = info.get("name")
            primary = info.get("primary_stat")
            talents = prep.get("talents_code")

            # popular: the spec-page baseline set, verbatim.
            pop_header = build_header(
                class_name, spec_name, primary, talents,
                actor_name=f"spec{spec_id}_popular",
            )
            actors["popular"].append(_actor_block(pop_header, prep["active_slots"], prep["baseline"]))
            built.append("popular")

            # simcbis: the collector's persisted rank-1 per-slot set.
            bis_gear = None
            try:
                bis_rows = databaseConnector.fetch_simc_bis(conn, cursor, spec_id, season)
            except Exception as e:
                bis_rows = None
                skipped["simcbis"] = f"fetch_simc_bis failed: {e}"
            if bis_rows:
                bis_gear, bis_slots = _simcbis_gear(
                    bis_rows, item_lookup, spec_id,
                    prep["enchant_map"], prep["gem_ranking"],
                )
            if bis_gear:
                bis_header = build_header(
                    class_name, spec_name, primary, talents,
                    actor_name=f"spec{spec_id}_simcbis",
                )
                actors["simcbis"].append(_actor_block(bis_header, bis_slots, bis_gear))
                built.append("simcbis")
            elif "simcbis" not in skipped:
                skipped["simcbis"] = "no simc_bis rows"

            manifest_specs[spec_id] = {"actors": built, "skipped": skipped}
            simcBis._log(f"tierlist: spec {spec_id} ({class_name}/{spec_name}) built={built} skipped={list(skipped)}")

    return actors, manifest_specs


def main(output_dir, season, target_error, only_specs):
    simcBis._init_pool_from_env()
    actors, manifest_specs = build_profiles(season, target_error, only_specs)

    os.makedirs(output_dir, exist_ok=True)
    for gs in GEAR_SETS:
        lines = tierlist_sim_options(target_error)
        lines.append("")
        for block in actors[gs]:
            lines.extend(block)
            lines.append("")
        out_path = os.path.join(output_dir, f"gearset_{gs}.simc")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print(f"Wrote {out_path} with {len(actors[gs])} actor(s)")

    manifest = {
        "season": season,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_error": target_error,
        "gear_sets": GEAR_SETS,
        "specs": manifest_specs,
    }
    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {manifest_path} covering {len(manifest_specs)} spec(s)")

    if not any(actors.values()):
        print("ERROR: no actors built for any gear set", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SimulationCraft tierlist profiles")
    parser.add_argument("--output_dir", required=True, help="Directory to write .simc profiles + manifest")
    parser.add_argument("--season", type=int, default=None, help="Season id (default: seasonInfo.blizzard_season_id)")
    parser.add_argument("--target_error", default="0.2", help="simc target_error for every actor (default 0.2)")
    parser.add_argument("--specs", default=None, help="Comma-separated spec ids to limit to (local testing)")
    args = parser.parse_args()

    season = args.season
    if season is None:
        # MYTHISTONE_SEASON_INFO lets a final snapshot of the outgoing season be
        # built after seasonInfo.json has flipped (see commonUtils.load_season_info).
        season_info_path = os.environ.get("MYTHISTONE_SEASON_INFO", "").strip() or str(
            simcBis.STATIC_DIR / "seasonInfo.json"
        )
        season_info = json.loads(Path(season_info_path).read_text(encoding="utf-8"))
        season = season_info.get("blizzard_season_id")
    if not season:
        print("ERROR: no season id (pass --season or set blizzard_season_id in seasonInfo.json)", file=sys.stderr)
        sys.exit(2)

    only_specs = None
    if args.specs:
        only_specs = {int(s) for s in args.specs.split(",") if s.strip()}

    main(args.output_dir, int(season), args.target_error, only_specs)
