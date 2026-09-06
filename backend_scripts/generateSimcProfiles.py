"""Build the SimulationCraft input profiles for the CI-driven DPS tierlist.

Emits one ``.simc`` file per gear set, each containing one actor for every
simulated spec (DPS + tanks; healers are never simmed). The GitHub Actions
matrix then runs each file once per target count (1/3/5/8) — passing
``desired_targets=N`` on the simc CLI — so a single simc invocation sims every
spec in that gear set for that target count under ``single_actor_batch=1``.

Three gear sets, all reusing the collector's existing gear/enchant/gem/talent
pipeline (simcBis.py) so the CI profiles never drift from production:

  * ``popular``  — the most-popular items/enchants/gems + most-popular talent
    loadout, i.e. exactly the baseline set shown on each spec page
    (simcBis._prepare_spec).
  * ``simcbis``  — the rank-1 per-slot items the collector's Top-Gear sweep
    persisted to ``simc_bis_items`` (with their bonus ids, enchants and gems),
    worn with the same popular talents the collector simmed them under.
  * ``top50``    — the top-50 verified players' actual loadout: per-slot
    most-common equipped item (with its bonus ids) from the ``top_player_loadouts``
    tables, worn with the top-50 players' most-common real Blizzard v2 export
    string (``top_player_loadouts.loadout_text``, surfaced by
    databaseConnector.fetch_top50_loadouts). The export string is the genuine
    in-game code the players used, captured verbatim from raider.io — NOT the
    synthetic ``loadout_key`` collector token (``logged-mplus__<id>``, not a talent
    code) and NOT a code reconstructed from the per-node rows. Real in-game strings
    are exactly what simc and the game accept, so there is no encoder, no
    choice-node bug and no committed-data-vs-simc skew. Enchants and gems are not
    stored per slot in the top-50 tables, so they are filled the same way
    popular/simcbis are (the top-50 enchant_map + gem_ranking from _prepare_spec,
    applied by socket budget).

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

import commonUtils
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

GEAR_SETS = ["popular", "simcbis", "top50"]


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


def _resolve_quality(base_quality, bonus_ids, bonus_quality):
    """Item rarity the way the spec page shows it: the catalog quality, overridden
    by any quality-carrying bonus id (truthy-only, last bonus wins) — the same rule
    as generateSpecPages.convert_slots / analyzer.js resolveQuality, so the modal's
    rim can never disagree with the spec page for the same item+bonus set."""
    q = base_quality
    for b in bonus_ids:
        bq = bonus_quality.get(b)
        if bq:
            q = bq
    return q


def _gear_display(active_slots, gear, talents_code, item_lookup, bonus_quality):
    """Compact display record for one (spec, gear set), consumed by the tierlist
    page's gear modal (assets/js/tierlist-modal.js).

    The item icon, name and rarity are resolved here (this job has item_lookup +
    the bonus->quality map), so the client never has to reach for the item-icon
    shards. Enchant / gem icons and links the client still resolves from the
    shared assets/json/gem_enchant_index.json catalog by id — the same one the
    analyzer uses — so this stays small and never drifts. The loadout string is
    Blizzard's export code (fetch_top_loadout), the same string the client decodes
    against assets/json/talent_trees/<spec>.json."""
    slots = {}
    for slot in active_slots:
        cand = gear.get(slot)
        if not cand:
            continue
        item_id = cand.get("item_id")
        info = item_lookup.get(item_id, {})
        bonus_ids = [b for b in str(cand.get("simc_bonus") or "").split("/") if b]
        entry = {
            "id": item_id,
            "name": info.get("name") or "",
            "icon": info.get("icon"),
            "quality": _resolve_quality(info.get("quality"), bonus_ids, bonus_quality),
            "bonus": bonus_ids,
        }
        if cand.get("enchant_id"):
            entry["enchant"] = int(cand["enchant_id"])
        if cand.get("gem_ids"):
            entry["gems"] = [int(g) for g in cand["gem_ids"]]
        slots[slot] = entry
    return {"talents": talents_code or "", "slots": slots}


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
        # Inherent + bonus sockets, via the one shared helper the spec page and
        # gather_candidates use, so gems land on the right slots.
        socket_count = commonUtils.count_item_sockets(
            bonus_ids,
            socket_bonus_counts,
            item_lookup.get(best["item_id"], {}).get("socketInfo"),
        )
        gem_ids = best.get("gem_ids")
        persisted[slot] = {
            "enchant_id": best.get("enchant_id"),
            "gem_ids": [g for g in str(gem_ids).split("/") if g] if gem_ids else None,
        }
        gear[slot] = {
            "item_id": best["item_id"],
            "simc_bonus": bonus_to_simc(bonus_list),
            "socket_count": socket_count,
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


def _top50_gear(loadouts, item_lookup, spec_id, enchant_map, gem_ranking):
    """Per-slot most-common item among the top-50 players' verified loadouts,
    shaped like _simcbis_gear so it flows through _actor_block / _gear_display.

    Items + their bonus ids come from top_player_loadout_items (via
    fetch_top50_loadouts); each top-50 player contributes one loadout per dungeon,
    so the per-slot vote is weighted the same way the spec page's top-50 stats are.
    Enchants and gems are NOT stored per slot in the top-50 tables (gems are a
    per-player {gem_item_id, usage_count} bag), so they are filled the same way
    popular/simcbis are: the top-50 enchant_map + gem_ranking already gathered in
    _prepare_spec, applied over the equipped set by socket budget
    (apply_enchants_and_gems). Returns (gear, active_slots) or (None, [])."""
    socket_bonus_counts = simcBis.load_bonus_socket_counts()
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
    for slot, counts in slot_item_counts.items():
        # Most common item; ties broken toward the higher count then the lower id
        # so the pick is deterministic across runs regardless of dict order.
        item_id = max(counts, key=lambda i: (counts[i], -i))
        bonus_counts = slot_item_bonus[slot][item_id]
        # Most common bonus set for that item (deterministic tie-break by string).
        bonus_str = max(bonus_counts, key=lambda b: (bonus_counts[b], b))
        bonus_ids = [b.strip() for b in str(bonus_str).split(",") if b.strip()]
        socket_count = commonUtils.count_item_sockets(
            bonus_ids,
            socket_bonus_counts,
            item_lookup.get(item_id, {}).get("socketInfo"),
        )
        gear[slot] = {
            "item_id": item_id,
            "simc_bonus": bonus_to_simc(bonus_str),
            "socket_count": socket_count,
        }
    if not gear:
        return None, []

    mh = gear.get("MAIN_HAND")
    if (mh and spec_id not in DUAL_WIELD_TWOHAND_SPECS
            and item_lookup.get(mh["item_id"], {}).get("inventoryType") in TWO_HAND_INVTYPES):
        gear.pop("OFF_HAND", None)

    # Fill enchants/gems over the equipped set (correct per-category gem budget),
    # exactly as _simcbis_gear does for the persisted BiS set.
    live_cands = {slot: [cand] for slot, cand in gear.items()}
    simcBis.apply_enchants_and_gems(live_cands, enchant_map, gem_ranking, item_lookup)

    active_slots = [s for s in ALL_SLOTS if s in gear]
    return gear, active_slots


def _top50_talents(loadouts):
    """Most-common real Blizzard v2 export string among the top-50 verified
    loadouts, returned verbatim.

    Each top-50 loadout carries the real in-game export string the collector
    captured from raider.io (``top_player_loadouts.loadout_text``, surfaced by
    fetch_top50_loadouts). Real in-game strings are exactly what simc and the game
    accept, so there is no encoder, no per-node choice-node reconstruction and no
    committed-data-vs-simc skew. The MOST COMMON non-empty string wins
    (deterministic tie-break by the string itself so the pick is stable across
    runs).

    Returns the export string (which decodes on the tierlist modal and inits in
    simc), or None when no top-50 loadout recorded a string (top50 then degrades
    gracefully until the collector repopulates the column)."""
    counts = {}
    for lo in loadouts or []:
        text = lo.get("loadout_text") if isinstance(lo, dict) else None
        if not isinstance(text, str):
            continue
        text = text.strip()
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return None
    return max(counts, key=lambda t: (counts[t], t))


def build_profiles(season, target_error, only_specs=None):
    """Return ({gearset: [actor_block, ...]}, manifest_specs, gear_data).

    gear_data is {spec_id: {gearset: {talents, slots}}} — the display records the
    tierlist page's gear modal reads (see _gear_display)."""
    specs, classes = simcBis.load_static()
    item_lookup = simcBis.load_item_lookup()
    # bonus id -> item quality, so the modal's rarity rim matches the spec page.
    # Committed static data (processBonusIds builds it), so no DB / secrets here.
    bonus_quality = json.loads(
        (simcBis.STATIC_DIR / "bonus_quality_map.json").read_text(encoding="utf-8")
    )

    actors = {gs: [] for gs in GEAR_SETS}
    manifest_specs = {}
    gear_data = {}

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
            gear_data.setdefault(spec_id, {})["popular"] = _gear_display(
                prep["active_slots"], prep["baseline"], talents, item_lookup, bonus_quality
            )
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
                gear_data.setdefault(spec_id, {})["simcbis"] = _gear_display(
                    bis_slots, bis_gear, talents, item_lookup, bonus_quality
                )
                built.append("simcbis")
            elif "simcbis" not in skipped:
                skipped["simcbis"] = "no simc_bis rows"

            # top50: the top-50 verified players' most-common per-slot gear worn
            # with their most-common WHOLE talent build (both from the top-50
            # tables). Gear + talents are wrapped together so a top50-only failure
            # records a per-spec skip instead of aborting the whole profile build
            # (which would silently emit no .simc / manifest / tierlist_gear at all).
            # Enchants/gems reuse the prep's top-50 maps (see _top50_gear). The
            # talent build is the real Blizzard v2 export string the players used in
            # game (top_player_loadouts.loadout_text, NOT the synthetic loadout_key)
            # so the actor sims and the modal decode the exact string the game
            # accepts (see _top50_talents).
            try:
                top50_loadouts = databaseConnector.fetch_top50_loadouts(conn, cursor, spec_id, season)
                top_gear = None
                top_talents = None
                if top50_loadouts:
                    top_gear, top_slots = _top50_gear(
                        top50_loadouts, item_lookup, spec_id,
                        prep["enchant_map"], prep["gem_ranking"],
                    )
                    top_talents = _top50_talents(top50_loadouts)
                if top_gear and top_talents:
                    top_header = build_header(
                        class_name, spec_name, primary, top_talents,
                        actor_name=f"spec{spec_id}_top50",
                    )
                    actors["top50"].append(_actor_block(top_header, top_slots, top_gear))
                    gear_data.setdefault(spec_id, {})["top50"] = _gear_display(
                        top_slots, top_gear, top_talents, item_lookup, bonus_quality
                    )
                    built.append("top50")
                elif not top_gear:
                    skipped["top50"] = "no top-50 loadouts"
                else:
                    # Gear is recorded but no real loadout_text is stored yet (the
                    # collector repopulates the column on its next run). Skip top50
                    # rather than sim its gear with the popular talents, so the bar
                    # never claims a build the top-50 players did not use.
                    skipped["top50"] = "no top-50 loadout_text"
            except Exception as e:
                skipped["top50"] = f"top50 build failed: {e}"

            manifest_specs[spec_id] = {"actors": built, "skipped": skipped}
            simcBis._log(f"tierlist: spec {spec_id} ({class_name}/{spec_name}) built={built} skipped={list(skipped)}")

    return actors, manifest_specs, gear_data


def main(output_dir, season, target_error, only_specs):
    simcBis._init_pool_from_env()
    actors, manifest_specs, gear_data = build_profiles(season, target_error, only_specs)

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

    # The per-(spec, gear set) display records for the tierlist page's gear modal.
    # Keyed by spec id as a string so it survives the JSON round trip the client
    # fetch reads. The assemble job copies this into assets/json/ (see
    # buildPages.yml) alongside the analyzer catalogs the modal resolves against.
    gear_path = os.path.join(output_dir, "tierlist_gear.json")
    gear_payload = {str(sid): sets for sid, sets in gear_data.items()}
    with open(gear_path, "w", encoding="utf-8") as f:
        json.dump(gear_payload, f, separators=(",", ":"), ensure_ascii=False)
    print(f"Wrote {gear_path} covering {len(gear_payload)} spec(s)")

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
