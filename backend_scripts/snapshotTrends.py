"""Weekly snapshot writer for the "Top Trends" bar.

Runs as a CI step *before* page generation. Reads the current nightly aggregates
and writes one row per (week_id, feed, group_key, entity) into `trend_snapshot`
(see database.sql). The page generators later read the latest two weeks back via
``databaseConnector.fetch_trend_snapshots`` + ``pageGeneration.build_trends`` to
render week-over-week movement.

Design notes:
  * ``week_id`` is the current Blizzard reset period. Two outputs each build:
      1. The current live records go to a **build-local JSON** (TRENDS_LIVE_PATH),
         rewritten every build and NEVER stored in the DB. pageGeneration.build_trends
         reads it as the fresh "now" side, so the displayed number reflects right-now
         rather than a value frozen at week-start.
      2. A **write-once weekly baseline** is stored in the DB (guarded by
         fetch_trend_week_exists): captured on the FIRST build of a period and left
         frozen. build_trends diffs the live "now" against the PREVIOUS period's
         baseline ("last week's snapshotted value"). Freezing at first build keeps a
         consistent phase so the delta spans a whole period instead of collapsing to
         a day right after a reset.
  * ``--force`` re-stores the baseline (backfill a bad run). ``--debug-fake-live``
    fakes only the live JSON (jittered, tagged as next week) so the bar renders
    locally with visible movement against the real baseline — no fake DB rows.
  * Spec / dungeon / buff feeds carry an S..F ``tier`` (0=S) + ``score``
    (tierMath lb_ci); every other feed carries a within-group ``rank_pos``.
    Tiers are computed in Python (tierMath), which is why this is a Python step
    rather than a stored procedure.
  * The heavy raw tables are never snapshotted as-is: talents are rolled up to
    per-talent pickrate, comps are ranked to the top few per group. Everything
    stored is bounded top-N (except talents, which are a small fixed set).
"""

import argparse
import hashlib
import json
import os
import random
from collections import defaultdict
from contextlib import closing

import databaseConnector
from commonUtils import current_season_id
from compArchetypes import build_dungeon_archetypes, collapse_comps, compute_glue_specs
from tierMath import build_buff_tiers, build_ckmeans_tiers, build_spec_tiers
from generateSpecPages import LOOKUP_DIR, load_json
from pageGeneration import TRENDS_LIVE_PATH

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    # 2 (not 1): the item feed sweep (build_item_rows -> generateItemPages.
    # build_payloads) checks out its OWN pooled connection while main still holds
    # this step's connection, so a single-slot pool would deadlock/exhaust.
    2,
)

TIER_LETTERS = ["S", "A", "B", "C", "D", "F"]

# How many weeks of snapshots to keep. Two is the functional minimum (this week +
# last week); a small buffer survives a skipped build or a mid-week backfill.
RETENTION_WEEKS = 8

# top-N kept per group for the bounded feeds
TOP_N_ITEMS_PER_SLOT = 5
TOP_N_MISC = 5           # embellishments / gems / crafted
TOP_N_COMBOS = 5         # tier-set / embellishment / crafted / gem combos
TOP_N_ARCHETYPES = 6     # families kept per context (matches the comps page top_n)
TOP_N_PULLS = 6          # most-lusted pull signatures kept per dungeon
TOP_N_LOOT = 8           # best-loot drops kept per dungeon (matches the page card)
TOP_N_ITEM_GLOBAL = 60   # popular items surfaced on the items-list bar (also the
                         # set that gets bounded per-item subpage feeds)
TOP_N_ITEM_SUB = 8       # entries kept per per-item subpage feed


def _ekey(raw):
    """A stable entity_key that fits the varchar(128) column. Short ids pass
    through; long/opaque values (comp strings) are md5-hashed so the primary key
    stays bounded and deterministic."""
    raw = str(raw)
    if len(raw) <= 128:
        return raw
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _flatten_tiers(tiers, id_key):
    """Yield (entity_id, tier_index, score, total_runs) from a tierMath tiers
    dict (letter -> [item dicts])."""
    for idx, letter in enumerate(TIER_LETTERS):
        for it in tiers.get(letter, []):
            yield (
                it[id_key],
                idx,
                float(it.get("score", it.get("lb_ci", 0.0)) or 0.0),
                int(it.get("total_runs", 0) or 0),
            )


def _pct(part, whole):
    return round(100.0 * part / whole, 4) if whole else 0.0


def _behind_pct(leader, score):
    """"% behind #1" on a tier feed's own score axis: (leader - score)/leader*100.
    0 == the group leader; larger == further behind. The bar diffs this so green/up
    means the gap to #1 closed (moved toward S). Guards a zero/absent leader."""
    if not leader:
        return 0.0
    return round(max(0.0, (leader - score) / leader * 100.0), 4)


def _row(week_id, feed, group_key, entity_key, label, tier, rank_pos, score, popularity, run_count):
    """One snapshot record as a dict. Written as-is to the build-local live JSON
    (pageGeneration reads it as the fresh 'now' side); converted to a DB tuple via
    _record_tuple only for the once-per-period stored baseline."""
    return {
        "week_id": int(week_id),
        "feed": feed,
        "group_key": str(group_key),
        "entity_key": _ekey(entity_key),
        "label": label,
        "tier": None if tier is None else int(tier),
        "rank_pos": None if rank_pos is None else int(rank_pos),
        "score": None if score is None else float(score),
        "popularity": float(popularity),
        "run_count": int(run_count),
    }


def _record_tuple(r):
    """Snapshot record dict -> the 10-tuple upsert_trend_rows expects."""
    return (
        r["week_id"], r["feed"], r["group_key"], r["entity_key"], r["label"],
        r["tier"], r["rank_pos"], r["score"], r["popularity"], r["run_count"],
    )


def write_live_trends(week_id, records):
    """Write the current live records to the build-local JSON that pageGeneration
    reads as the fresh 'now' side of the bar. Overwritten every build; never the DB."""
    os.makedirs(os.path.dirname(TRENDS_LIVE_PATH) or ".", exist_ok=True)
    with open(TRENDS_LIVE_PATH, "w", encoding="utf-8") as fh:
        json.dump({"week_id": int(week_id), "records": records}, fh, separators=(",", ":"))
    print(f"  wrote live trends -> {TRENDS_LIVE_PATH} ({len(records)} records)")


def build_global_rows(conn, cursor, week_id, season, lookups):
    """Spec, dungeon and group-buff feeds (global scope, S..F tiered)."""
    rows = []
    spec_lookup = lookups["specs"]
    class_lookup = lookups["classes"]
    dungeon_lookup = lookups["dungeons"]
    buff_lookup = lookups["buffs"]

    # --- specs -------------------------------------------------------------
    # Tiered feeds carry "% behind #1" as their popularity: the displayed number now
    # matches the tierlist axis (score) instead of an unrelated run-share, so a spec /
    # dungeon that climbs a tier also shows its gap to #1 shrinking.
    spec_data = databaseConnector.fetch_spec_upgrades(conn, cursor)
    spec_tiers = build_spec_tiers(spec_lookup, class_lookup, spec_data, weight_base=1.6, k=6)
    spec_flat = list(_flatten_tiers(spec_tiers, "spec_id"))
    spec_leader = max((score for _, _, score, _ in spec_flat), default=0.0)
    for sid, tier, score, tr in spec_flat:
        rows.append(_row(week_id, "spec", "", sid, None, tier, None, score, _behind_pct(spec_leader, score), tr))

    # --- dungeons ----------------------------------------------------------
    dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, season)
    # live highest-timed-key per dungeon overrides the slower rollup ceiling
    dungeon_max_timed = databaseConnector.fetch_max_timed_level_per_dungeon(conn, cursor, season)
    dungeon_tiers = build_ckmeans_tiers(
        dungeon_lookup, dungeon_data, weight_base=1.6, k=6,
        max_timed_levels=dungeon_max_timed,
    )
    dungeon_flat = list(_flatten_tiers(dungeon_tiers, "dungeon_id"))
    dungeon_leader = max((score for _, _, score, _ in dungeon_flat), default=0.0)
    for did, tier, score, tr in dungeon_flat:
        rows.append(_row(week_id, "dungeon", "", did, None, tier, None, score, _behind_pct(dungeon_leader, score), tr))

    # --- group buffs -------------------------------------------------------
    group_buffs = lookups["group_buffs"]
    buff_stats = databaseConnector.fetch_groupbuffs_stats(
        conn, cursor, group_buffs, season, 12, 14
    )
    buff_tiers = build_buff_tiers(buff_lookup, buff_stats)
    buff_flat = [
        (it["buff_id"], idx, float(it.get("score", it.get("lb_ci", 0.0)) or 0.0), int(it.get("runs", 0) or 0))
        for idx, letter in enumerate(TIER_LETTERS)
        for it in buff_tiers.get(letter, [])
    ]
    buff_leader = max((score for _, _, score, _ in buff_flat), default=0.0)
    for bid, idx, score, runs in buff_flat:
        rows.append(_row(week_id, "buff", "", bid, None, idx, None, score, _behind_pct(buff_leader, score), runs))
    return rows


def _rank_rows(week_id, feed, group_key, ranked, denom, key_fn, label_fn=None):
    """Emit rank_pos rows for a pre-sorted (desc by run_count) list."""
    out = []
    for pos, item in enumerate(ranked, start=1):
        run_count = item["run_count"]
        out.append(_row(
            week_id, feed, group_key, key_fn(item),
            label_fn(item) if label_fn else None,
            None, pos, None, _pct(run_count, denom), run_count,
        ))
    return out


def build_spec_rows(conn, cursor, week_id, season, spec_id, spec_runs):
    """All per-spec feeds for one spec: talents (all), items per slot, and the
    embellishment / gem / crafted / combo feeds (bounded top-N)."""
    rows = []
    gk = str(spec_id)

    # talents (rolled up per talent across all three trees; store all)
    talents = databaseConnector.fetch_talent_usage(conn, cursor, spec_id, season)
    talents.sort(key=lambda t: t["run_count"], reverse=True)
    for pos, t in enumerate(talents, start=1):
        rows.append(_row(
            week_id, "talent", gk, f"{t['tree']}:{t['talent_id']}", None,
            None, pos, None, _pct(t["run_count"], spec_runs), t["run_count"],
        ))

    # items: top-N per slot, share within the slot
    equipment = databaseConnector.fetch_equipment_usage(conn, cursor, spec_id, season)
    by_slot = {}
    for e in equipment:
        by_slot.setdefault(e["slot"], []).append(e)
    for slot, items in by_slot.items():
        items.sort(key=lambda e: e["run_count"], reverse=True)
        slot_total = sum(e["run_count"] for e in items)
        for pos, e in enumerate(items[:TOP_N_ITEMS_PER_SLOT], start=1):
            rows.append(_row(
                week_id, "item", gk, f"{slot}:{e['item_id']}", slot,
                None, pos, None, _pct(e["run_count"], slot_total), e["run_count"],
            ))

    # embellishments / gems / crafted / missives: top-N, share within the feed
    for feed, fetch in (
        ("embellishment", databaseConnector.fetch_embellishment_usage),
        ("gem", databaseConnector.fetch_gem_usage),
        ("crafted", databaseConnector.fetch_crafted_usage),
        ("missive", databaseConnector.fetch_missive_usage),
    ):
        usage = fetch(conn, cursor, spec_id, season)
        usage.sort(key=lambda u: u["run_count"], reverse=True)
        denom = sum(u["run_count"] for u in usage)
        rows += _rank_rows(
            week_id, feed, gk, usage[:TOP_N_MISC], denom,
            key_fn=lambda u: u["item_id"],
        )

    # gear combos — reuse the existing spec-comp fetchers (already top-10/15).
    # Rows are (comp, total_runs, max_timed_key, max_depleted_key).
    for feed, fetch in (
        ("set_combo", databaseConnector.fetch_tier_set_comps),
        ("embellishment_combo", databaseConnector.fetch_embellishment_comps),
        ("crafted_combo", databaseConnector.fetch_crafted_comps),
        ("gem_combo", databaseConnector.fetch_gem_comps),
    ):
        combos = fetch(conn, cursor, spec_id, season)
        # A "combo" is only meaningful with >= 2 members; drop 1- or 0-member combos
        # (they render as a single lone icon that reads as a plain item, not a combo).
        parsed = [
            {"comp": r[0], "run_count": int(r[1] or 0)} for r in combos
            if len([x for x in str(r[0] or "").split(",") if x.strip()]) >= 2
        ]
        parsed.sort(key=lambda c: c["run_count"], reverse=True)
        denom = sum(c["run_count"] for c in parsed)
        rows += _rank_rows(
            week_id, feed, gk, parsed[:TOP_N_COMBOS], denom,
            key_fn=lambda c: c["comp"], label_fn=lambda c: c["comp"],
        )
    return rows


def jitter_records(records, week_id, jitter=0.4, seed=1234):
    """Debug only: return a copy of the records re-tagged to ``week_id`` with
    popularity / score / run_count nudged by a random relative amount and the tier
    / rank occasionally shifted. Used to fake the live "now" snapshot so the bar
    renders locally with visible movement against the real baseline. Deterministic
    (seeded) so reruns are stable. Never in CI."""
    rng = random.Random(seed)
    out = []
    for r in records:
        factor = 1.0 + rng.uniform(-jitter, jitter)
        p_tier = r["tier"]
        if p_tier is not None:
            p_tier = min(5, max(0, p_tier + rng.choice([-1, 0, 0, 1])))
        p_rank = r["rank_pos"]
        if p_rank is not None:
            p_rank = max(1, p_rank + rng.choice([-2, -1, 0, 1, 2]))
        out.append({
            **r,
            "week_id": int(week_id),
            "tier": p_tier,
            "rank_pos": p_rank,
            "score": None if r["score"] is None else r["score"] * factor,
            "popularity": max(0.0, r["popularity"] * factor),
            "run_count": max(0, int(r["run_count"] * factor)),
        })
    return out


def build_archetype_rows(conn, cursor, week_id, season, spec_lookup, class_lookup, dungeon_ids):
    """Team-comp *archetype* feed (the comps page's grouped families), one bounded
    top-N list per context: the global 'all' bar plus one per dungeon (which also
    powers the dungeon-page bar). Reuses the exact clustering the comps page renders
    (collapse_comps -> build_dungeon_archetypes) off a single aggregated_dungeon_comps
    scan, so no extra SQL.

    Ranked by popularity (runs) — the most stable "is this comp rising or falling"
    signal. entity_key is the family's most-PLAYED member (`key_c`), a handle that
    survives the displayed core flipping to a fresh top-key comp; label is the
    displayed core (`c`) so the bar renders the recognisable archetype icons."""
    raw = databaseConnector.fetch_all_comps(conn, cursor, season)
    collapsed = collapse_comps(raw, spec_lookup)
    arch = build_dungeon_archetypes(
        collapsed, spec_lookup, class_lookup, dungeon_ids, top_n=TOP_N_ARCHETYPES
    )
    rows = []
    for ctx, flavours in arch.items():
        popular = flavours.get("popular", [])
        ctx_total = sum(int(f["runs"]) for f in popular)
        for pos, f in enumerate(popular, start=1):
            key_c = f.get("key_c") or f["c"]
            rows.append(_row(
                week_id, "archetype", str(ctx),
                ",".join(str(s) for s in key_c),          # stable identity handle
                ",".join(str(s) for s in f["c"]),         # displayed core -> icon cluster
                None, pos, None, _pct(int(f["runs"]), ctx_total), int(f["runs"]),
            ))

        # "best for high keys" families -> the comps page bar (a SEPARATE feed so the
        # dungeon page keeps the popular archetype). Only the global 'all' context.
        # A ranked feed: the rank-1 family is "meta" purely by sitting at #1, shown
        # through its rank movement (no separate meta flag).
        if str(ctx) == "all":
            highkey = flavours.get("highkey", [])
            hk_total = sum(int(f["hk_runs"]) for f in highkey)
            for pos, f in enumerate(highkey, start=1):
                key_c = f.get("key_c") or f["c"]
                rows.append(_row(
                    week_id, "archetype_hk", "all",
                    ",".join(str(s) for s in key_c),
                    ",".join(str(s) for s in f["c"]),
                    None, pos, None,
                    _pct(int(f["hk_runs"]), hk_total), int(f["hk_runs"]),
                ))

            # Glue Specs / Flexibility Index -> the second comps-page feed, so the bar
            # fills 12 without duplicating the small high-key family set. Same data as
            # the comps page's Flexibility card (compute_glue_specs), ranked by flex_pct
            # across roles; entity_key is the spec id (spec-icon render, rank movement).
            flex_input = [{"specs": e["c"], "timed": e["t"], "depleted": e["d"],
                           "avg_key": e["avg_key"], "max_key": e["mk"]} for e in collapsed]
            glue_by_role, _glue_flat = compute_glue_specs(flex_input, spec_lookup)
            flat = [g for specs in glue_by_role.values() for g in specs]
            flat.sort(key=lambda g: (g["flex_pct"], g["runs"], -int(g["spec_id"])), reverse=True)
            for pos, g in enumerate(flat, start=1):
                rows.append(_row(
                    week_id, "flex", "all", g["spec_id"], None,
                    None, pos, round(float(g["flex_score"]), 4),
                    float(g["flex_pct"]), int(g["runs"]),
                ))
    return rows


def build_dungeon_pull_rows(conn, cursor, week_id, dungeon_ids):
    """Most-lusted pull feed, one bounded top-N list per dungeon. Source is the same
    lust timeline the dungeon page shows; each row's pull signature ("<npc>:<count>,
    ...") is stored as the label so the bar can render the pull's npc portrait cluster
    with per-mob multiplicity badges. entity_key is the sorted npc-id set (counts
    dropped) so a pull keeps its identity across weeks even if a mob's count wobbles.
    Ranked by lust volume; popularity is the pull's lust percentage.

    A plain tuple cursor is assumed here (snapshotTrends' own cursor); the lust query
    returns (top_npcs, total_pulls, lust_count, lust_percentage, ...)."""
    rows = []
    for did in dungeon_ids:
        timeline = databaseConnector.fetch_dungeon_lust_timeline(conn, cursor, did) or []
        pos = 0
        for r in timeline:
            top_npcs = r[0]
            if not top_npcs:
                continue
            lust_count = int(r[2] or 0)
            lust_pct = float(r[3] or 0.0)
            npc_ids = ",".join(
                seg.split(":")[0].strip()
                for seg in str(top_npcs).split(",") if seg.strip()
            )
            pos += 1
            rows.append(_row(
                week_id, "pull", str(did), _ekey(npc_ids), top_npcs,
                None, pos, None, round(lust_pct, 4), lust_count,
            ))
            if pos >= TOP_N_PULLS:
                break
    return rows


def build_dungeon_loot_rows(conn, cursor, week_id, season, dungeon_lookup, spec_ids):
    """Best-loot feed, one bounded top-N list per dungeon. Mirrors the dungeon page's
    "Best Loot From This Dungeon" card: the items that drop in a dungeon (Raidbots
    sources joined via journal_instance_id) ranked by how much the current meta
    equips them (global per-item usage summed across specs). Ranked by runs;
    popularity is the item's share of that dungeon's total loot runs.

    Uses the caller's plain tuple cursor: fetch_item_spec_usage returns
    (item_id, run_count, max_timed_key, max_depleted_key)."""
    equippable_items = load_json(os.path.join(LOOKUP_DIR, "equippable-items.json"))
    instance_to_dungeon = {}
    for d_id, d_data in dungeon_lookup.items():
        jii = d_data.get("journal_instance_id")
        if jii is not None:
            instance_to_dungeon[int(jii)] = str(d_id)
    source_items_by_dungeon = defaultdict(set)
    for it in equippable_items:
        for src in it.get("sources", []) or []:
            d_id = instance_to_dungeon.get(src.get("instanceId"))
            if d_id:
                source_items_by_dungeon[d_id].add(it["id"])

    # global per-item usage across the meta (summed across specs)
    item_total_runs = defaultdict(int)
    for sid in spec_ids:
        try:
            spec_id_int = int(sid)
        except (TypeError, ValueError):
            continue
        for row in databaseConnector.fetch_item_spec_usage(conn, cursor, season, spec_id_int) or []:
            raw_iid = row[0]
            runs = int(row[1] or 0)
            if runs <= 0 or not str(raw_iid).isdigit():
                continue
            item_total_runs[int(raw_iid)] += runs

    rows = []
    for did, item_ids in source_items_by_dungeon.items():
        loot = [(iid, item_total_runs.get(iid, 0)) for iid in item_ids
                if item_total_runs.get(iid, 0) > 0]
        loot.sort(key=lambda x: x[1], reverse=True)
        denom = sum(r for _, r in loot)
        for pos, (iid, runs) in enumerate(loot[:TOP_N_LOOT], start=1):
            rows.append(_row(
                week_id, "loot", str(did), iid, None,
                None, pos, None, _pct(runs, denom), runs,
            ))
    return rows


def build_item_rows(week_id, season):
    """Global per-slot item feed (items-list bar) + bounded per-item subpage feeds,
    both off one generateItemPages sweep so the displayed numbers match the item
    pages exactly. build_payloads opens/closes its own pooled connection.

    Global feed: top-N items site-wide by per-slot adoption (payload global.adoption),
    entity_key "<slot>:<item_id>". Per-item feeds are emitted only for that popular
    top-N (bounds the row count, skips the long tail): used-by-specs / gems /
    embellishments / missives / ilvl variants, each a ranked share feed that the item
    subpage only shows when the item actually has that data."""
    import generateItemPages
    ctx = generateItemPages.load_static_lookups()
    payloads, _manifest = generateItemPages.build_payloads(season, ctx)

    ranked = []
    for pl in payloads.values():
        g = pl.get("global") or {}
        adoption = g.get("adoption")
        if adoption is None:
            continue
        ranked.append((pl["slotKey"], int(pl["id"]), float(adoption), pl))
    ranked.sort(key=lambda x: x[2], reverse=True)
    top = ranked[:TOP_N_ITEM_GLOBAL]

    rows = []
    for pos, (slot, item_id, adoption, _pl) in enumerate(top, start=1):
        rows.append(_row(
            week_id, "item", "", f"{slot}:{item_id}", None,
            None, pos, None, round(adoption, 4), 0,
        ))

    for slot, item_id, _adoption, pl in top:
        g = pl.get("global") or {}
        gk = str(item_id)

        # spec_overview is a TOP-LEVEL payload key (not under "global"); gems /
        # embellishments / missives / variants live under "global".
        specs_ov = [s for s in pl.get("spec_overview", []) if s.get("adoption") is not None]
        specs_ov.sort(key=lambda s: s["adoption"], reverse=True)
        for pos, s in enumerate(specs_ov[:TOP_N_ITEM_SUB], start=1):
            rows.append(_row(
                week_id, "item_spec", gk, s["spec_id"], None,
                None, pos, None, float(s["adoption"]), int(s.get("runs", 0)),
            ))

        for feed, key in (("item_gem", "gems"),
                          ("item_embellishment", "embellishments"),
                          ("item_missive", "missives")):
            for pos, e in enumerate((g.get(key) or [])[:TOP_N_ITEM_SUB], start=1):
                rows.append(_row(
                    week_id, feed, gk, e["id"], None,
                    None, pos, None, float(e.get("pct", 0.0)), int(e.get("runs", 0)),
                ))

        for pos, v in enumerate((g.get("variants") or [])[:TOP_N_ITEM_SUB], start=1):
            # The item page identifies a variant by its ilvl badge, falling back to
            # the track tag(s), then "Standard". Mirror that as the bar's badge text,
            # and key the row by ilvl (or the stable bonus signature when ilvl is
            # absent) so the same variant keeps its identity across weeks. item_id
            # stays the first ":"-segment so _resolve_entry resolves the item link.
            ilvl = v.get("ilvl")
            tags = v.get("tags") or []
            if ilvl:
                label = f"ilvl {ilvl}"
                disc = str(ilvl)
            elif tags:
                label = " ".join(tags)
                disc = v.get("bonus") or "-".join(tags)
            else:
                label = "Standard"
                disc = v.get("bonus") or "std"
            rows.append(_row(
                week_id, "item_variant", gk, f"{item_id}:{disc}", label,
                None, pos, None, float(v.get("pct", 0.0)), int(v.get("runs", 0)),
            ))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Write the weekly Top Trends snapshot.")
    parser.add_argument("--season", type=int, default=None, help="override season id")
    parser.add_argument("--week", type=int, default=None, help="override week_id (reset period)")
    parser.add_argument(
        "--debug-fake-live", action="store_true",
        help="DEBUG: write a jittered fake LIVE snapshot (this week's data nudged, "
             "tagged as next week) instead of the real one, so the bar renders locally "
             "with visible movement against the real stored baseline. No fake rows are "
             "written to the DB. Never use in CI.",
    )
    parser.add_argument(
        "--debug-jitter", type=float, default=0.4,
        help="relative jitter magnitude for --debug-fake-live (default 0.4)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="re-snapshot the current week even if it already exists (overrides the "
             "write-once-per-period rule; useful to backfill after a bad first run).",
    )
    args = parser.parse_args()

    lookups = {
        "specs": load_json(os.path.join(LOOKUP_DIR, "specs.json")),
        "classes": load_json(os.path.join(LOOKUP_DIR, "classes.json")),
        "dungeons": load_json(os.path.join(LOOKUP_DIR, "dungeons.json")),
        "group_buffs": load_json(os.path.join(LOOKUP_DIR, "groupbuffs.json")),
    }
    lookups["buffs"] = {b.get("id"): b for b in lookups["group_buffs"]}

    if args.season is not None:
        season = args.season
    else:
        season = current_season_id()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)

        week_id = args.week if args.week is not None else databaseConnector.fetch_current_period(
            conn, cursor, season
        )
        if week_id is None:
            print(f"No started reset period for season {season}; nothing to snapshot yet.")
            return

        print(f"Computing live trends for season {season}, week {week_id}...")

        # Always compute the current live records. These are the FRESH "now" side of
        # the bar; they go to a build-local JSON, never the DB, so the displayed
        # numbers reflect right-now rather than being frozen at week-start.
        records = build_global_rows(conn, cursor, week_id, season, lookups)
        print(f"  global feeds: {len(records)} records")
        spec_run_map = {
            r["id"]: r["count"]
            for r in databaseConnector.fetch_spec_run_counts(conn, cursor)
        }
        for sid in lookups["specs"]:
            spec_id = int(sid)
            records += build_spec_rows(
                conn, cursor, week_id, season, spec_id, spec_run_map.get(spec_id, 0)
            )
        dungeon_ids = [str(d) for d in lookups["dungeons"]]
        records += build_archetype_rows(
            conn, cursor, week_id, season, lookups["specs"], lookups["classes"], dungeon_ids
        )
        records += build_dungeon_pull_rows(conn, cursor, week_id, dungeon_ids)
        print(f"  + pull feed: {len(records)} records")
        records += build_dungeon_loot_rows(
            conn, cursor, week_id, season, lookups["dungeons"], lookups["specs"].keys()
        )
        print(f"  + loot feed: {len(records)} records")
        # Global item + bounded per-item feeds. build_payloads opens its OWN pooled
        # connection, so it runs outside this cursor's read session.
        records += build_item_rows(week_id, season)
        print(f"  total live records: {len(records)}")

        # 1) Current "now" -> build-local JSON, refreshed every build (NOT the DB).
        #    --debug-fake-live jitters it and tags it as the NEXT week so it differs
        #    from the real baseline, making the bar render locally with movement.
        if args.debug_fake_live:
            fake = jitter_records(records, week_id + 1, args.debug_jitter)
            write_live_trends(week_id + 1, fake)
            print(f"  debug: wrote a jittered fake live snapshot (week {week_id + 1})")
        else:
            write_live_trends(week_id, records)

        # 2) Weekly baseline -> DB, WRITE-ONCE per reset period. This frozen value is
        #    what build_trends diffs the live "now" against as "last week's
        #    snapshotted value". Rewriting it every build would let a week that ends a
        #    day before the reset become "last week" the next day and collapse the
        #    delta to ~1 day. --force backfills a bad run. (--debug-fake-live never
        #    writes fake rows to the DB — only the live JSON is faked.)
        baseline_exists = databaseConnector.fetch_trend_week_exists(conn, cursor, week_id)
        if args.force or not baseline_exists:
            tuples = [_record_tuple(r) for r in records]
            databaseConnector.upsert_trend_rows(conn, cursor, tuples)
            print(f"  stored weekly baseline for week {week_id} ({len(tuples)} rows)")
        else:
            print(f"  week {week_id} baseline already stored; keeping it (write-once).")

        databaseConnector.prune_trend_snapshots(conn, cursor, keep_from_week=week_id - RETENTION_WEEKS)
        databaseConnector.commit_with_retry(conn)
        print("Done.")


if __name__ == "__main__":
    main()
