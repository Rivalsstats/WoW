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
from contextlib import closing

import databaseConnector
from aggregateData import get_access_token, get_current_season_id
from tierMath import build_buff_tiers, build_ckmeans_tiers, build_spec_tiers
from generateSpecPages import LOOKUP_DIR, load_json
from pageGeneration import TRENDS_LIVE_PATH

CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)

TIER_LETTERS = ["S", "A", "B", "C", "D", "F"]

# How many weeks of snapshots to keep. Two is the functional minimum (this week +
# last week); a small buffer survives a skipped build or a mid-week backfill.
RETENTION_WEEKS = 8

# top-N kept per group for the bounded feeds
TOP_N_ITEMS_PER_SLOT = 5
TOP_N_MISC = 5           # embellishments / gems / crafted
TOP_N_COMBOS = 5         # tier-set / embellishment / crafted / gem combos
TOP_N_DUNGEON_COMPS = 5


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
    spec_data = databaseConnector.fetch_spec_upgrades(conn, cursor)
    spec_tiers = build_spec_tiers(spec_lookup, class_lookup, spec_data, weight_base=1.6, k=6)
    spec_flat = list(_flatten_tiers(spec_tiers, "spec_id"))
    spec_total = sum(tr for _, _, _, tr in spec_flat)
    for sid, tier, score, tr in spec_flat:
        rows.append(_row(week_id, "spec", "", sid, None, tier, None, score, _pct(tr, spec_total), tr))

    # --- dungeons ----------------------------------------------------------
    dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, season)
    dungeon_tiers = build_ckmeans_tiers(dungeon_lookup, dungeon_data, weight_base=1.6, k=6)
    dungeon_flat = list(_flatten_tiers(dungeon_tiers, "dungeon_id"))
    dungeon_total = sum(tr for _, _, _, tr in dungeon_flat)
    for did, tier, score, tr in dungeon_flat:
        rows.append(_row(week_id, "dungeon", "", did, None, tier, None, score, _pct(tr, dungeon_total), tr))

    # --- group buffs -------------------------------------------------------
    group_buffs = lookups["group_buffs"]
    buff_stats = databaseConnector.fetch_groupbuffs_stats(
        conn, cursor, group_buffs, season, 12, 14
    )
    buff_tiers = build_buff_tiers(buff_lookup, buff_stats)
    for idx, letter in enumerate(TIER_LETTERS):
        for it in buff_tiers.get(letter, []):
            pct = float(it.get("score", it.get("lb_ci", 0.0)) or 0.0)
            rows.append(_row(
                week_id, "buff", "", it["buff_id"], None, idx, None, pct, pct,
                int(it.get("runs", 0) or 0),
            ))
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

    # embellishments / gems / crafted — top-N, share within the feed
    for feed, fetch in (
        ("embellishment", databaseConnector.fetch_embellishment_usage),
        ("gem", databaseConnector.fetch_gem_usage),
        ("crafted", databaseConnector.fetch_crafted_usage),
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
        parsed = [{"comp": r[0], "run_count": int(r[1] or 0)} for r in combos]
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


def build_dungeon_rows(conn, cursor, week_id, season, dungeon_id):
    """Per-dungeon 'best for high keys' comps (top-N, ranked by high-key score)."""
    top, dungeon_total = databaseConnector.fetch_dungeon_high_key_comps(
        conn, cursor, dungeon_id, season, TOP_N_DUNGEON_COMPS
    )
    rows = []
    for pos, c in enumerate(top, start=1):
        rows.append(_row(
            week_id, "comp", str(dungeon_id), c["comp"], c["comp"],
            None, pos, None, _pct(c["total_runs"], dungeon_total), c["total_runs"],
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
        token = get_access_token(CLIENT_ID, CLIENT_SECRET)
        season = get_current_season_id(token)

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
        for did in lookups["dungeons"]:
            records += build_dungeon_rows(conn, cursor, week_id, season, did)
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
