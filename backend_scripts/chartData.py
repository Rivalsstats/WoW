"""Pure-python chart dataset builders shared by the dashboard page and the
image_generation renderers. Stdlib only — no matplotlib/PIL/DB imports."""

from collections import defaultdict

RARITY_COLORS = {
    "Legendary": "#ff8000",
    "Epic": "#a335ee",
    "Uncommon": "#1eff00",
    "Depleted": "#FF0000",
}


def compute_shades(rgb, count):
    """Generate brightness-shaded variants for a base RGB tuple."""
    r, g, b = rgb
    shades = []
    for idx in range(count):
        offset = (idx - (count - 1) / 2) / (count - 1 or 1)
        factor = 1 + offset * 0.2
        clamp = lambda v: min(255, max(0, round(v * factor)))
        shades.append({"r": clamp(r), "g": clamp(g), "b": clamp(b)})
    return shades


def create_spec_scatter(spec_upgrades, spec_lookup, class_lookup, highest_key):
    """
    spec_upgrades: list of dicts:
      {"spec_id": int, "keystone_level": int, "upgrade_3": int, "upgrade_2": int,
       "upgrade_1": int, "depleted": int, "total_runs": int}
    spec_lookup: dict keyed by string spec_id -> spec metadata
    class_lookup: dict keyed by string classID -> class metadata (color etc.)
    highest_key: fallback row/dict that contains 'keystone_level' (max level)
    Returns: list of point dicts for scatter plot
    """
    # Ceiling for the depletion penalty. Use the highest key that actually
    # appears in the spec data so the penalty scales with the spec-tracked
    # range (the global "highest run" can exceed this and isn't guaranteed to
    # have spec data, which would silently inflate every spec's penalty).
    max_level = max(
        (int(r["keystone_level"]) for r in spec_upgrades),
        default=int(highest_key.get("keystone_level", 0)),
    )
    BASE_EXP = 1.3

    # group rows by spec_id
    rows_by_spec = {}
    for r in spec_upgrades:
        sid = int(r["spec_id"])
        rows_by_spec.setdefault(sid, []).append(r)

    points = []
    for spec_id, rows in rows_by_spec.items():
        total_runs = 0
        total_score = 0.0

        # iterate each keystone level row for this spec
        for row in rows:
            lvl = int(row["keystone_level"])
            # counts for each tier (ensure ints)
            c3 = int(row.get("upgrade_3", 0))
            c2 = int(row.get("upgrade_2", 0))
            c1 = int(row.get("upgrade_1", 0))
            cdep = int(row.get("depleted", 0))

            # depleted: negative weight, scaled by (max_level+1 - lvl)
            if cdep:
                weight_dep = -(max_level + 1 - lvl)
                total_runs += cdep
                total_score += weight_dep * cdep

            # tiers 3,2,1: n * BASE_EXP^(lvl-1)
            if c3:
                weight3 = 3 * (BASE_EXP ** (lvl - 1))
                total_runs += c3
                total_score += weight3 * c3
            if c2:
                weight2 = 2 * (BASE_EXP ** (lvl - 1))
                total_runs += c2
                total_score += weight2 * c2
            if c1:
                weight1 = 1 * (BASE_EXP ** (lvl - 1))
                total_runs += c1
                total_score += weight1 * c1

            # Note: row['total_runs'] should equal c1+c2+c3+cdep but we already count per-tier above

        perf = (total_score / total_runs) if total_runs > 0 else 0.0
        runs = total_runs

        # lookup spec & class data (safe lookups)
        sdata = spec_lookup.get(str(spec_id))
        if not sdata:
            # skip unknown specs
            continue
        cdata = class_lookup.get(str(sdata.get("classID", "")), {})

        color = cdata.get("color", {"r": 150, "g": 150, "b": 150})
        rcol = int(color.get("r", 150))
        gcol = int(color.get("g", 150))
        bcol = int(color.get("b", 150))
        border = f"rgba({rcol},{gcol},{bcol},0.8)"
        bg = f"rgba({rcol},{gcol},{bcol},0.4)"
        icon_url = f"/data/icons/{sdata.get('SpellIconFileId')}.jpg"

        points.append(
            {
                "label": sdata.get("name", f"Spec {spec_id}"),
                "x": round(perf, 4),
                "y": runs,
                "iconUrl": icon_url,
                "borderColor": border,
                "backgroundColor": bg,
            }
        )

    return points


def create_dungeon_ease(dungeon_data, dungeon_lookup, top_n=None):
    """
    rows: list of dicts from  SQL:
      { "dungeon_id": ..., "keystone_level": ..., "tier_3": ..., "tier_2": ..., "tier_1": ..., "depleted": ..., "total_runs": ... }
    dungeon_lookup: mapping keyed by string dungeon_id -> info (with name.en_US)
    top_n: optional int to limit returned dungeons to top N by total runs
    Returns: {"keyLevels": [...], "datasets": [{label, data, rawCounts}, ...]}
    """
    # aggregate counts per dungeon -> level
    counts_by_dungeon = defaultdict(lambda: defaultdict(int))
    total_by_dungeon = defaultdict(int)
    levels_set = set()

    for r in dungeon_data:
        dungeon_id = str(r["dungeon_id"])
        level = int(r["keystone_level"])
        # popularity should reflect timed keys only, so exclude depleted runs
        cnt = int(r.get("total_runs", 0)) - int(r.get("depleted", 0))

        counts_by_dungeon[dungeon_id][level] += cnt
        total_by_dungeon[dungeon_id] += cnt
        levels_set.add(level)

    # all keystone levels (sorted)
    key_levels = sorted(levels_set)

    # total runs across dungeons for every level (denominator for percent)
    total_by_level = {
        lvl: sum(counts_by_dungeon[d].get(lvl, 0) for d in counts_by_dungeon.keys())
        for lvl in key_levels
    }

    # sort dungeons by total runs desc
    dungeon_ids_sorted = sorted(
        counts_by_dungeon.keys(), key=lambda d: total_by_dungeon[d], reverse=True
    )
    if top_n:
        dungeon_ids_sorted = dungeon_ids_sorted[:top_n]

    datasets = []
    for dungeon_id in dungeon_ids_sorted:
        info = dungeon_lookup.get(dungeon_id, {})
        name = info.get("name", {}).get("en_US", dungeon_id)

        pct_data = []
        raw_counts = []
        for lvl in key_levels:
            cnt = counts_by_dungeon[dungeon_id].get(lvl, 0)
            raw_counts.append(cnt)
            denom = (
                total_by_level.get(lvl, 0) or 1
            )  # avoid div0; if denom==0 results will be 0
            pct = round((cnt / denom) * 100.0, 1) if denom else 0.0
            pct_data.append(pct)

        datasets.append({"label": name, "data": pct_data, "rawCounts": raw_counts})

    return {"keyLevels": key_levels, "datasets": datasets}


def create_dungeon_week_deltas(period_rows, min_runs=1000):
    """
    period_rows: list of dicts from fetch_dungeon_timed_runs_last_two_periods:
      {"period_id": ..., "dungeon_id": ..., "timed_runs": ...}
    Returns {str(dungeon_id): delta} where delta is the change in the dungeon's
    share of timed runs (percentage points, rounded to 0.1) between the two most
    recent weekly periods, or None when there is no meaningful comparison yet
    (single period at season start, or either week below min_runs timed runs —
    e.g. right after a reset).
    """
    runs_by_period = defaultdict(lambda: defaultdict(int))
    for r in period_rows:
        runs_by_period[int(r["period_id"])][str(r["dungeon_id"])] += int(
            r["timed_runs"]
        )
    if len(runs_by_period) < 2:
        return None

    prev_id, cur_id = sorted(runs_by_period)[-2:]
    prev, cur = runs_by_period[prev_id], runs_by_period[cur_id]
    prev_total, cur_total = sum(prev.values()), sum(cur.values())
    if prev_total < min_runs or cur_total < min_runs:
        return None

    return {
        d: round((cur.get(d, 0) / cur_total - prev.get(d, 0) / prev_total) * 100.0, 1)
        for d in set(prev) | set(cur)
    }
