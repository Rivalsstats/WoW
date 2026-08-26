import os
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from contextlib import closing
import argparse
import databaseConnector
from collections import defaultdict, Counter
from pageGeneration import generateSpecNav, generateDungeonNav, build_global_trends
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
    load_season_info,
)

try:
    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        # >=2 so build_global_trends() can check out a second connection while the
        # page build still holds its own (otherwise the trends bar silently hides).
        4,
    )
except Exception as pool_err:
    print(f"WARNING: database connection pool unavailable: {pool_err}")

# Re-exported from chartData for existing importers; the implementations moved
# there so the image_generation renderers can use them without this module.
from chartData import (
    RARITY_COLORS,
    compute_shades,
    create_spec_scatter,
    create_dungeon_ease,
)


METRIC_KEYS = ("total_runs", "depleted", "upgrade_1", "upgrade_2", "upgrade_3")


def createKeysPerWeek(rows):
    """Build the "Keys per Week" datasets + labels from the per-(week, day) rows
    returned by fetch_runs_per_period.

    Two grains, decided by how much of the season has elapsed:
      * >= 2 weeks of data -> one point per week (the normal weekly view).
      * exactly 1 week      -> one point per day, so the season's first week
        renders as a daily breakdown instead of a single lonely weekly point.

    Week numbers are normalised so the earliest week present is "Week 1". That
    keeps this axis numbered identically to the ordinal "Key Throughput" axis
    and makes it resilient to a stale Blizzard pre-season period lingering in
    season_periods (which would otherwise inflate every week number by one; see
    .claude/skills/blizzard-preseason-period). Gaps between real weeks are left
    intact rather than compressed. Returns (datasets, labels, grain).
    """
    if not rows:
        return [], [], "week"

    weeks = sorted({r["week"] for r in rows})
    grain = "day" if len(weeks) == 1 else "week"

    if grain == "day":
        # single week -> dense per-day axis over days 1..max observed day
        by_day = {r["day"]: r for r in rows}
        buckets = [by_day.get(day) for day in range(1, max(by_day) + 1)]
        period_labels = [f"Day {day}" for day in range(1, max(by_day) + 1)]
    else:
        # >= 2 weeks -> collapse the per-day rows back to weekly totals
        offset = weeks[0] - 1  # normalise so the earliest present week == Week 1
        agg = {w: {k: 0 for k in METRIC_KEYS} for w in weeks}
        for r in rows:
            for k in METRIC_KEYS:
                agg[r["week"]][k] += r[k]
        buckets = [agg[w] for w in weeks]
        period_labels = [f"Week {w - offset}" for w in weeks]

    def col(key):
        # a missing interior day bucket (no runs that day) reads as zero
        return [(b[key] if b else 0) for b in buckets]

    total_counts = col("total_runs")
    depleted_counts = col("depleted")
    plus_one_counts = col("upgrade_1")
    plus_two_counts = col("upgrade_2")
    plus_three_counts = col("upgrade_3")

    line_colors = {
        "Total": "#4A90E2",
        "Depleted": RARITY_COLORS["Depleted"],
        "+1": RARITY_COLORS["Uncommon"],
        "+2": RARITY_COLORS["Epic"],
        "+3": RARITY_COLORS["Legendary"],
    }

    # build the datasets list
    period_datasets = [
        {
            "label": "Total",
            "data": total_counts,
            "tension": 0.3,
            "borderColor": line_colors["Total"],
            "pointBackgroundColor": line_colors["Total"],
            "pointRadius": 4,
            "pointHoverRadius": 6,
        },
        {
            "label": "Depleted",
            "data": depleted_counts,
            "tension": 0.3,
            "borderColor": line_colors["Depleted"],
            "pointBackgroundColor": line_colors["Depleted"],
            "pointRadius": 4,
            "pointHoverRadius": 6,
        },
        {
            "label": "+1",
            "data": plus_one_counts,
            "tension": 0.3,
            "borderColor": line_colors["+1"],
            "pointBackgroundColor": line_colors["+1"],
            "pointRadius": 4,
            "pointHoverRadius": 6,
        },
        {
            "label": "+2",
            "data": plus_two_counts,
            "tension": 0.3,
            "borderColor": line_colors["+2"],
            "pointBackgroundColor": line_colors["+2"],
            "pointRadius": 4,
            "pointHoverRadius": 6,
        },
        {
            "label": "+3",
            "data": plus_three_counts,
            "tension": 0.3,
            "borderColor": line_colors["+3"],
            "pointBackgroundColor": line_colors["+3"],
            "pointRadius": 4,
            "pointHoverRadius": 6,
        },
    ]
    return period_datasets, period_labels, grain


def createDungeonPopularity(dungeons, dungeon_lookup):
    # Extract arrays
    short_names = []
    full_names = []
    icon_urls = []
    total_counts = []
    depleted_counts = []
    plus1_counts = []
    plus2_counts = []
    plus3_counts = []

    for d in dungeons:
        info = dungeon_lookup[str(d["dungeon_id"])]
        short_names.append(info["raiderio_short_name"])
        full_names.append(info["name"]["en_US"])
        # adjust path as‑needed
        icon_urls.append(f"/data/icons/{info['icon']}")

        total_counts.append(d["total_runs"])
        # find each tier (default 0)
        depleted_counts.append(d["depleted"])
        plus1_counts.append(d["upgrade_1"])
        plus2_counts.append(d["upgrade_2"])
        plus3_counts.append(d["upgrade_3"])

    # Build the Chart.js datasets
    datasets = [
        {
            "label": "Depleted",
            "data": depleted_counts,
            "backgroundColor": RARITY_COLORS["Depleted"],
            "stack": "Stack 0",
            "order": 0,
        },
        {
            "label": "+1",
            "data": plus1_counts,
            "backgroundColor": RARITY_COLORS["Uncommon"],
            "stack": "Stack 0",
            "order": 0,
        },
        {
            "label": "+2",
            "data": plus2_counts,
            "backgroundColor": RARITY_COLORS["Epic"],
            "stack": "Stack 0",
            "order": 0,
        },
        {
            "label": "+3",
            "data": plus3_counts,
            "backgroundColor": RARITY_COLORS["Legendary"],
            "stack": "Stack 0",
            "order": 0,
        },
    ]

    return {
        "labels": short_names,
        "fullNames": full_names,
        "iconUrls": icon_urls,
        "totalCounts": total_counts,
        "datasets": datasets,
    }


def assemble_spec_level_datasets(
    rows, spec_lookup, class_lookup, top_n, include_other=True
):
    """
    rows: list of dicts: {"spec_id": int, "keystone_level": int, "count": int}
    spec_lookup: dict keyed by string spec_id -> spec info (has 'name' and 'classID')
    class_lookup: dict keyed by string classID -> class info (has color.r/g/b)
    Returns: (key_levels_list, datasets_json_string)
      key_levels_list: sorted list of keystone levels (ints)
      datasets_json_string: JSON-serialized list of dataset objects ready for Chart.js
    """

    # normalize input rows -> map[level][spec] = count
    counts_by_level = defaultdict(lambda: defaultdict(int))
    total_by_spec = Counter()
    levels_set = set()

    for r in rows:
        spec_id = int(r["spec_id"])
        level = int(r["keystone_level"])
        cnt = int(r["count"])
        levels_set.add(level)
        counts_by_level[level][spec_id] += cnt
        total_by_spec[spec_id] += cnt

    if not levels_set:
        return [], json.dumps([])

    key_levels = sorted(levels_set)

    # pick top N specs by overall total
    top_specs = [s for s, _ in total_by_spec.most_common(top_n)]

    # Compute 'Other' if enabled
    all_spec_ids = set(total_by_spec.keys())
    other_specs = sorted(list(all_spec_ids - set(top_specs)))

    # build ordered list of specs to produce datasets for
    specs_order = sorted(
        top_specs,
        key=lambda s: (
            spec_lookup.get(str(s), {}).get("classID"),
            -total_by_spec.get(s, 0),
        ),
    )
    if include_other and other_specs:
        specs_order.append("OTHER")

    # precompute totals per level for denominator
    total_at_level = {lvl: sum(counts_by_level[lvl].values()) for lvl in key_levels}

    class_groups = defaultdict(list)
    for spec in specs_order:
        cid = str(spec_lookup[str(spec)]["classID"])
        class_groups[cid].append(spec)

    # Precompute shades per class
    spec_to_shade = {}
    for cid, group in class_groups.items():
        base = class_lookup[cid]["color"]
        count = len(group)
        shades = compute_shades((int(base["r"]), int(base["g"]), int(base["b"])), count)
        for i, spec in enumerate(group):
            spec_to_shade[str(spec)] = shades[i]
    datasets = []
    for spec in specs_order:
        label = None
        # compute raw counts array aligned with key_levels
        raw_counts = []
        for lvl in key_levels:
            if spec == "OTHER":
                # sum counts of other specs for this level
                c = sum(counts_by_level[lvl].get(sid, 0) for sid in other_specs)
            else:
                c = counts_by_level[lvl].get(spec, 0)
            raw_counts.append(c)

        # label + color
        if spec == "OTHER":
            label = "Other"
            backgroundColor = "rgba(180,180,180,0.6)"
        else:
            spec_str = str(spec)
            spec_info = spec_lookup.get(spec_str) or {}
            label = spec_info.get("name") or f"Spec {spec}"
            class_id = spec_info.get("classID")
            if class_id is None:
                # fallback gray
                backgroundColor = "rgba(150,150,150,0.7)"
            else:
                cls = class_lookup.get(str(class_id)) or {}
                color = spec_to_shade.get(
                    spec_str, cls.get("color", {"r": 150, "g": 150, "b": 150})
                )
                # guard numeric conversion
                r = int(color.get("r", 150))
                g = int(color.get("g", 150))
                b = int(color.get("b", 150))
                backgroundColor = f"rgba({r}, {g}, {b}, 0.8)"

        # compute percentages per level (if total_at_level is 0 => 0)
        data_pcts = []
        for i, lvl in enumerate(key_levels):
            denom = total_at_level.get(lvl, 0)
            if denom:
                pct = (raw_counts[i] / denom) * 100.0
            else:
                pct = 0.0
            # clamp to small decimals
            data_pcts.append(round(pct, 3))

        dataset = {
            "label": label,
            "data": data_pcts,  # percentages (for Chart.js)
            "rawCounts": raw_counts,  # parallel raw counts for tooltip
            "backgroundColor": backgroundColor,
            "borderWidth": 0,
        }
        datasets.append(dataset)

    return key_levels, json.dumps(datasets)


REGION_ORDER = ["us", "eu", "kr", "tw", "cn"]
REGION_COLORS = {
    "us": "#4A90E2",
    "eu": "#2DCE89",
    "kr": "#B37FEB",
    "tw": "#FB8C00",
    "cn": "#F5365C",
}


def _region_sort_key(region):
    region = region.lower()
    return (
        REGION_ORDER.index(region) if region in REGION_ORDER else len(REGION_ORDER),
        region,
    )


OVERALL_COLOR = "#ffffff"


def build_period_bounds(period_info):
    """
    Flatten data/static/periods.json into a {(region, period_id): (start, end)}
    lookup of static period boundaries (epoch ms).
    """
    bounds = {}
    for region, info in period_info.items():
        for p in info.get("periods", []):
            bounds[(region.lower(), int(p["id"]))] = (
                int(p["start_timestamp"]),
                int(p["end_timestamp"]),
            )
    return bounds


def _region_period_gaps(period_bounds, region_extents):
    """Regions whose latest run falls at/after the last season_period we know
    about — i.e. their current period is missing from season_periods/periods.json.

    In steady state the current (ongoing) period's end_timestamp is in the
    future, so a region's max run ts is < its max known period end and NOTHING
    is returned (the self-heal then makes zero network calls). A gap appears only
    when a region's current period is absent, e.g. KR/TW between the Wednesday
    getStaticData period fetch and their Thursday reset.
    """
    region_max_end = {}
    for (reg, _pid), (_start, end) in period_bounds.items():
        if end is not None:
            region_max_end[reg] = max(region_max_end.get(reg, end), end)
    gaps = []
    for reg, max_ts in region_extents.items():
        if max_ts is None:
            continue
        known_end = region_max_end.get(reg)
        if known_end is None or max_ts >= known_end:
            gaps.append(reg)
    return sorted(gaps, key=_region_sort_key)


def _rewrite_periods_json(healed):
    """Persist healed periods into data/static/periods.json so they survive to
    the next build (fail-soft, mirroring how the dungeon-icon self-heal rewrites
    npcs.json). A write failure only costs the persistence; the current render
    already used the in-memory period_bounds.
    """
    path = os.path.join(LOOKUP_DIR, "periods.json")
    try:
        data = load_json(path)
        for region, pid, start, end in healed:
            entry = data.setdefault(region, {"periods": []})
            periods = entry.setdefault("periods", [])
            if not any(int(p.get("id")) == pid for p in periods):
                periods.append(
                    {"id": pid, "start_timestamp": start, "end_timestamp": end}
                )
                periods.sort(key=lambda p: int(p["start_timestamp"]))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Self-heal: rewrote {path} with {len(healed)} healed period(s)")
    except Exception as e:
        print(f"WARNING: self-heal could not rewrite periods.json: {e}")


def heal_missing_periods(
    conn, cursor, season, period_bounds, region_extents, blizzard=None
):
    """On-demand, fetch-based self-heal for regions whose current season_period
    is missing (see _region_period_gaps for the root cause). Gated on a detected
    gap, so steady state makes zero network calls, exactly like the dungeon-icon
    self-heal gating on a missing set.

    For each gap region it fetches THIS season's period list from the Blizzard
    API, inserts any missing non-preseason period into season_periods, updates
    the in-memory ``period_bounds`` (critical: compute_key_throughput reads
    bounds from periods.json, not the DB), and rewrites periods.json. Returns the
    list of healed ``(region, period_id, start, end)`` so the caller can fold
    those regions' runs into the throughput data for this render.

    ``blizzard`` is injectable for tests; it defaults to the
    fetchSeasonAndPeriodInfo module (get_access_token / fetch_season_details /
    fetch_period_details / is_preseason_period). The CALLER wraps this in
    try/except: any failure (missing creds, API, network) leaves the render on
    the data it already has — never raises, never fails the build.
    """
    gaps = _region_period_gaps(period_bounds, region_extents)
    if not gaps:
        return []
    if blizzard is None:
        import fetchSeasonAndPeriodInfo as blizzard  # lazy: avoid import cost/creds in steady state

    token = blizzard.get_access_token()
    healed = []
    for region in gaps:
        detail = blizzard.fetch_season_details(region, season, token)
        season_start = detail["start_timestamp"]
        for p in detail.get("periods", []):
            pid = int(p["id"])
            if (region, pid) in period_bounds:
                continue
            per = blizzard.fetch_period_details(region, pid, token)
            if blizzard.is_preseason_period(per, season_start):
                continue
            start = int(per["start_timestamp"])
            end = int(per["end_timestamp"])
            databaseConnector.insert_season_periods(
                conn, cursor, region, pid, start, end, season
            )
            period_bounds[(region, pid)] = (start, end)
            healed.append((region, pid, start, end))
            print(f"Self-heal: added missing period {pid} for {region}")
    if healed:
        databaseConnector.commit_changes(conn)
        _rewrite_periods_json(healed)
    return healed


def _daily_throughput_series(rows, period_bounds, region_day_rows):
    """Per-region per-day "Key Throughput" chart lines for the season's first week.

    aggregated_key_throughput is keyed per (season, region, period) with only a
    run_count + max_ts, so it has no per-day grain and must not gain one. In the
    season's first week a single weekly point looks sparse, so the chart is drawn
    from raw per-(region, period, day) run counts (fetch_runs_per_region_day).
    The KPI headline still comes from the weekly rows (see compute_key_throughput);
    only the chart labels + series switch to day grain.

    The region set is taken from the aggregated `rows` (the SAME canonical set
    the weekly chart draws), NOT from the runs query, so the two charts never
    disagree on which regions appear. Each region is lined up with the exact
    period_id it carries in `rows`, and its day counts are pulled for that same
    period_id (aggregated_key_throughput and the runs query share one
    runs<->season_periods join, so a region present in `rows` resolves to day
    rows here). A region in `rows` with no day rows for its period draws an
    all-None (empty) line rather than being dropped, keeping the legend in step.

    Each region's day rate = region_day_count / day-minutes, with the day
    boundaries anchored at THAT region's own period start (numerator and
    denominator therefore share one region-relative clock). A completed day is
    1440 minutes; the region's last (ongoing) day is elapsed minutes (that
    region's latest run ts - the day's start). The Overall line sums the
    available per-region rates per day, exactly like the weekly Overall. Region
    colour/order reuse REGION_COLORS / _region_sort_key / OVERALL_COLOR. Fails
    loudly if a region's static period bounds are missing.

    Returns just {"labels", "series"}; the caller keeps the weekly KPI figures.
    """
    MS_PER_MIN = 60000.0
    DAY_MS = 86400000

    if not rows:
        return {"labels": [], "series": []}

    # per region (from the canonical aggregated rows): its current-week period
    # (max period_id) plus that region's latest observed run ts. period_bounds
    # reflects periods.json, which can already list a not-yet-started next
    # period, so the aggregated rows are what say which week we are actually in.
    latest_pid = {}
    latest_maxts = {}
    for r in rows:
        reg = r["region"].lower()
        pid = r["period_id"]
        if reg not in latest_pid or pid > latest_pid[reg]:
            latest_pid[reg] = pid
            latest_maxts[reg] = r["max_ts"]
        elif pid == latest_pid[reg]:
            latest_maxts[reg] = max(latest_maxts[reg], r["max_ts"])

    # day counts keyed by (region, period_id) so each region can be joined to the
    # exact period it carries in the aggregated rows.
    region_day_counts = defaultdict(dict)
    for rd in region_day_rows or []:
        reg = rd["region"].lower()
        region_day_counts[(reg, rd["period_id"])][rd["day"]] = rd["run_count"]

    # region set is the weekly (aggregated) set, so daily and weekly always agree
    regions = sorted(latest_pid, key=_region_sort_key)
    per_region_days = [
        region_day_counts.get((reg, latest_pid[reg]), {}) for reg in regions
    ]
    n_days = max((max(days) for days in per_region_days if days), default=0)
    if n_days == 0:
        return {"labels": [], "series": []}

    series = []
    for reg in regions:
        if (reg, latest_pid[reg]) not in period_bounds:
            raise ValueError(
                f"compute_key_throughput: no static period bounds for region "
                f"{reg!r} period {latest_pid[reg]}; cannot compute week-1 "
                "daily throughput rates."
            )
        start_ts = period_bounds[(reg, latest_pid[reg])][0]
        latest_ts = latest_maxts[reg]
        counts = region_day_counts.get((reg, latest_pid[reg]), {})
        # a region present in the aggregated rows but with no per-day runs rows
        # (a genuine runs/aggregate inconsistency) draws an all-None empty line.
        region_last_day = max(counts) if counts else 0
        data = []
        for day in range(1, n_days + 1):
            if day > region_last_day:
                data.append(None)  # region has no data this far into the week yet
                continue
            day_start = start_ts + (day - 1) * DAY_MS
            day_end = day_start + DAY_MS
            if latest_ts >= day_end:
                minutes = DAY_MS / MS_PER_MIN  # completed day
            else:
                minutes = (latest_ts - day_start) / MS_PER_MIN  # ongoing day so far
            count = counts.get(day, 0)
            data.append(round(count / minutes, 1) if minutes and minutes > 0 else None)
        series.append(
            {
                "region": reg.upper(),
                "color": REGION_COLORS.get(reg, "#9ca3af"),
                "data": data,
                "overall": False,
            }
        )

    # Overall line = sum of the available per-region rates per day (same
    # definition as the weekly Overall).
    overall = []
    for i in range(n_days):
        vals = [s["data"][i] for s in series if s["data"][i] is not None]
        overall.append(round(sum(vals), 1) if vals else None)
    series.insert(
        0,
        {"region": "Overall", "color": OVERALL_COLOR, "data": overall, "overall": True},
    )

    labels = [f"Day {d}" for d in range(1, n_days + 1)]
    return {"labels": labels, "series": series}


def compute_key_throughput(rows, period_bounds, now_ts=None, daily_region_rows=None):
    """
    Build the dashboard "Key Throughput" figures from the pre-aggregated
    per-(region, period) rows returned by fetch_key_throughput.

    Produces the headline keys-per-minute KPI (latest period combined + season
    average) and a per-region weekly time series (plus a combined "Overall"
    line) for the chart.

    Rate = run count / period length (minutes). The period bounds come from the
    static periods.json (`period_bounds`), so the denominator is the *actual*
    period length and the rate tracks the weekly key counts faithfully:
      * Completed period -> the full (end - start) span. A week is a week
        regardless of collection gaps, so a low-count week reads as a low rate
        instead of being inflated by a short observed run span.
      * Ongoing (current) period -> elapsed time so far (latest run - start),
        so a partially elapsed week isn't divided by a whole week.

    When `daily_region_rows` is supplied (season week 1, single week of data)
    ONLY the chart labels + series switch to the per-region per-day breakdown so
    the chart stays in step with the "Keys per Week" card. The KPI headline
    (current_total / season_total / delta_pct) is always the weekly number, so
    the "/min this week" figure is identical whether the chart is weekly or
    daily; see _daily_throughput_series.
    """
    MS_PER_MIN = 60000.0
    if now_ts is None:
        now_ts = datetime.now(timezone.utc).timestamp() * 1000.0

    def elapsed_min(r):
        """Minutes the period has actually been running (within observed data)."""
        bounds = period_bounds.get((r["region"].lower(), r["period_id"]))
        if not bounds:
            return None
        start, end = bounds
        if end is None or end > now_ts:
            # ongoing period: only count time up to the latest recorded run
            end = r["max_ts"]
        if start is None or end is None:
            return None
        span = (end - start) / MS_PER_MIN
        return span if span > 0 else None

    def rate(r):
        span = elapsed_min(r)
        if not r["run_count"] or not span:
            return None
        return round(r["run_count"] / span, 1)

    def combined_rate(subrows):
        # sum the per-region rates so each region is normalised by its own
        # (region-specific) reset schedule before being combined.
        vals = [rate(r) for r in subrows]
        vals = [v for v in vals if v is not None]
        return round(sum(vals), 1) if vals else 0.0

    if not rows:
        return {
            "current_total": 0.0,
            "season_total": 0.0,
            "delta_pct": 0.0,
            "labels": [],
            "series": [],
        }

    periods = sorted({r["period_id"] for r in rows})
    period_index = {pid: i for i, pid in enumerate(periods)}
    labels = [f"Week {i + 1}" for i in range(len(periods))]

    regions = sorted({r["region"] for r in rows}, key=_region_sort_key)

    series = []
    for region in regions:
        data = [None] * len(periods)
        for r in rows:
            if r["region"] == region:
                data[period_index[r["period_id"]]] = rate(r)
        series.append(
            {
                "region": region.upper(),
                "color": REGION_COLORS.get(region, "#9ca3af"),
                "data": data,
                "overall": False,
            }
        )

    # combined "Overall" line = sum of the available per-region rates per week
    overall = []
    for i in range(len(periods)):
        vals = [s["data"][i] for s in series if s["data"][i] is not None]
        overall.append(round(sum(vals), 1) if vals else None)
    series.insert(
        0,
        {"region": "Overall", "color": OVERALL_COLOR, "data": overall, "overall": True},
    )

    latest_period = periods[-1]
    current_total = combined_rate([r for r in rows if r["period_id"] == latest_period])
    season_rows_rate = [v for v in overall if v is not None]
    season_total = (
        round(sum(season_rows_rate) / len(season_rows_rate), 1)
        if season_rows_rate
        else 0.0
    )
    delta_pct = (
        round((current_total - season_total) / season_total * 100.0, 1)
        if season_total
        else 0.0
    )

    # Season week 1: keep the weekly KPI headline, but redraw the chart itself as
    # per-region daily lines.
    if daily_region_rows is not None:
        daily = _daily_throughput_series(rows, period_bounds, daily_region_rows)
        labels = daily["labels"]
        series = daily["series"]

    return {
        "current_total": current_total,
        "season_total": season_total,
        "delta_pct": delta_pct,
        "labels": labels,
        "series": series,
    }


def compute_patch_annotations(patches, period_bounds, region="us"):
    """
    Map patch releases (from data/static/patches.json) onto the weekly chart
    axis shared by "Keys per Week" and "Key Throughput".

    `first_seen_ts` is the CDN push of a patch's earliest retail build, which
    precedes go-live by a few days; since patches go live at a weekly reset,
    the go-live week is the first period starting at or after that timestamp.
    A line is drawn at the boundary before that week (index - 0.5), so
    everything right of the line is post-patch. Patches outside the season's
    periods (or landing on the season-start week) are skipped.
    """
    periods = sorted(
        (pid, start)
        for (r, pid), (start, _end) in period_bounds.items()
        if r == region
    )
    annotations = []
    for patch in patches:
        ts = patch.get("first_seen_ts")
        if ts is None:
            continue
        week_index = next(
            (i for i, (_pid, start) in enumerate(periods) if start >= ts), None
        )
        if not week_index:  # None (after season end / not live yet) or week 0
            continue
        # drop the expansion-wide major version ("12.0.5" -> "0.5"): it is the
        # same for every patch in a season and only clutters the small label
        label = patch["version"].split(".", 1)[1]
        annotations.append({"label": label, "x": week_index - 0.5})
    return annotations


def compute_completion_heatmap(rows):
    """
    Build the dashboard "When are keys completed?" heatmap data from the
    per-(region, day, hour) rows returned by fetch_completion_heatmap.

    Returns {"regions": [{"key", "label", "color"}, ...],
             "grids": {key: [168 ints]}} where grid index = day * 24 + hour
    (day 0=Sunday..6=Saturday, hour 0-23, all UTC — client JS rotates the
    cyclic 168-cell week into the viewer's local time). The "all" grid is the
    elementwise sum over regions so the client stays a dumb renderer; region
    entries are derived from the data so new regions appear automatically.
    """
    regions = sorted({r["region"] for r in rows}, key=_region_sort_key)
    grids = {"all": [0] * 168}
    for region in regions:
        grids[region] = [0] * 168
    for r in rows:
        idx = r["day"] * 24 + r["hour"]
        grids[r["region"]][idx] += r["count"]
        grids["all"][idx] += r["count"]
    region_meta = [{"key": "all", "label": "All", "color": OVERALL_COLOR}] + [
        {
            "key": region,
            "label": region.upper(),
            "color": REGION_COLORS.get(region, "#9ca3af"),
        }
        for region in regions
    ]
    return {"regions": region_meta, "grids": grids}


def main(template_path, output_dir):

    from image_generation.dungeon_popularity_ease import create_dungeon_popularity_vs_ease_img
    print("Generating Dashboard page...")
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    season_info = load_season_info(LOOKUP_DIR)
    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    dungeon_nav = generateDungeonNav(dungeon_lookup)

    template = env.get_template(os.path.basename(template_path))
    print("Fetching data from database...")
    current_season_id = int(season_info["blizzard_season_id"])
    # Static period bounds from periods.json (the self-heal below may add to this
    # in memory and rewrite the file). compute_key_throughput reads bounds from
    # here, not the DB.
    period_bounds = build_period_bounds(
        load_json(os.path.join(LOOKUP_DIR, "periods.json"))
    )
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.configure_read_session(conn, cursor)
        print("fetching runs...")
        longest_run = databaseConnector.fetch_longest_run(
            conn, cursor, current_season_id
        )
        highest_run = databaseConnector.fetch_max_key_run(
            conn, cursor, current_season_id
        )
        print("fetching spec run counts...")
        spec_run_counts = databaseConnector.fetch_spec_run_counts(
            conn, cursor
        )
        print("fetching spec run counts per level...")
        counts_per_level = databaseConnector.fetch_spec_run_counts_per_level(
            conn, cursor
        )
        print("fetching runs per period...")
        runs_per_period = databaseConnector.fetch_runs_per_period(
            conn, cursor, current_season_id
        )
        print("fetching dungeon run data...")
        dungeon_data = databaseConnector.fetch_runs_per_dungeon(
            conn, cursor, current_season_id
        )
        print("fetching dungeon runs per level...")
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, current_season_id
        )
        print("fetching spec upgrades...")
        spec_upgrades = databaseConnector.fetch_spec_upgrades(
            conn, cursor
        )
        print("fetching key throughput...")
        key_throughput_rows = databaseConnector.fetch_key_throughput(
            conn, cursor, current_season_id
        )
        # On-demand period self-heal (gated, fail-soft): if any region has runs
        # past its last known season_period (its current period is missing —
        # e.g. KR/TW between the Wednesday period fetch and their Thursday reset),
        # fetch the missing bounds from Blizzard so the region rejoins the runs.
        # Wrapped so any failure just renders with the data we already have.
        region_extents = databaseConnector.fetch_region_run_extent(
            conn, cursor, current_season_id
        )
        healed_periods = []
        try:
            healed_periods = heal_missing_periods(
                conn, cursor, current_season_id, period_bounds, region_extents
            )
        except Exception as heal_err:
            print(
                f"WARNING: period self-heal skipped (rendering with existing "
                f"data): {heal_err}"
            )
        if healed_periods:
            # the healed regions' late runs now join season_periods: refresh the
            # run-based series and inject the healed (region, period) throughput
            # rows in memory (no aggregated_key_throughput rebuild / RENAME).
            runs_per_period = databaseConnector.fetch_runs_per_period(
                conn, cursor, current_season_id
            )
            for region, pid, start, end in healed_periods:
                stats = databaseConnector.fetch_period_run_stats(
                    conn, cursor, current_season_id, region, start, end
                )
                if stats["run_count"]:
                    key_throughput_rows.append(
                        {
                            "region": region,
                            "period_id": pid,
                            "run_count": stats["run_count"],
                            "max_ts": stats["max_ts"],
                        }
                    )
        # Season week 1 (single week of data) redraws the throughput chart as
        # per-region daily lines. Fetch the raw per-(region, day) run counts only
        # then, so the normal 2+ week path pays for no extra scan. Same
        # single-week rule createKeysPerWeek uses to pick the "day" grain. Done
        # after the self-heal so healed periods are included.
        key_throughput_region_day_rows = None
        if len({r["week"] for r in runs_per_period}) == 1:
            print("fetching per-region daily runs (season week 1)...")
            key_throughput_region_day_rows = (
                databaseConnector.fetch_runs_per_region_day(
                    conn, cursor, current_season_id
                )
            )
        print("fetching completion heatmap...")
        completion_heatmap_rows = databaseConnector.fetch_completion_heatmap(
            conn, cursor, current_season_id
        )
    print("Creating Keys Per Week...")
    # grain ("week" vs the season-week-1 "day" breakdown) is decided here from
    # the per-(week, day) run counts and shared with the throughput chart below
    # so both cards stay in the same grain.
    period_datasets, period_labels, period_grain = createKeysPerWeek(runs_per_period)
    print("Computing key throughput...")
    generated_at = datetime.now(timezone.utc).timestamp()
    key_throughput = compute_key_throughput(
        key_throughput_rows,
        period_bounds,
        now_ts=generated_at * 1000.0,
        daily_region_rows=key_throughput_region_day_rows,
    )
    print("Computing patch annotations...")
    patch_list = load_json(os.path.join(LOOKUP_DIR, "patches.json"))
    patch_annotations = compute_patch_annotations(patch_list, period_bounds)
    print("Computing completion heatmap...")
    completion_heatmap = compute_completion_heatmap(completion_heatmap_rows)
    print("Assembling Spec Run Counts per Level...")
    key_levels, datasets_json = assemble_spec_level_datasets(
        counts_per_level,
        spec_lookup=spec_lookup,
        class_lookup=class_lookup,
        top_n=None,  # list all
        include_other=True,
    )
    print("Creating Spec Scatter...")
    scatter_data = create_spec_scatter(
        spec_upgrades, spec_lookup, class_lookup, highest_run
    )
    print("Creating Dungeon Popularity...")
    dungeon_chart = createDungeonPopularity(dungeon_data, dungeon_lookup)
    print("Creating Dungeon Ease...")
    ease_data = create_dungeon_ease(dungeon_runs_per_level, dungeon_lookup)
    runs = [
        {"name": "Longest", "data": longest_run, "icon": "hourglass_bottom"},
        {"name": "Highest", "data": highest_run, "icon": "leaderboard"},
    ]
    print("Rendering template...")

    output_html = template.render(
        trends=build_global_trends(),
        generated_at=generated_at,
        spec_nav=spec_nav,
        dungeon_nav=dungeon_nav,
        dungeon_lookup=dungeon_lookup,
        spec_lookup=spec_lookup,
        class_lookup=class_lookup,
        spec_run_counts=spec_run_counts,
        runs=runs,
        key_throughput=key_throughput,
        patch_annotations=patch_annotations,
        completion_heatmap=completion_heatmap,
        runs_per_period=runs_per_period,
        key_levels=key_levels,
        spec_run_counts_per_level=datasets_json,
        period_datasets=period_datasets,
        period_labels=period_labels,
        period_grain=period_grain,
        dungeon_labels=dungeon_chart["labels"],
        dungeon_full_names=dungeon_chart["fullNames"],
        dungeon_icon_urls=dungeon_chart["iconUrls"],
        dungeon_total_counts=dungeon_chart["totalCounts"],
        dungeon_datasets=dungeon_chart["datasets"],
        scatter_data=scatter_data,
        dungeon_ease_levels=ease_data["keyLevels"],
        dungeon_ease_datasets=ease_data["datasets"],
        breadcrumbs=[
            {"title": "Pages", "href": "/Pages"},
            {"title": "Dashboard", "href": "/Dashboard"},
        ],
        active_page="dashboard",
        notifications=notifications,
        season_info=season_info,
    )

    # Write output
    out_path = os.path.join(
        output_dir,
        "dashboard.html",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path}")
    print("Generating dungeon popularity vs ease image...")
    preview_path = os.path.join("assets", "img", "previews", "dungeon_popularity_across_keylevels.png")
    os.makedirs(os.path.dirname(preview_path), exist_ok=True)
    # dungeon_runs_per_level was already fetched for the page above
    create_dungeon_popularity_vs_ease_img(
        preview_path, current_season_id,
        dungeon_runs_per_level=dungeon_runs_per_level,
    )
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW Dashboard page")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    args = parser.parse_args()
    main(args.template, args.output_dir)
