import os
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from contextlib import closing
import aggregateData
import argparse
import databaseConnector
from collections import defaultdict, Counter
from pageGeneration import generateSpecNav
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
)

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)

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


def createKeysPerWeek(periods):

    # build labels: “1”, “2”, ….
    period_labels = [f"Week:{p['week']} " for p in periods]

    # pull out the raw counts
    total_counts = [p["total_runs"] for p in periods]
    depleted_counts = [p["depleted"] for p in periods]
    plus_one_counts = [p["upgrade_1"] for p in periods]
    plus_two_counts = [p["upgrade_2"] for p in periods]
    plus_three_counts = [p["upgrade_3"] for p in periods]

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
    return period_datasets, period_labels


def create_spec_scatter(spec_upgrades, spec_lookup, class_lookup, highest_key):
    """
    spec_upgrades: list of dicts:
      {"spec_id": int, "keystone_level": int, "upgrade_3": int, "upgrade_2": int,
       "upgrade_1": int, "depleted": int, "total_runs": int}
    spec_lookup: dict keyed by string spec_id -> spec metadata
    class_lookup: dict keyed by string classID -> class metadata (color etc.)
    highest_key: one row/dict that contains 'keystone_level' (max level)
    Returns: list of point dicts for scatter plot
    """
    # get max level from provided highest_key
    max_level = int(highest_key.get("keystone_level", 0))
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

        points.append({
            "label": sdata.get("name", f"Spec {spec_id}"),
            "x": round(perf, 4),
            "y": runs,
            "iconUrl": icon_url,
            "borderColor": border,
            "backgroundColor": bg,
        })

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
        # prefer the provided total_runs column
        cnt = int(r.get("total_runs", 0))

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
            denom = total_by_level.get(lvl, 0) or 1  # avoid div0; if denom==0 results will be 0
            pct = round((cnt / denom) * 100.0, 1) if denom else 0.0
            pct_data.append(pct)

        datasets.append({
            "label": name,
            "data": pct_data,
            "rawCounts": raw_counts
        })

    return {"keyLevels": key_levels, "datasets": datasets}

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
        depleted_counts.append(d['depleted'])
        plus1_counts.append(d['upgrade_1'])
        plus2_counts.append(d['upgrade_2'])
        plus3_counts.append(d['upgrade_3'])

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

def assemble_spec_level_datasets(rows, spec_lookup, class_lookup, top_n, include_other=True):
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
        key=lambda s: (spec_lookup.get(str(s), {}).get("classID"), -total_by_spec.get(s, 0))
    )
    if include_other and other_specs:
        specs_order.append("OTHER") 

    # precompute totals per level for denominator
    total_at_level = { lvl: sum(counts_by_level[lvl].values()) for lvl in key_levels }

    class_groups = defaultdict(list)
    for spec in specs_order:
        cid = str(spec_lookup[str(spec)]["classID"])
        class_groups[cid].append(spec)

    # Precompute shades per class
    spec_to_shade = {}
    for cid, group in class_groups.items():
        base = class_lookup[cid]["color"]
        count = len(group)
        shades = compute_shades(
            (int(base["r"]), int(base["g"]), int(base["b"])), count
        )
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
                color = spec_to_shade.get(spec_str, cls.get("color", {"r": 150, "g": 150, "b": 150}))
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
            "data": data_pcts,            # percentages (for Chart.js)
            "rawCounts": raw_counts,      # parallel raw counts for tooltip
            "backgroundColor": backgroundColor,
            "borderWidth": 0,
        }
        datasets.append(dataset)

    return key_levels, json.dumps(datasets)


def main(template_path, output_dir):
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
    spec_nav = generateSpecNav(spec_lookup, class_lookup) 



    
    template = env.get_template(os.path.basename(template_path))
    print("Fetching data from database...")
    access_token = aggregateData.get_access_token(os.environ['CLIENT_ID'], os.environ['CLIENT_SECRET'])
    current_season_id = aggregateData.get_current_season_id(access_token)
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        print("fetching runs...")
        shortest_run = databaseConnector.fetch_shortest_run(conn, cursor, current_season_id)
        longest_run = databaseConnector.fetch_longest_run(conn, cursor, current_season_id)
        highest_run = databaseConnector.fetch_max_key_run(conn, cursor, current_season_id)
        print("fetching spec run counts...")
        spec_run_counts = databaseConnector.fetch_spec_run_counts(conn,cursor,current_season_id)
        print("fetching spec run counts per level...")
        counts_per_level = databaseConnector.fetch_spec_run_counts_per_level(conn, cursor, current_season_id)
        print("fetching runs per period...")
        runs_per_period = databaseConnector.fetch_runs_per_period(conn, cursor, current_season_id)
        print("fetching dungeon run data...")
        dungeon_data = databaseConnector.fetch_runs_per_dungeon(conn, cursor, current_season_id)
        print("fetching dungeon runs per level...")
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, current_season_id)
        print("fetching spec upgrades...")
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor, current_season_id)
    print("Assembling Spec Run Counts per Level...")
    key_levels, datasets_json = assemble_spec_level_datasets(
        counts_per_level,
        spec_lookup=spec_lookup,
        class_lookup=class_lookup,
        top_n=None, #list all
        include_other=True
    )
    print("Creating Spec Scatter...")
    scatter_data = create_spec_scatter(spec_upgrades, spec_lookup, class_lookup, highest_run)
    print("Creating Keys Per Week...")
    period_datasets, period_labels = createKeysPerWeek(runs_per_period)
    print("Creating Dungeon Popularity...")
    dungeon_chart = createDungeonPopularity(dungeon_data, dungeon_lookup)
    print("Creating Dungeon Ease...")
    ease_data = create_dungeon_ease(dungeon_runs_per_level, dungeon_lookup)
    runs = [
        {"name": "Shortest", "data": shortest_run, "icon": "sprint"},
        {"name": "Longest",  "data": longest_run, "icon": "hourglass_bottom"},
        {"name": "Highest",  "data": highest_run, "icon": "leaderboard"},
    ]
    print("Rendering template...")
    
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
        dungeon_lookup=dungeon_lookup,
        spec_lookup=spec_lookup,
        class_lookup=class_lookup,
        spec_run_counts=spec_run_counts,
        runs=runs,
        runs_per_period=runs_per_period,
        key_levels=key_levels,
        spec_run_counts_per_level=datasets_json,
        period_datasets=period_datasets,
        period_labels=period_labels,
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
            {"title": "Dashboard", "href": "/Dashboard"}
        ],
        active_page="dashboard",
        notifications=notifications,
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WoW Dashboard page")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    args = parser.parse_args()
    main(args.template, args.output_dir)
