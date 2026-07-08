"""Sim DPS tierlist page generator.

Ranks every simulated spec (DPS and tanks separately; healers are never
simmed) by SimulationCraft DPS produced in the build pipeline's matrix sim jobs
(see generateSimcProfiles.py + .github/workflows/buildPages.yml). Every spec is
simmed in one batch on a single simc build at four target counts (1/3/5/8),
5-minute LightMovement fights, in two gear sets:

  * ``popular``  — the spec-page baseline set (most-popular items/enchants/gems
    + most-popular talents).
  * ``simcbis``  — the collector's Top-Gear rank-1 per-slot BiS set.

This reads the matrix jobs' ``json2`` outputs (one file per gear set × target
count, named ``sim_{gearset}_{targets}t.json``) from ``--sim_results_dir`` — no
database access — and renders one tab per target count with both gear sets shown
side by side per spec row.
"""

import os
import sys
import re
import glob
import json
import argparse
from datetime import datetime, timedelta, timezone
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pageGeneration import generateSpecNav, generateDungeonNav

# Static lookup dir (inlined so this generator needs no DB deps, only jinja2).
LOOKUP_DIR = "data/static"

# A page whose newest sim run is older than this many days is flagged as stale.
STALE_DAYS = int(os.environ.get("TIERLIST_STALE_DAYS", "14"))

ROLE_NAMES = {0: "Tank", 1: "Healer", 2: "Dps"}

# Target counts in tab order, and gear sets in per-row column order.
TARGET_ORDER = [1, 3, 5, 8]
GEAR_SETS = [("popular", "Popular"), ("simcbis", "SimC BIS")]

ACTOR_RE = re.compile(r"^spec(\d+)_([A-Za-z0-9]+)$")

# Tier letter by % behind the role's leader.
TIER_BOUNDS = [
    ("S", 2.5),
    ("A", 5.0),
    ("B", 10.0),
    ("C", 15.0),
    ("D", float("inf")),
]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def tier_for_pct_behind(pct):
    for letter, bound in TIER_BOUNDS:
        if pct <= bound:
            return letter
    return "D"


def parse_results(sim_results_dir):
    """Parse the matrix jobs' json2 outputs + meta sidecars.

    Returns (data, simc_version, simmed_at) where data is
    {target_count: {spec_id: {gearset: dps}}}.
    """
    data = {}
    simc_version = None
    simmed_at = None

    # Recursive so it works whether the artifacts are flattened into
    # sim_results/ (merge-multiple) or nested one directory per artifact.
    for path in sorted(glob.glob(os.path.join(sim_results_dir, "**", "*.json"), recursive=True)):
        base = os.path.basename(path)
        if base.startswith("meta") or base.startswith("manifest"):
            continue
        try:
            result = load_json(path)
        except Exception as e:
            print(f"WARN: could not read {base}: {e}", file=sys.stderr)
            continue

        sim = result.get("sim", {})
        # Target count: filename is authoritative (the job sets it); fall back
        # to the simmed desired_targets if the name is unexpected.
        targets = None
        m = re.search(r"_(\d+)t\.json$", base)
        if m:
            targets = int(m.group(1))
        if targets is None:
            try:
                targets = int(sim.get("options", {}).get("desired_targets"))
            except (TypeError, ValueError):
                print(f"WARN: {base} has no target count, skipping", file=sys.stderr)
                continue

        for player in sim.get("players", []):
            am = ACTOR_RE.match(player.get("name", ""))
            if not am:
                continue
            spec_id = int(am.group(1))
            gearset = am.group(2)
            try:
                dps = float(player["collected_data"]["dps"]["mean"])
            except (KeyError, TypeError, ValueError):
                continue
            data.setdefault(targets, {}).setdefault(spec_id, {})[gearset] = dps

        if simc_version is None:
            ver = result.get("version") or result.get("git_revision")
            if ver:
                simc_version = str(ver)[:64]

    for path in glob.glob(os.path.join(sim_results_dir, "**", "meta_*.json"), recursive=True):
        try:
            ts = datetime.fromisoformat(load_json(path).get("simmed_at"))
        except Exception:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if simmed_at is None or ts > simmed_at:
            simmed_at = ts

    return data, simc_version, simmed_at


def build_tab(spec_dps, spec_lookup, class_lookup):
    """Build one target-count tab: {"Dps": [row, ...], "Tank": [row, ...]}.

    spec_dps: {spec_id: {gearset: dps}} for this target count. Each spec's
    primary value (used for ranking and tier) is the best of its gear sets; each
    gear set keeps its own bar relative to the role leader.
    """
    grouped = {"Dps": [], "Tank": []}
    for spec_id, values in spec_dps.items():
        sdata = spec_lookup.get(str(spec_id))
        if not sdata:
            continue
        role_name = ROLE_NAMES.get(int(sdata.get("role", 2)))
        if role_name not in grouped:
            continue
        available = {gs: v for gs, v in values.items() if v}
        if not available:
            continue
        class_data = class_lookup.get(str(sdata.get("classID", "")), {})
        class_name = class_data.get("name", "Unknown")
        grouped[role_name].append(
            {
                "spec_id": spec_id,
                "name": sdata.get("name", "Unknown"),
                "class_name": class_name,
                "clean_class": class_name.replace(" ", ""),
                "icon": sdata.get("SpellIconFileId"),
                "url": f"/classes/{role_name}/{sdata.get('name')}_{class_name}",
                "values": values,
                "primary": max(available.values()),
            }
        )

    for role_name, rows in grouped.items():
        rows.sort(key=lambda x: x["primary"], reverse=True)
        leader = rows[0]["primary"] if rows else 0
        for rank, row in enumerate(rows, start=1):
            pct_behind = ((leader - row["primary"]) / leader * 100.0) if leader else 0.0
            row["rank"] = rank
            row["pct_behind"] = pct_behind
            row["tier"] = tier_for_pct_behind(pct_behind)
            row["bars"] = [
                {
                    "gearset": gs,
                    "label": label,
                    "dps": row["values"].get(gs),
                    "bar_pct": (row["values"][gs] / leader * 100.0)
                    if (leader and row["values"].get(gs)) else 0.0,
                }
                for gs, label in GEAR_SETS
            ]
    return grouped


def main(template_path, output_dir, sim_results_dir):
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))

    data, simc_version, simmed_at = parse_results(sim_results_dir)
    if not data:
        print(f"ERROR: no sim results parsed from {sim_results_dir}; not writing the page", file=sys.stderr)
        sys.exit(2)

    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))

    # total simulated specs per role, so the page can say "23 of 26 simmed"
    expected = {"Dps": 0, "Tank": 0}
    for sdata in spec_lookup.values():
        role_name = ROLE_NAMES.get(int(sdata.get("role", 2)))
        if role_name in expected:
            expected[role_name] += 1

    # One tab per target count that produced results, in canonical order.
    present = [t for t in TARGET_ORDER if t in data] + sorted(t for t in data if t not in TARGET_ORDER)
    tabs = []
    for targets in present:
        grouped = build_tab(data[targets], spec_lookup, class_lookup)
        if grouped["Dps"] or grouped["Tank"]:
            tabs.append({"targets": targets, "dps_rows": grouped["Dps"], "tank_rows": grouped["Tank"]})
    if not tabs:
        print("ERROR: sim results contained no known specs; not writing the page", file=sys.stderr)
        sys.exit(2)

    simmed_str = simmed_at.strftime("%Y-%m-%d") if simmed_at else ""
    is_stale = bool(
        simmed_at and simmed_at < datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)
    )

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template(os.path.basename(template_path))
    output_html = template.render(
        tabs=tabs,
        gear_sets=GEAR_SETS,
        expected_counts=expected,
        stale_days=STALE_DAYS,
        simc_version=simc_version,
        simmed_str=simmed_str,
        is_stale=is_stale,
        spec_lookup=spec_lookup,
        class_lookup=class_lookup,
        dungeon_lookup=dungeon_lookup,
        dungeon_nav=generateDungeonNav(dungeon_lookup),
        spec_nav=generateSpecNav(spec_lookup, class_lookup),
        season_info=season_info,
        active_page="tierlist",
        breadcrumbs=[
            {"title": "Pages", "href": "/pages"},
            {"title": "Sim DPS Tierlist", "href": "/pages/tierlist"},
        ],
        notifications=notifications,
        cur_page="tierlist",
    )

    out_path = os.path.join(output_dir, "tierlist.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path} ({len(tabs)} target tab(s), simc {simc_version}, simmed {simmed_str})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the sim DPS tierlist page")
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    parser.add_argument("--output_dir", required=True, help="Directory to write generated HTML pages")
    parser.add_argument("--sim_results_dir", required=True, help="Directory of matrix sim json2 outputs")
    args = parser.parse_args()

    main(args.template, args.output_dir, args.sim_results_dir)
