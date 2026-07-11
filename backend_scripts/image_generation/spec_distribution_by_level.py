"""Renderer for the spec-distribution-across-keylevels stacked bar chart."""

import os
from contextlib import closing

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.offsetbox import AnnotationBbox, OffsetImage

import databaseConnector
from chartData import compute_shades
from commonUtils import get_class_lookup, get_spec_lookup
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import composite_chart_onto_bg, watermark_file


def create_spec_popularity_by_level_img(out_path, season):
    """
    Creates and saves a stacked horizontal bar chart of total key counts per spec,
    split by upgrade tier (depleted, upgrade_1, upgrade_2, upgrade_3).

    Uses rows returned by fetch_spec_upgrades:
      [{"spec_id","keystone_level","upgrade_3","upgrade_2","upgrade_1","depleted","total_runs"}, ...]
    Returns the post_data facts dict.
    """
    init_matplotlib()
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor)

    # build records from DB rows (one row per spec-level)
    records = []
    for r in spec_upgrades:
        sid = int(r["spec_id"])
        records.append(
            {
                "level": int(r["keystone_level"]),
                "spec_id": sid,
                "spec_name": spec_lookup[str(sid)]["name"],
                "count": int(r.get("total_runs", 0)),
            }
        )

    df = pd.DataFrame.from_records(records)
    if df.empty:
        raise ValueError("No key data found in spec_upgrades")

    # group specs by class for color shades (use spec ids present in df for stability)
    specs_seen = sorted(set(df["spec_id"].tolist()))
    specs_by_class = {}
    for sid in specs_seen:
        cid = spec_lookup[str(sid)]["classID"]
        specs_by_class.setdefault(cid, []).append(sid)

    color_map = {}
    for cid, sids in specs_by_class.items():
        base = class_lookup[str(cid)]["color"]
        rgb_base = (int(base["r"]), int(base["g"]), int(base["b"]))
        shades = compute_shades(rgb_base, len(sids))
        for sid, shade in zip(sorted(sids), shades):
            color_map[sid] = (shade["r"] / 255, shade["g"] / 255, shade["b"] / 255)

    # pivot to level x spec table (counts), then convert to per-level pct
    pivot = df.pivot_table(
        index="level", columns="spec_id", values="count", aggfunc="sum", fill_value=0
    ).sort_index()
    pct = pivot.div(pivot.sum(axis=1).replace(0, 1), axis=0)  # avoid division by zero

    # order specs by classID then spec id (missing classID -> sort last)
    def _class_sort_key(sid):
        cid = spec_lookup.get(str(sid), {}).get("classID")
        return (cid if cid is not None else 10**9, sid)

    ordered = sorted(pivot.columns.tolist(), key=_class_sort_key)
    pct = pct[ordered]

    DPI = config.DPI
    fig, ax = plt.subplots(
        figsize=(config.WIDTH / DPI, config.HEIGHT / DPI),
        dpi=DPI,
    )

    # build color list for ordered specs (fallback gray if missing)
    colors = [color_map.get(sid, (0.6, 0.6, 0.6)) for sid in ordered]

    pct.plot(
        kind="barh",
        stacked=True,
        width=0.8,
        color=colors,
        legend=False,
        linewidth=0,
        ax=ax,
    )

    ax.set_ylabel("Keystone Level")
    ax.set_title("Spec Distribution across Keylevels")
    ax.set_xlim(0, 1)
    ax.set_xticklabels([])

    yticks = ax.get_yticks()
    bar_height = 0.8
    y0 = yticks[0] - bar_height / 2 - 0.05

    # icons along the bottom: evenly spaced across the width
    N = len(ordered)
    x_fracs = [i / (N - 1) if N > 1 else 0.5 for i in range(N)]
    for x_frac, sid in zip(x_fracs, ordered):
        icon_file = os.path.join(
            config.ICON_DIR, f"{spec_lookup[str(sid)]['SpellIconFileId']}.jpg"
        )
        arr_img = plt.imread(icon_file)
        im = OffsetImage(arr_img, zoom=0.35)
        ab = AnnotationBbox(
            im,
            (x_frac, y0),
            xycoords=("axes fraction", "data"),
            box_alignment=(0.5, 1),
            frameon=False,
        )
        ax.add_artist(ab)

    plt.tight_layout(rect=[0, 0.08, 1, 1])

    plt.savefig(out_path, transparent=True)
    plt.close()

    composite_chart_onto_bg(out_path)
    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    # prepare social post data: find highest keylevel and top specs at that level
    if spec_upgrades:
        max_keylevel = max(int(r["keystone_level"]) for r in spec_upgrades)
    else:
        max_keylevel = None

    top_specs = []
    if max_keylevel is not None:
        # total runs per spec at the max level
        runs_at_max = {}
        for r in spec_upgrades:
            if int(r["keystone_level"]) == max_keylevel:
                sid = int(r["spec_id"])
                runs_at_max[sid] = runs_at_max.get(sid, 0) + int(r.get("total_runs", 0))
        for sid, cnt in runs_at_max.items():
            top_specs.append(
                {
                    "specName": spec_lookup[str(sid)]["name"],
                    "className": class_lookup[str(spec_lookup[str(sid)]["classID"])][
                        "name"
                    ],
                    "count": cnt,
                }
            )
        top_specs = sorted(top_specs, key=lambda s: s["count"], reverse=True)[:3]

    post_data = {
        "tierlist_type": "Spec Popularity by Keylevel",
        "highest_keylevel": max_keylevel,
        "highest_specs": [
            f"{spec['specName']} - {spec['className']}" for spec in top_specs
        ],
    }
    return post_data
