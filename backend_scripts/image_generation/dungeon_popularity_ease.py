"""Renderer for the dungeon-popularity-across-keylevels stacked bar chart.
Also used as the dashboard page's OG preview image."""

from contextlib import closing

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import databaseConnector
from chartData import create_dungeon_ease, create_dungeon_week_deltas
from commonUtils import get_dungeon_lookup
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import composite_chart_onto_bg, watermark_file

# trend colors matching the tier badge palette (readable on the dark bg)
TREND_UP_HEX = "#52d769"
TREND_DOWN_HEX = "#ff7b7b"


def _draw_week_trend_panel(fig, legend, dungeon_names, deltas_by_name, cmap):
    """Draw the week-over-week share-change rows in the free space below the
    legend: one row per dungeon in legend order (color swatch, trend triangle,
    signed delta). Coordinates are figure fractions; triangle/swatch sizes are
    specified in pixels and converted so they stay symmetric on the 16:9 canvas.
    Bebas Neue has no arrow glyphs, so the triangles are Polygon patches."""
    fig.canvas.draw()  # legend position is only known after a layout pass
    lg = legend.get_window_extent().transformed(fig.transFigure.inverted())

    x0 = lg.x0
    title_y = lg.y0 - 30 / config.HEIGHT
    rows_top = title_y - 30 / config.HEIGHT
    # keep clear of the watermark strip (bottom 8% + margin)
    row_h = min(
        24 / config.HEIGHT, (rows_top - 0.1) / max(len(dungeon_names), 1)
    )

    fig.text(
        x0,
        title_y,
        "Share change vs. last week",
        fontsize=config.VERY_SMALL_SIZE,
        color=config.TEXT_HEX,
        va="top",
    )

    sw_w, sw_h = 20 / config.WIDTH, 10 / config.HEIGHT
    tri_cx = x0 + (20 + 22) / config.WIDTH
    tri_sx, tri_sy = 5 / config.WIDTH, 4.5 / config.HEIGHT
    text_x = x0 + (20 + 40) / config.WIDTH

    for idx, name in enumerate(dungeon_names):
        delta = deltas_by_name.get(name)
        if delta is None:
            continue
        if delta == 0:
            delta = 0.0  # normalize -0.0 so it doesn't print as "-0.0%"
        cy = rows_top - idx * row_h
        fig.add_artist(
            mpatches.Rectangle(
                (x0, cy - sw_h / 2),
                sw_w,
                sw_h,
                transform=fig.transFigure,
                facecolor=cmap(idx % cmap.N),
                edgecolor="none",
            )
        )
        if delta > 0:
            col = TREND_UP_HEX
            pts = [
                (tri_cx - tri_sx, cy - tri_sy),
                (tri_cx + tri_sx, cy - tri_sy),
                (tri_cx, cy + tri_sy),
            ]
        elif delta < 0:
            col = TREND_DOWN_HEX
            pts = [
                (tri_cx - tri_sx, cy + tri_sy),
                (tri_cx + tri_sx, cy + tri_sy),
                (tri_cx, cy - tri_sy),
            ]
        else:
            col = config.MUTED_HEX
            pts = None
        if pts:
            fig.add_artist(
                mpatches.Polygon(
                    pts,
                    closed=True,
                    transform=fig.transFigure,
                    facecolor=col,
                    edgecolor="none",
                )
            )
        else:
            fig.add_artist(
                mpatches.Rectangle(
                    (tri_cx - tri_sx, cy - 1 / config.HEIGHT),
                    2 * tri_sx,
                    2 / config.HEIGHT,
                    transform=fig.transFigure,
                    facecolor=col,
                    edgecolor="none",
                )
            )
        fig.text(
            text_x,
            cy,
            f"{delta:+.1f}%",
            fontsize=config.VERY_SMALL_SIZE,
            color=col,
            va="center",
        )


def create_dungeon_popularity_vs_ease_img(out_path, season):
    """
    Creates and saves a stacked horizontal bar chart showing, for each Mythic+ level,
    the share of total runs completed in each dungeon (i.e. “ease”).
    """
    init_matplotlib()
    dungeon_lookup = get_dungeon_lookup()

    # --- prepare data ---
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )
        period_rows = databaseConnector.fetch_dungeon_timed_runs_last_two_periods(
            conn, cursor, season
        )

    # week-over-week share change per dungeon; None when there aren't two
    # comparable weeks yet, in which case the trend panel is skipped
    week_deltas = create_dungeon_week_deltas(period_rows)
    deltas_by_name = None
    if week_deltas:
        id_to_name = {
            str(did): info.get("name", {}).get("en_US", str(did))
            for did, info in dungeon_lookup.items()
        }
        deltas_by_name = {
            id_to_name.get(did, did): delta for did, delta in week_deltas.items()
        }

    ease_data = create_dungeon_ease(dungeon_runs_per_level, dungeon_lookup, None)
    key_levels = ease_data["keyLevels"]
    datasets = ease_data["datasets"]

    # build a DataFrame so we can sort and assign colors
    df = pd.DataFrame(datasets)
    # ensure consistent order
    df = df.set_index("label").loc[[d["label"] for d in datasets]].reset_index()
    dungeon_names = df["label"].tolist()
    pct_matrix = df["data"].tolist()

    # --- plotting ---
    fig, ax = plt.subplots(figsize=(config.WIDTH / config.DPI, config.HEIGHT / config.DPI), dpi=config.DPI)

    # stack each dungeon as a segment in each bar
    y_pos = np.arange(len(key_levels))
    left = np.zeros(len(key_levels))

    # pick a colormap
    cmap = plt.get_cmap("tab20")

    for idx, (dungeon, pct_vals) in enumerate(zip(dungeon_names, pct_matrix)):
        color = cmap(idx % cmap.N)
        ax.barh(
            y=y_pos, width=pct_vals, left=left, height=0.8, label=dungeon, color=color
        )
        left += np.array(pct_vals)

    # --- styling ---
    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"M + Level {lvl}" for lvl in key_levels])
    ax.invert_yaxis()  # highest level at top
    # no frame box, no dead bands above/below the outer bars
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_ylim(len(key_levels) - 0.5, -0.5)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Share of Runs (%)")
    ax.set_title("Dungeon Popularity across Mythic+ Levels", pad=15)
    ax.set_xticklabels([])

    # legend outside
    legend = ax.legend(
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
        labelcolor=config.MUTED_HEX,
        fontsize=config.VERY_SMALL_SIZE,
    )

    plt.tight_layout(rect=[0, 0.08, 1, 1])

    if deltas_by_name:
        _draw_week_trend_panel(fig, legend, dungeon_names, deltas_by_name, cmap)

    plt.savefig(out_path, transparent=True)
    plt.close(fig)

    composite_chart_onto_bg(out_path)
    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    # assemble OpenAI post data
    post_data = {
        "chart_type": "Dungeon Popularity across Keylevels",
        "levels_covered": len(key_levels),
        "top_dungeon": dungeon_names[0],
        "bottom_dungeon": dungeon_names[-1],
    }
    if deltas_by_name:
        riser = max(deltas_by_name, key=deltas_by_name.get)
        faller = min(deltas_by_name, key=deltas_by_name.get)
        post_data["weekly_share_riser"] = f"{riser} ({deltas_by_name[riser]:+.1f}%)"
        post_data["weekly_share_faller"] = f"{faller} ({deltas_by_name[faller]:+.1f}%)"

    return post_data
