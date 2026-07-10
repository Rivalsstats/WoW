"""Renderer for the dungeon-popularity-across-keylevels stacked bar chart.
Also used as the dashboard page's OG preview image."""

from contextlib import closing

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import databaseConnector
from chartData import create_dungeon_ease
from commonUtils import get_dungeon_lookup
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import watermark_file


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
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

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
    ax.set_yticklabels([f"M + Level {lvl}" for lvl in key_levels], color="white")
    ax.invert_yaxis()  # highest level at top
    ax.set_xlim(0, 100)
    ax.set_xlabel("Share of Runs (%)", color="white")
    ax.set_title("Dungeon Popularity across Mythic+ Levels", color="white", pad=15)
    ax.set_xticklabels([])

    # legend outside
    ax.legend(
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
        labelcolor="white",
        fontsize=config.VERY_SMALL_SIZE,
    )

    plt.tight_layout(rect=[0, 0.08, 1, 1])

    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    # assemble OpenAI post data
    post_data = {
        "chart_type": "Dungeon Popularity across Keylevels",
        "levels_covered": len(key_levels),
        "top_dungeon": dungeon_names[0],
        "bottom_dungeon": dungeon_names[-1],
    }

    return post_data
