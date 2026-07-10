"""Renderer for the dungeon tier-list image (score-based tiers per dungeon)."""

import os
from collections import defaultdict
from contextlib import closing

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image

import databaseConnector
from commonUtils import get_dungeon_lookup, humanize_number
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import LANCZOS, watermark_file


def compute_dungeon_score_from_rows(dungeon_rows, w_depleted=-1, w_1=1, w_2=2, w_3=3):
    """
    dungeon_rows: iterable of rows returned by fetch_runs_per_dungeon_per_level
                  but only the rows for a single dungeon_id.
    Each row is a dict with keys:
      'keystone_level', 'upgrade_3', 'upgrade_2', 'upgrade_1', 'depleted', ...
    Returns: float score (same formula as original compute_dungeon_score).
    """
    total = 0.0
    for r in dungeon_rows:
        lvl = int(r["keystone_level"])
        depleted = int(r.get("depleted", 0))
        u1 = int(r.get("upgrade_1", 0))
        u2 = int(r.get("upgrade_2", 0))
        u3 = int(r.get("upgrade_3", 0))

        total += lvl * (w_depleted * depleted + w_1 * u1 + w_2 * u2 + w_3 * u3)
    return total


# === helper: build a DataFrame of dungeon scores and tier labels ===
def build_dungeon_scores_df(db_rows):
    """
    Build the same DataFrame as the previous build_dungeon_scores_df but using
    rows returned by fetch_runs_per_dungeon_per_level (flat rows per level).

    db_rows: list of dicts like returned by fetch_runs_per_dungeon_per_level
    Returns: pandas.DataFrame with columns ['id','count','score','tier']
    """
    if not db_rows:
        return pd.DataFrame(columns=["id", "count", "score", "tier"])

    # group rows by dungeon_id
    groups = defaultdict(list)
    for r in db_rows:
        groups[int(r["dungeon_id"])].append(r)

    rows = []
    for dungeon_id, rows_for_d in groups.items():
        score = compute_dungeon_score_from_rows(rows_for_d)
        # derive 'count' as total_runs summed across levels (similar to previous 'count')
        total_runs = sum(int(r.get("total_runs", 0)) for r in rows_for_d)
        rows.append({"id": dungeon_id, "count": total_runs, "score": score})

    df = pd.DataFrame(rows)

    # assign quantile‐based tiers (fallback to equal-width bins if qcut fails)
    try:
        df["tier"] = pd.qcut(df["score"], q=5, labels=["F", "C", "B", "A", "S"])
    except ValueError:
        # e.g. not enough unique values to form 5 quantiles -> use pd.cut fallback
        df["tier"] = pd.cut(df["score"], bins=5, labels=["F", "C", "B", "A", "S"])

    # order the categories (S highest)
    df["tier"] = pd.Categorical(
        df["tier"], categories=["S", "A", "B", "C", "F"], ordered=True
    )
    df = df.sort_values(["tier", "score"], ascending=[True, False])
    return df


def create_dungeon_tierlist_img(
    out_path, season, icon_size=0.4
):
    """
    Generates a horizontal tier-list of dungeons, one row per tier,
    placing each dungeon's spell-icon in the tier. Background is the
    dungeon art (faded). Saves to PNG and returns the post_data facts dict.
    """
    init_matplotlib()
    dungeon_lookup = get_dungeon_lookup()

    WIDTH, HEIGHT, DPI = config.WIDTH, config.HEIGHT, config.DPI

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )
        total_runs = databaseConnector.fetch_total_season_runs(conn, cursor, season)

    df = build_dungeon_scores_df(dungeon_data)

    fig, ax = plt.subplots(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

    tiers = ["S", "A", "B", "C", "F"]
    y_positions = {t: len(tiers) - 1 - i for i, t in enumerate(tiers)}
    x_offsets = {t: 0 for t in tiers}
    max_x = len(df) * (icon_size + 0.02)  # or simply: max(x_offsets.values())

    # draw backgrounds and tier labels
    for t in tiers:
        y = y_positions[t]
        sub = df[df["tier"] == t]
        if not sub.empty:
            did = str(sub.iloc[0]["id"])
            icon_file = dungeon_lookup[did]["icon"]
            bg = Image.open(os.path.join(config.ICON_DIR, icon_file))
            # fill full width
            w0, h0 = bg.size
            scale = WIDTH / w0
            bg = bg.resize((WIDTH, int(h0 * scale)), LANCZOS)
            band_h = HEIGHT / len(tiers)
            top = (bg.height - band_h) // 2
            bg = bg.crop((0, top, WIDTH, top + int(band_h)))
            ax.imshow(
                np.asarray(bg),
                extent=(0, max_x, y - 0.4, y + 0.4),
                aspect="auto",
                alpha=0.3,
                zorder=0,
            )
        ax.text(
            -0.05 * max_x,
            y,
            f"{t}-Tier",
            va="center",
            ha="right",
            fontsize=config.SMALL_SIZE,
            fontweight="bold",
            color=config.tier_colors[t],
            zorder=1,
        )
    # determine pixel size for icons
    fig_w, fig_h = fig.get_size_inches()
    icon_w_in = icon_size * fig_w
    icon_px = int(icon_w_in * DPI)
    border_w = max(1, icon_px // 20)

    for _, row in df.iterrows():
        t = row["tier"]
        y = y_positions[t]
        x = x_offsets[t]

        icon = Image.open(
            os.path.join(config.ICON_DIR, dungeon_lookup[str(row["id"])]["icon"])
        )
        icon = icon.resize((icon_px, icon_px))
        canv = Image.new(
            "RGBA", (icon_px + 2 * border_w, icon_px + 2 * border_w), (0, 0, 0, 0)
        )
        canv.paste(icon, (border_w, border_w))
        # inset_axes in FRACTIONAL units: [left, bottom, width, height]
        # left = x / max_x, bottom = (y - icon_size/2) / len(tiers)
        left_frac = x / max_x
        center_frac = (y + 0.5) / len(tiers)
        bottom_frac = center_frac - (icon_size / 2) / len(tiers)
        ax_ins = ax.inset_axes(
            [left_frac, bottom_frac, icon_size / max_x * max_x, icon_size / len(tiers)],
            transform=ax.transAxes,
            zorder=2,
        )
        ax_ins.imshow(canv)
        ax_ins.axis("off")

        dungeon_name = dungeon_lookup[str(row["id"])]["name"]["en_US"]
        # height for label: about 5% of the axes height
        text_h = 0.05
        label_gap = 0.02
        # inset for text: same left_frac, shifted down by text_h
        text_ins = ax.inset_axes(
            [
                left_frac,
                bottom_frac - text_h - label_gap,  # move below the icon
                icon_size,  # same width
                text_h,
            ],  # small height
            transform=ax.transAxes,
            zorder=2,
        )
        text_ins.text(
            0.5,
            1.0,  # x=center, y=top of this box
            dungeon_name,
            va="top",
            ha="center",
            color="white",
            fontsize=config.VERY_SMALL_SIZE,
            wrap=True,  # auto‑wrap if it’s long
        )
        text_ins.axis("off")

        x_offsets[t] += icon_size + 0.2

    # finalize
    ax.set_xlim(-0.05 * max_x, max_x + 0.02)
    ax.set_ylim(-0.5, len(tiers) - 0.5)
    ax.axis("off")
    plt.title("Dungeon Tier List", color="white", fontsize=config.SUBTITLE_SIZE, pad=20)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    # generate the social‐media post text
    best = df.iloc[0]
    worst = df.iloc[-1]
    second_best = (
        dungeon_lookup[str(df.iloc[1]["id"])]["name"]["en_US"] if len(df) > 1 else ""
    )
    second_worst = (
        dungeon_lookup[str(df.iloc[-2]["id"])]["name"]["en_US"] if len(df) > 2 else ""
    )
    post_data = {
        "tierlist_type": "Dungeon Tierlist",
        "best_dungeon": dungeon_lookup[str(best["id"])]["name"]["en_US"],
        "worst_dungeon": dungeon_lookup[str(worst["id"])]["name"]["en_US"],
        "second_best_dungeon": second_best,
        "second_worst_dungeon": second_worst,
        "total_runs": humanize_number(total_runs),
    }
    return post_data
