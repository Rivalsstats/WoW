"""Renderer for the overall spec-popularity tier list image."""

import os
from contextlib import closing

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

import databaseConnector
from commonUtils import get_class_lookup, get_dungeon_lookup, get_spec_lookup, humanize_number
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import LANCZOS, watermark_file


def create_overall_spec_popularity_img(out_path, season, icon_size=0.4):
    """
    Creates and saves a tierlist of total key counts per spec.
    Uses DB-returned rows from:
      - fetch_spec_upgrades(...) -> list of dicts with 'spec_id','keystone_level','total_runs',...
      - fetch_runs_per_dungeon_per_level(...) -> list of dicts with 'dungeon_id','keystone_level','total_runs',...
    Returns the post_data facts dict, or None when there is no spec data.
    """
    init_matplotlib()
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    dungeon_lookup = get_dungeon_lookup()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor)
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )

    # --- build spec-level totals from spec_upgrades rows ---
    if not spec_upgrades:
        # nothing to do
        return None

    # aggregate total runs per spec_id
    spec_totals = {}
    for r in spec_upgrades:
        sid = int(r["spec_id"])
        spec_totals[sid] = spec_totals.get(sid, 0) + int(r.get("total_runs", 0))

    df = pd.DataFrame(
        {
            "spec_id": list(spec_totals.keys()),
            "total_keys": list(spec_totals.values()),
        }
    )

    # assign tier bins using quantiles (same labels as before)
    df["tier"] = pd.cut(
        df["total_keys"],
        bins=[
            -1,
            df["total_keys"].quantile(0.2),
            df["total_keys"].quantile(0.4),
            df["total_keys"].quantile(0.6),
            df["total_keys"].quantile(0.8),
            df["total_keys"].max() + 1,
        ],
        labels=["F", "C", "B", "A", "S"],
    )
    tier_order = ["S", "A", "B", "C", "F"]
    df["tier"] = pd.Categorical(df["tier"], categories=tier_order, ordered=True)
    df = df.sort_values(["tier", "total_keys"], ascending=[True, False])

    # --- build dungeon DataFrame from dungeon_runs_per_level (for background icons) ---
    if not dungeon_runs_per_level:
        ddf = pd.DataFrame(columns=["id", "count"])
    else:
        d_groups = {}
        for r in dungeon_runs_per_level:
            did = int(r["dungeon_id"])
            d_groups[did] = d_groups.get(did, 0) + int(r.get("total_runs", 0))
        ddf = (
            pd.DataFrame([{"id": k, "count": v} for k, v in d_groups.items()])
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )

    # pick top dungeons → one per tier (same behavior as before: top N dungeons overall)
    d_by_tier = {}
    for i in range(min(len(tier_order), len(ddf))):
        d_by_tier[tier_order[i]] = str(ddf.loc[i, "id"])

    # prepare plot (rest of plotting code unchanged)
    DPI = 100
    fig, ax = plt.subplots(figsize=(config.WIDTH / DPI, config.HEIGHT / DPI), dpi=DPI)
    # dark background
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

    y_positions = {t: (len(tier_order) - 1 - i) for i, t in enumerate(tier_order)}
    x_offsets = {t: 0 for t in tier_order}

    # dynamic pixel sizing for icons & borders
    fig_w, fig_h = fig.get_size_inches()
    icon_width_in = icon_size * fig_w
    icon_px = int(icon_width_in * fig.dpi)
    border_width = max(1, int(icon_px * 0.075))
    dpi = fig.dpi
    width_px = int(fig_w * dpi)  # full figure width in px
    # each band spans 0.8 data-units; total data-units = len(tier_order)
    band_frac = 0.8 / len(tier_order)
    height_px = int(fig_h * dpi * band_frac)  # band height in px
    spacing = icon_size + 0.02
    count_by_tier = df["tier"].value_counts().to_dict()
    tier_widths = {t: count_by_tier.get(t, 0) * spacing for t in tier_order}
    max_x = max(tier_widths.values()) if tier_widths else 0

    # draw dungeon‐icon backdrops & tier labels
    for t in tier_order:
        y = y_positions[t]
        # full‐row background from dungeon icon
        if t in d_by_tier:
            did = d_by_tier[t]
            icon_file = dungeon_lookup[did]["icon"]
            bg_img = Image.open(os.path.join(config.ICON_DIR, icon_file))

            orig_w, orig_h = bg_img.size
            new_h = int(width_px * (orig_h / orig_w))
            # make sure height is at least the band height
            if new_h < height_px:
                new_h = height_px
            bg_resized = bg_img.resize((width_px, new_h), LANCZOS)

            # center‐crop vertically to exactly the band height
            top = (new_h - height_px) // 2
            bg_cropped = bg_resized.crop((0, top, width_px, top + height_px))

            ax.imshow(
                np.asarray(bg_cropped),
                extent=(0, max_x, y - 0.4, y + 0.4),
                aspect="auto",
                alpha=0.5,
                zorder=0,
            )
        # tier label
        ax.text(
            -0.02,
            y,
            f"{t}-Tier",
            va="center",
            ha="right",
            fontsize=config.SMALL_SIZE,
            fontweight="bold",
            color=config.tier_colors[t],
            zorder=1,
        )

    # plot each spec icon with padded border
    for _, row in df.iterrows():
        t = row["tier"]
        sid = str(row["spec_id"])
        y = y_positions[t]
        x = x_offsets[t]

        # get class color
        spec = spec_lookup[sid]
        cls_info = class_lookup[str(spec["classID"])]
        color_rgb = (
            int(cls_info["color"]["r"]),
            int(cls_info["color"]["g"]),
            int(cls_info["color"]["b"]),
        )

        # load & resize icon
        icon_path = os.path.join(config.ICON_DIR, f"{spec['SpellIconFileId']}.jpg")
        icon = Image.open(icon_path).convert("RGBA").resize((icon_px, icon_px))

        # padded canvas for full border
        canvas_s = icon_px + 2 * border_width
        canvas = Image.new("RGBA", (canvas_s, canvas_s), (0, 0, 0, 0))
        canvas.paste(icon, (border_width, border_width))

        # draw border
        draw = ImageDraw.Draw(canvas)
        draw.rectangle(
            [0, 0, canvas_s - 1, canvas_s - 1], outline=color_rgb, width=border_width
        )

        # inset axes (icon_size fraction wide)
        ax_ins = ax.inset_axes(
            [x, y - icon_size / 2, icon_size, icon_size],
            transform=ax.transData,
            zorder=2,
        )
        ax_ins.imshow(canvas)
        ax_ins.axis("off")

        x_offsets[t] += icon_size + 0.02

    # finalize layout
    max_x = max(x_offsets.values()) if x_offsets else 0
    ax.set_xlim(-0.5, max_x + 0.05)
    ax.set_ylim(-0.5, len(tier_order) - 0.5)
    ax.axis("off")
    plt.title(
        "Mythic+ Spec Popularity Tier List",
        color="white",
        fontsize=config.SUBTITLE_SIZE,
        pad=20,
    )
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    # prepare social post data using aggregated spec totals
    max_row = df.loc[df["total_keys"].idxmax()]
    min_row = df.loc[df["total_keys"].idxmin()]
    post_data = {
        "tierlist_type": "Spec Popularity Overall",
        "most_popular_spec": {
            "name": f"{spec_lookup[str(int(max_row['spec_id']))]['name']} {class_lookup[str(spec_lookup[str(int(max_row['spec_id']))]['classID'])]['name']}",
            "runs": humanize_number(int(max_row["total_keys"])),
        },
        "least_popular_spec": {
            "name": f"{spec_lookup[str(int(min_row['spec_id']))]['name']} {class_lookup[str(spec_lookup[str(int(min_row['spec_id']))]['classID'])]['name']}",
            "runs": humanize_number(int(min_row["total_keys"])),
        },
        "total_runs": humanize_number(
            sum(int(r.get("total_runs", 0)) for r in dungeon_runs_per_level)
        ),
    }
    return post_data
