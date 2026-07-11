"""Renderer for the spec popularity-vs-performance scatter plot.
Also used as the index page's OG preview image."""

from contextlib import closing

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from PIL import Image

import databaseConnector
from chartData import create_spec_scatter
from commonUtils import get_class_lookup, get_spec_lookup
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import composite_chart_onto_bg, parse_color, watermark_file


def create_spec_popularity_vs_performance_img(
    out_path, season
):
    """
    Generate and save/show a scatter plot of spec performance vs popularity,
    using spec icons as markers, reusing create_spec_scatter.
    If output_path is provided, saves the figure to that path.
    """
    init_matplotlib()
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor)
        highest_run = databaseConnector.fetch_max_key_run(conn, cursor, season)
    # get point data dicts with x, y, iconUrl, borderColor, backgroundColor
    raw_points = create_spec_scatter(
        spec_upgrades, spec_lookup, class_lookup, highest_run
    )

    # transform raw_points to local representation
    points = []
    for p in raw_points:
        # parse borderColor and backgroundColor strings 'rgba(r,g,b,a)'

        border = parse_color(p["borderColor"])
        face = parse_color(p["backgroundColor"])
        # convert icon URL to local file path (strip leading slash)
        icon_path = p["iconUrl"].lstrip("/")

        points.append(
            {
                "x": p["x"],
                "y": p["y"],
                "icon_path": icon_path,
                "edge_color": border,
                "face_color": face,
            }
        )

    # plotting

    fig, ax = plt.subplots(figsize=(config.WIDTH / config.DPI, config.HEIGHT / config.DPI), dpi=config.DPI)

    # draw each icon marker
    for p in points:
        try:
            img = Image.open(p["icon_path"]).convert("RGBA")
        except Exception:
            continue
        im = OffsetImage(np.array(img), zoom=0.2)
        ab = AnnotationBbox(
            im,
            (p["x"], p["y"]),
            frameon=True,
            bboxprops={
                "edgecolor": p["edge_color"],
                "facecolor": p["face_color"],
                "linewidth": 1.5,
                "boxstyle": "round,pad=0.2",
            },
        )
        ax.add_artist(ab)

    ax.set_xlabel("Performance")
    ax.set_ylabel("Popularity")
    ax.set_title("Spec Popularity vs Performance")
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.grid(True)

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]
    dx = (max(xs) - min(xs)) * 0.05
    dy = (max(ys) - min(ys)) * 0.05
    ax.set_xlim(min(xs) - dx, max(xs) + dx)
    ax.set_ylim(min(ys) - dy, max(ys) + dy)

    ys = np.array([p["y"] for p in raw_points])
    xs = np.array([p["x"] for p in raw_points])

    # fit a straight line: x ≈ m*y + b
    m = (xs @ ys) / (ys @ ys)
    b = 0.0

    # annotate each point with its residual
    for p in raw_points:
        expected = m * p["y"] + b
        p["residual"] = p["x"] - expected

    plt.tight_layout(rect=[0, 0.08, 1, 1])

    plt.savefig(out_path, transparent=True)

    plt.close(fig)

    composite_chart_onto_bg(out_path)
    watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10)

    most_overperforming = max(raw_points, key=lambda p: p["residual"])
    most_underperforming = min(raw_points, key=lambda p: p["residual"])
    post_data = {
        "chart_type": "Dungeon Popularity across Keylevels",
        "most_overperforming_spec": most_overperforming["label"],
        "most_underperforming_spec": most_underperforming["label"],
    }
    return post_data
