"""Renderer for the spec popularity-vs-performance scatter plot.
Also used as the index page's OG preview image."""

from contextlib import closing

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
from PIL import Image

import databaseConnector
from chartData import create_spec_scatter
from commonUtils import get_class_lookup, get_spec_lookup
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.pil_helpers import composite_chart_onto_bg, parse_color, watermark_file


def create_spec_popularity_vs_performance_img(
    out_path, season, spec_upgrades=None, highest_run=None
):
    """
    Generate and save/show a scatter plot of spec performance vs popularity,
    using spec icons as markers, reusing create_spec_scatter.
    If output_path is provided, saves the figure to that path.

    `spec_upgrades` / `highest_run` accept data the caller already fetched;
    anything left as None is fetched here.
    """
    init_matplotlib()
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()

    if spec_upgrades is None or highest_run is None:
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()
            if spec_upgrades is None:
                spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor)
            if highest_run is None:
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

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]

    avg_perf = sum(xs) / len(xs) if xs else 0.0

    def format_perf(val, _pos):
        if not avg_perf:
            return "0%"
        return f"{(val - avg_perf) / avg_perf * 100:+.0f}%"

    def format_runs(val, _pos):
        if val >= 1e9:
            return f"{val / 1e9:.1f}B"
        if val >= 1e6:
            return f"{val / 1e6:.1f}M"
        if val >= 1e3:
            return f"{val / 1e3:.1f}k"
        return f"{val:.0f}"

    ax.set_xlabel("Performance vs Average")
    ax.set_ylabel("Runs")
    ax.set_title("Spec Popularity vs Performance")
    ax.xaxis.set_major_formatter(FuncFormatter(format_perf))
    ax.yaxis.set_major_formatter(FuncFormatter(format_runs))
    ax.grid(True)

    # Compressive X scale: GAMMA < 1 spreads specs close to the average and
    # squeezes the extreme performers toward the edges. Tune here.
    GAMMA = 0.5

    def _x_forward(x):
        d = np.asarray(x, dtype=float) - avg_perf
        return avg_perf + np.sign(d) * np.abs(d) ** GAMMA

    def _x_inverse(p):
        d = np.asarray(p, dtype=float) - avg_perf
        return avg_perf + np.sign(d) * np.abs(d) ** (1.0 / GAMMA)

    ax.set_xscale("function", functions=(_x_forward, _x_inverse))

    # Keep 0% (the average) dead center: symmetric x-limits around avg_perf.
    # Limits are in real-x space; the signed-power scale is odd, so the
    # transformed extent stays symmetric and avg_perf lands in the center.
    maxdev = max((abs(x - avg_perf) for x in xs), default=0.0)
    if maxdev <= 0:
        maxdev = abs(avg_perf) or 1.0
    xpad = maxdev * 0.05
    lo_x = avg_perf - maxdev - xpad
    hi_x = avg_perf + maxdev + xpad
    ax.set_xlim(lo_x, hi_x)

    # Explicit ticks at readable percentages of avg; the FuncFormatter turns
    # each real-x position back into its true signed percent label.
    nice_pcts = [-100, -50, -25, -10, -5, 0, 5, 10, 25, 50, 100]
    tick_positions = [
        avg_perf * (1 + pct / 100.0)
        for pct in nice_pcts
        if lo_x <= avg_perf * (1 + pct / 100.0) <= hi_x
    ]
    ax.set_xticks(tick_positions)
    # set_xscale reset the major formatter, so re-apply the percent labels.
    ax.xaxis.set_major_formatter(FuncFormatter(format_perf))

    # Compressive Y (run count) scale: log spreads the crowded low/mid-run
    # band so icons overlap less. Runs are strictly positive for plotted specs.
    min_run = min(ys)
    max_run = max(ys)
    if min_run <= 0:
        min_run = min((y for y in ys if y > 0), default=1.0)
    ax.set_yscale("log")
    ax.set_ylim(min_run / 1.2, max_run * 1.2)

    # set_yscale reset the formatter; re-apply human-readable run labels and
    # place explicit ticks at nice round run counts within range (no minors).
    ax.yaxis.set_major_formatter(FuncFormatter(format_runs))
    nice_runs = [
        1e2, 2e2, 5e2, 1e3, 2e3, 5e3, 1e4, 2e4, 5e4, 1e5, 2e5, 5e5, 1e6,
    ]
    run_ticks = [r for r in nice_runs if min_run / 1.2 <= r <= max_run * 1.2]
    ax.yaxis.set_major_locator(FixedLocator(run_ticks))
    ax.yaxis.set_minor_locator(NullLocator())

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
