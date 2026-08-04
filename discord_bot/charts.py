"""Matplotlib chart renderers for the genuinely 2-D visualisations.

Only used where a unicode bar can't carry the data (weekly lines, level
distribution). Each render is disk-cached under the nightly ``data_date`` key, so a
given chart is drawn at most once per data cycle and served warm afterwards. The
palette mirrors image_generation/config.py so charts look like the site. (The spec
scatter and the tierlists are drawn by the shared ``image_generation`` renderers via
``social_render``, not here.)
"""

import asyncio
import os
import time

import matplotlib

matplotlib.use("Agg")  # headless backend; must precede pyplot import

import matplotlib.pyplot as plt  # noqa: E402

from . import cache  # noqa: E402

BG_HEX = "#11151e"
TEXT_HEX = "#e9ecf2"
MUTED_HEX = "#969eac"
DIVIDER_HEX = "#303746"
SIZE = (10.8, 6.075)  # 1080x608 at DPI 100 — Discord-friendly
DPI = 100

_render_locks: dict[str, asyncio.Lock] = {}


def _style(ax, title=None):
    ax.set_facecolor(BG_HEX)
    for spine in ax.spines.values():
        spine.set_color(DIVIDER_HEX)
    ax.tick_params(colors=MUTED_HEX, labelsize=9)
    ax.grid(True, color=DIVIDER_HEX, linestyle="--", alpha=0.5)
    if title:
        ax.set_title(title, color=TEXT_HEX, fontsize=14, pad=12)


def _new_fig():
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    fig.patch.set_facecolor(BG_HEX)
    return fig, ax


def _save(fig, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + ".tmp"
    fig.savefig(tmp, format="png", facecolor=BG_HEX, bbox_inches="tight")
    plt.close(fig)
    os.replace(tmp, out_path)


def cached_path(chart: str, params_slug: str):
    """The already-rendered chart path for this data cycle, or None if not yet drawn.

    Lets a command skip its DB fetch entirely when the image is still warm."""
    path = cache.chart_path(chart, params_slug)
    return path if os.path.exists(path) else None


async def render(chart: str, params_slug: str, builder) -> str:
    """Return a cached PNG path, rendering it (off the event loop) if missing."""
    path = cache.chart_path(chart, params_slug)
    if os.path.exists(path):
        return path
    lock = _render_locks.setdefault(path, asyncio.Lock())
    async with lock:
        if os.path.exists(path):
            return path
        await asyncio.to_thread(builder, path)
        return path


# --- sync builders (pre-fetched data in; no DB access) ---------------------
def build_keys_per_week(rows, periods, out_path):
    by_region = {}
    for r in rows:
        by_region.setdefault(r["region"], []).append(r)
    fig, ax = _new_fig()
    _style(ax, "Keys completed per week")
    for region, region_rows in sorted(by_region.items()):
        region_rows.sort(key=lambda r: r["period_id"])
        xs = list(range(1, len(region_rows) + 1))
        ys = [r["run_count"] for r in region_rows]
        ax.plot(xs, ys, marker="o", label=region.upper())
    ax.set_xlabel("Week", color=MUTED_HEX)
    ax.set_ylabel("Runs", color=MUTED_HEX)
    legend = ax.legend(loc="upper left", facecolor=BG_HEX, edgecolor=DIVIDER_HEX)
    for text in legend.get_texts():
        text.set_color(TEXT_HEX)
    _save(fig, out_path)


def build_key_throughput(rows, period_info, out_path, now_ts=None):
    """Keys-per-minute per region over the season (mirrors the dashboard's Key
    Throughput chart): run_count / period length, where a completed period uses its
    full (end-start) span and the ongoing period uses elapsed time to the latest run."""
    MS_PER_MIN = 60000.0
    if now_ts is None:
        now_ts = time.time() * 1000.0
    # periods.json is keyed by region -> {periods: [{id, start_timestamp, end_timestamp}]}.
    bounds = {}
    for region, info in period_info.items():
        for p in info.get("periods", []):
            bounds[(region.lower(), int(p["id"]))] = (int(p["start_timestamp"]), int(p["end_timestamp"]))
    by_region = {}
    for r in rows:
        by_region.setdefault(r["region"], []).append(r)
    fig, ax = _new_fig()
    _style(ax, "Key throughput (keys / minute)")
    for region, region_rows in sorted(by_region.items()):
        region_rows.sort(key=lambda r: r["period_id"])
        xs, ys = [], []
        for i, r in enumerate(region_rows, start=1):
            b = bounds.get((r["region"].lower(), r["period_id"]))
            if not b or not r["run_count"]:
                continue
            start, end = b
            if end > now_ts:  # ongoing period: only count time up to the latest run
                end = r["max_ts"]
            span = (end - start) / MS_PER_MIN
            if span <= 0:
                continue
            xs.append(i)
            ys.append(round(r["run_count"] / span, 1))
        if xs:
            ax.plot(xs, ys, marker="o", label=region.upper())
    ax.set_xlabel("Week", color=MUTED_HEX)
    ax.set_ylabel("Keys / minute", color=MUTED_HEX)
    legend = ax.legend(loc="upper left", facecolor=BG_HEX, edgecolor=DIVIDER_HEX)
    for text in legend.get_texts():
        text.set_color(TEXT_HEX)
    _save(fig, out_path)
