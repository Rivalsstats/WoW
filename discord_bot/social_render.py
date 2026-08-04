"""Bridge to the site's ``image_generation`` social renderers.

The bot reuses the exact same renderers the website uses for its og:images, so the
spec/dungeon tierlists and the popularity-vs-performance scatter look identical to the
site. The renderers read spec/dungeon icon files from ``data/icons`` (which the bot
image doesn't ship — 82 MB / 18k files), so we lazily download just the icons a given
render needs from the site before drawing. DB data is fetched by the caller (via the
bot's read-session ``db.run``) and passed in, so nothing here touches the pool.

Each ``*_builder`` writes a PNG to ``out_path`` and is invoked inside
``charts.render`` (off the event loop, disk-cached per data cycle).
"""

import logging
import os
import urllib.request

from image_generation import config as ig_config
from image_generation.dungeon_tierlist import create_dungeon_tierlist_img
from image_generation.spec_popularity_performance import (
    create_spec_popularity_vs_performance_img,
)
from image_generation.spec_popularity_tierlist import create_spec_tierlist_img
from image_generation.tierlist_preview import generate_preview_image

from . import config, lookups

log = logging.getLogger("mythistone.bot")

ICON_DIR = ig_config.ICON_DIR  # "data/icons", relative to the app CWD


def _spec_icon_files():
    return [f"{m['SpellIconFileId']}.jpg" for m in lookups.SPECS.values() if m.get("SpellIconFileId")]


def _dungeon_icon_files():
    return [m["icon"] for m in lookups.DUNGEONS.values() if m.get("icon")]


def _ensure_icons(filenames):
    """Download any missing icon files from the site into ICON_DIR (best-effort)."""
    os.makedirs(ICON_DIR, exist_ok=True)
    for name in filenames:
        dest = os.path.join(ICON_DIR, name)
        if os.path.exists(dest):
            continue
        url = f"{config.SITE_BASE}/data/icons/{name}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = resp.read()
            tmp = dest + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, dest)
        except Exception:  # noqa: BLE001 - a missing icon degrades to a blank tile
            log.warning("social render: failed to fetch icon %s", name)


def scatter_builder(out_path, *, spec_upgrades, highest_run):
    _ensure_icons(_spec_icon_files())
    create_spec_popularity_vs_performance_img(
        out_path, config.SEASON, spec_upgrades=spec_upgrades, highest_run=highest_run
    )


def spec_tierlist_builder(out_path, *, spec_upgrades, total_runs):
    _ensure_icons(_spec_icon_files())
    create_spec_tierlist_img(
        out_path, config.SEASON, spec_upgrades=spec_upgrades, total_runs=total_runs
    )


def dungeon_tierlist_builder(out_path, *, dungeon_data, total_runs):
    _ensure_icons(_dungeon_icon_files())
    create_dungeon_tierlist_img(
        out_path, config.SEASON, dungeon_data=dungeon_data, total_runs=total_runs
    )


def simdps_builder(out_path, *, rows, targets):
    """Render the Sim DPS tierlist image (top-8 bars) for a target count.

    ``rows`` are the site's pre-computed, already-tiered DPS rows for that target
    count (from the published simdps_tierlist artifact), so tiers/ranks match the
    site exactly and no DB/clustering is needed here."""
    _ensure_icons(_spec_icon_files())
    generate_preview_image(rows, lookups.SPECS, lookups.CLASSES, config.SEASON_NAME, targets, out_path=out_path)
