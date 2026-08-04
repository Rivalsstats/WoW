"""Renderer for the spec tierlist image — the index page's weighted-performance
tierlist (lb_ci score + ckmeans binning via tierMath) drawn as an index-style
tierlist card. Replaces the old matplotlib popularity tierlist."""

import os
from contextlib import closing

import databaseConnector
from commonUtils import get_class_lookup, get_spec_lookup, humanize_number
from tierMath import build_spec_tiers
from image_generation import config
from image_generation.tierlist_card import TIER_LETTERS, render_tierlist_card


def _spec_entry(item, spec_lookup, class_lookup):
    sid = str(item["spec_id"])
    meta = spec_lookup.get(sid, {})
    class_meta = class_lookup.get(str(meta.get("classID", "")), {})
    col = class_meta.get("color", {})
    try:
        border = (int(col["r"]), int(col["g"]), int(col["b"]))
    except Exception:
        border = None
    return {
        "icon_path": os.path.join(config.ICON_DIR, f"{meta.get('SpellIconFileId')}.jpg"),
        "border": border,
        "name": f"{meta.get('name', '')} {class_meta.get('name', '')}".strip(),
    }


def create_spec_tierlist_img(out_path, season, spec_upgrades=None, total_runs=None):
    """Build the spec tierlist card; returns the post_data facts dict or None
    when there is no spec data. Tiers match the index page exactly.

    ``spec_upgrades`` / ``total_runs`` accept data the caller already fetched;
    anything left as None is fetched here."""
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()

    if spec_upgrades is None or total_runs is None:
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()
            if spec_upgrades is None:
                spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor)
            if total_runs is None:
                total_runs = databaseConnector.fetch_total_season_runs(conn, cursor, season)

    if not spec_upgrades:
        return None

    tiers_raw = build_spec_tiers(spec_lookup, class_lookup, spec_upgrades)
    tiers = {
        L: [_spec_entry(it, spec_lookup, class_lookup) for it in tiers_raw.get(L, [])]
        for L in TIER_LETTERS
    }

    render_tierlist_card(
        out_path,
        "Mythic+ Spec Tierlist",
        f"{humanize_number(total_runs)} runs analyzed  •  weighted by key level",
        tiers,
    )

    # best/worst by the same ordering the rows use (tiers are score-sorted)
    ordered = [e for L in TIER_LETTERS for e in tiers.get(L, [])]
    post_data = {
        "tierlist_type": "Spec Performance Tierlist",
        "best_spec": ordered[0]["name"] if ordered else "",
        "worst_spec": ordered[-1]["name"] if ordered else "",
        "total_runs": humanize_number(total_runs),
    }
    return post_data
