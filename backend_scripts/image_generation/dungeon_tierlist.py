"""Renderer for the dungeon tierlist image — same lb_ci + ckmeans tiers as the
index page's dungeon tierlist (via tierMath), drawn as an index-style tierlist
card with dungeon icon tiles. Replaces the old matplotlib score/quantile one."""

import os
from contextlib import closing

import databaseConnector
from commonUtils import get_dungeon_lookup, humanize_number
from tierMath import build_ckmeans_tiers
from image_generation import config
from image_generation.tierlist_card import TIER_LETTERS, render_tierlist_card


def _dungeon_entry(item, dungeon_lookup, highest_keys):
    meta = dungeon_lookup.get(str(item["dungeon_id"]), {})
    name = meta.get("name", {})
    label = name.get("en_US", f"Dungeon {item['dungeon_id']}") if isinstance(name, dict) else str(name)
    icon = meta.get("icon")
    # build_ckmeans_tiers keeps the aggregated upgrade/total counts on the item
    total = int(item.get("total_runs", 0))
    timed = sum(int(item.get(k, 0)) for k in ("upgrade_1", "upgrade_2", "upgrade_3"))
    highest = highest_keys.get(int(item["dungeon_id"]), 0)
    return {
        "icon_path": os.path.join(config.ICON_DIR, icon) if icon else None,
        "border": None,
        "label": label,
        "caption": f"{humanize_number(total)} runs" if total else "",
        "top_left": f"+{highest}" if highest else "",
        "top_right": f"{timed / total * 100:.0f}% timed" if total else "",
    }


def create_dungeon_tierlist_img(out_path, season, icon_size=None):
    """Build the dungeon tierlist card; returns the post_data facts dict.
    icon_size is accepted for caller compatibility and ignored."""
    dungeon_lookup = get_dungeon_lookup()

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )
        total_runs = databaseConnector.fetch_total_season_runs(conn, cursor, season)

    # highest key seen per dungeon (max keystone_level with at least one run)
    highest_keys = {}
    for r in dungeon_data:
        if int(r.get("total_runs", 0)) > 0:
            did = int(r["dungeon_id"])
            lvl = int(r.get("keystone_level", 0))
            if lvl > highest_keys.get(did, 0):
                highest_keys[did] = lvl

    tiers_raw = build_ckmeans_tiers(dungeon_lookup, dungeon_data)
    tiers = {
        L: [_dungeon_entry(it, dungeon_lookup, highest_keys) for it in tiers_raw.get(L, [])]
        for L in TIER_LETTERS
    }

    render_tierlist_card(
        out_path,
        "Mythic+ Dungeon Tierlist",
        f"{humanize_number(total_runs)} runs analyzed  •  weighted by key level",
        tiers,
    )

    ordered = [e for L in TIER_LETTERS for e in tiers.get(L, [])]
    second_best = ordered[1]["label"] if len(ordered) > 1 else ""
    second_worst = ordered[-2]["label"] if len(ordered) > 2 else ""
    post_data = {
        "tierlist_type": "Dungeon Tierlist",
        "best_dungeon": ordered[0]["label"] if ordered else "",
        "worst_dungeon": ordered[-1]["label"] if ordered else "",
        "second_best_dungeon": second_best,
        "second_worst_dungeon": second_worst,
        "total_runs": humanize_number(total_runs),
    }
    return post_data
