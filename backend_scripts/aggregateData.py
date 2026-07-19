import databaseConnector
import json
from collections import defaultdict
from pathlib import Path
import requests

API_BASE = "https://{region}.api.blizzard.com"
OAUTH_BASE = "https://oauth.battle.net/token"
LOCALE = "en_US"
NAMESPACE_DYNAMIC = "dynamic-{region}"

SPEC_PATH = Path("data", "static", "specs.json")
TALENT_POINTS_PATH = Path("data", "static", "spendableTalents.json")


def load_existing_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def get_access_token(CLIENT_ID: str, CLIENT_SECRET: str) -> str:
    url = "https://oauth.battle.net/token"
    resp = requests.post(
        url, data={"grant_type": "client_credentials"}, auth=(CLIENT_ID, CLIENT_SECRET)
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_json(url: str, params: dict, token: str) -> dict | None:
    try:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Error fetching JSON from {url}: {e}")
        return None


def get_current_season_id(token: str) -> int:
    region = "us"
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/index"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = fetch_json(url, params, token)
    if not data or not data.get("seasons"):
        return None
    return data["current_season"]["id"]


def get_items_for_slot(conn, cursor, spec_id, current_season_id, slot: str):
    item_list = databaseConnector.fetch_top_items_for_slot(
        conn, cursor, spec_id, current_season_id, slot
    )
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(
            conn, cursor, spec_id, current_season_id, item
        )
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append(
            {
                "item": item,
                "count": int(count),
                "bonus": {"ids": bonus_ids, "count": int(bonus_count)}
                if bonus_list and bonus_list[0]
                else None,
            }
        )
    return item_data


def get_items_for_slot_group(conn, cursor, spec_id, current_season_id, slot_group: str):
    item_list = databaseConnector.fetch_top_items_for_slot_group(
        conn, cursor, spec_id, current_season_id, slot_group
    )
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(
            conn, cursor, spec_id, current_season_id, item
        )
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append(
            {
                "item": item,
                "count": int(count),
                "bonus": {"ids": bonus_ids, "count": int(bonus_count)}
                if bonus_list and bonus_list[0]
                else None,
            }
        )
    return item_data


def get_hero_trees(conn, cursor, spec_id, current_season_id, valid_subtrees=None):
    top_hero_trees = databaseConnector.fetch_hero_tree_overview(
        conn, cursor, spec_id
    )
    overall_hero_trees = []
    for hero_tree_id, count, max_timed_key, max_depleted_key in top_hero_trees:
        # Skip hero trees that don't belong to this spec (e.g. cross-spec
        # contaminated loadouts) so downstream subTrees lookups never miss.
        if valid_subtrees is not None and int(hero_tree_id) not in valid_subtrees:
            print(
                f"WARNING: spec {spec_id} returned hero tree {hero_tree_id} "
                f"(count={count}) not in its subTrees {sorted(valid_subtrees)}; "
                f"skipping contaminated loadout."
            )
            continue
        overall_hero_trees.append({"id": hero_tree_id, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})
    return overall_hero_trees


def get_enchants_for_slot(conn, cursor, spec_id, current_season_id, slot_group):
    top_enchants = databaseConnector.fetch_top_enchant_for_slot(
        conn, cursor, spec_id, current_season_id, slot_group, 10
    )
    overall_enchants = []
    for enchant_item, count, max_timed_key, max_depleted_key in top_enchants:
        overall_enchants.append({"id": enchant_item, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})
    overall_enchants.sort(key=lambda x: x["count"], reverse=True)
    return overall_enchants


def get_sockets(conn, cursor, spec_id, current_season_id):
    top_sockets = databaseConnector.fetch_top_sockets(
        conn, cursor, spec_id, current_season_id
    )
    overall_sockets = []
    for socket, count, max_timed_key, max_depleted_key in top_sockets:
        overall_sockets.append({"id": socket, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})

    return overall_sockets


def get_loadout(conn, cursor, spec_id, current_season_id):
    top_loadouts = databaseConnector.fetch_top_loadout(
        conn, cursor, spec_id, current_season_id
    )
    overall_loadouts = {}
    for hero_talent_id, loadout, count, max_timed_key, max_depleted_key in top_loadouts:
        overall_loadouts[hero_talent_id] = {"loadout": loadout, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)}
    return overall_loadouts


def get_talent_differences(talent_diffs, points_available, valid_talents):
    overall_talent_diffs = {}
    dungeon_counts = {}
    total_count = 0
    talent_counts = {}
    talent_ranks = {}
    dungeon_talent_counts = {}
    for row in talent_diffs:
        hero_talent_id, dungeon, talent_id, count = row[:4]
        avg_rank = row[4] if len(row)>4 else 1.0

        if int(talent_id) not in valid_talents:
            continue
        dungeon_counts[dungeon] = dungeon_counts.get(dungeon, 0) + int(count)
        talent_counts[talent_id] = talent_counts.get(talent_id, 0) + int(count)
        total_count += int(count)
        talent_ranks[talent_id] = float(avg_rank or 1.0)
        dungeon_talent_counts[dungeon] = dungeon_talent_counts.get(dungeon, {})
        dungeon_talent_counts[dungeon][talent_id] = dungeon_talent_counts[dungeon].get(
            talent_id, 0
        ) + int(count)
    overall_talent_diffs["total_count"] = total_count
    data_count = max(talent_counts.values()) if talent_counts else 1
    overall_talent_diffs["data_count"] = data_count
    enriched_talent_counts = []
    for talent, count in talent_counts.items():
        enriched_talent_counts.append(
            {"id": talent, "count": int(count), "pct": (int(count) / data_count) * 100, "avg_rank": talent_ranks.get(talent, 1.0)}
        )
    overall_talent_diffs["overall_dungeon_talents"] = enriched_talent_counts

    enriched_dungeon_talent_counts = {}
    for dungeon, talents in dungeon_talent_counts.items():
        enriched_dungeon_talents = []
        dungeon_data_count = max(talents.values()) if talents else 1
        for talent, count in talents.items():
            enriched_dungeon_talents.append(
                {
                    "id": talent,
                    "count": int(count),
                    "pct": (int(count) / dungeon_data_count) * 100,
                    "avg_rank": talent_ranks.get(talent, 1.0)
                }
            )
        enriched_dungeon_talent_counts[dungeon] = enriched_dungeon_talents
    overall_talent_diffs["dungeon_talent_counts"] = enriched_dungeon_talent_counts
    return overall_talent_diffs


def get_hero_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_hero_talent_diffs = databaseConnector.fetch_hero_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    hero_talent_points_available = spendable_talents.get("hero", 0)

    return get_talent_differences(
        top_hero_talent_diffs, hero_talent_points_available, valid_talents
    )


def get_spec_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents, rows=None
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    if rows is None:
        rows = databaseConnector.fetch_spec_talents_differences(
            conn, cursor, spec_id, current_season_id
        )
    spec_talent_points_available = spendable_talents.get("spec", 0)

    return get_talent_differences(
        rows, spec_talent_points_available, valid_talents
    )


def get_class_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_class_talent_diffs = databaseConnector.fetch_class_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    class_talent_points_available = spendable_talents.get("class", 0)

    return get_talent_differences(
        top_class_talent_diffs, class_talent_points_available, valid_talents
    )


def _by_hero_tree(rows, valid_talents):
    """Partition raw talent-difference rows by hero tree.

    rows: iterable of (hero_talent_id, dungeon, talent_id, count, avg_rank).
    Returns {hero_tree_id: <same dict shape as get_talent_differences>}.

    get_talent_differences already reads hero_talent_id as row[0] and ignores
    it, so running it on a single-tree slice produces tree-relative stats.
    """
    buckets = defaultdict(list)
    for row in rows:
        buckets[int(row[0])].append(row)
    return {
        tid: get_talent_differences(tree_rows, 0, valid_talents)
        for tid, tree_rows in buckets.items()
    }


def get_hero_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    return _by_hero_tree(
        databaseConnector.fetch_hero_talents_differences(
            conn, cursor, spec_id, current_season_id
        ),
        valid_talents,
    )


def get_spec_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents, rows=None
):
    if rows is None:
        rows = databaseConnector.fetch_spec_talents_differences(
            conn, cursor, spec_id, current_season_id
        )
    return _by_hero_tree(rows, valid_talents)


def get_class_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    return _by_hero_tree(
        databaseConnector.fetch_class_talents_differences(
            conn, cursor, spec_id, current_season_id
        ),
        valid_talents,
    )


def biggest_deviations_per_dungeon(
    data,
    top_n=3,
    top_overall=None,
    top_dungeon_pct=None,
    top_weight=0.7,
    normal_weight=0.3,
):
    """
    Returns for each dungeon the top N gains and top N losses compared to overall distribution.
    Output format:
    {
      "<dungeon_id>": {
         "gains": [ {talent_id, overall_pct, dungeon_pct, pct_point_diff, rel_pct_change_percent, weighted_pct}, ... ],
         "losses": [ { ... }, ... ]
      }, ...
    }

    When `top_overall` (node_id -> overall top-50 adoption %) and `top_dungeon_pct`
    ({dungeon_str: {node_id: adoption %}}) are supplied, each talent's ranking score
    (`weighted_pct`) blends the general-population relative change with the top-50
    players' relative change, weighting the top-50 signal `top_weight` vs
    `normal_weight`. Without them it falls back to the plain general relative change.
    """
    # build map of overall pct by talent id
    overall_map = {
        int(item["id"]): float(item["pct"])
        for item in data.get("overall_dungeon_talents", [])
    }
    use_top = top_overall is not None and top_dungeon_pct is not None

    results = {}
    for dungeon, talents in data.get("dungeon_talent_counts", {}).items():
        rows = []
        for t in talents:
            tid = int(t["id"])
            dungeon_pct = float(t.get("pct", 0))
            overall_pct = overall_map.get(tid)
            # skip talents that don't have an overall baseline
            if overall_pct is None:
                continue
            pct_point_diff = (
                dungeon_pct - overall_pct
            )  # signed difference in percentage points
            rel_change = None
            if overall_pct != 0:
                rel_change = (pct_point_diff / overall_pct) * 100.0

            # Blend in the top-50 players' per-dungeon relative change so that
            # talents the elite actually swap for a given dungeon rank higher.
            weighted = rel_change
            if use_top:
                t_over = float(top_overall.get(tid, 0.0) or 0.0)
                if t_over > 0:
                    t_dun = float(
                        (top_dungeon_pct.get(str(dungeon), {}) or {}).get(tid, 0.0)
                    )
                    top_rel = ((t_dun - t_over) / t_over) * 100.0
                    if rel_change is None:
                        weighted = top_rel
                    else:
                        weighted = normal_weight * rel_change + top_weight * top_rel
                # else: top players never take this node at all -> no top signal,
                # keep the plain general relative change.

            rows.append(
                {
                    "talent_id": tid,
                    "overall_pct": overall_pct,
                    "dungeon_pct": dungeon_pct,
                    "pct_point_diff": pct_point_diff,
                    "rel_pct_change_percent": rel_change,
                    "weighted_pct": weighted,
                    "id": tid,
                    "pct": pct_point_diff, # Keep this for compatibility if it's used elsewhere, though usually rel_change is better
                }
            )

        # gains: positive weighted score, sorted descending
        gains = [r for r in rows if r["weighted_pct"] is not None and r["weighted_pct"] > 0]
        gains_sorted = sorted(gains, key=lambda r: r["weighted_pct"], reverse=True)[
            :top_n
        ]

        # losses: negative weighted score, sorted ascending (most negative first)
        losses = [r for r in rows if r["weighted_pct"] is not None and r["weighted_pct"] < 0]
        losses_sorted = sorted(losses, key=lambda r: r["weighted_pct"])[:top_n]

        results[str(dungeon)] = {"gains": gains_sorted, "losses": losses_sorted}

    return results


def get_hero_tree_differences(conn, cursor, spec_id, current_season_id, valid_subtrees=None):
    top_hero_tree_differences = databaseConnector.fetch_hero_tree_differences(
        conn, cursor, spec_id, current_season_id
    )
    overall_counts = {}
    total_count = 0
    dungeon_counts = {}
    data = {}
    for hero_tree, dungeon, count, avg_rank in top_hero_tree_differences:
        # Skip hero trees that don't belong to this spec (e.g. cross-spec
        # contaminated loadouts) so downstream subtree-id lookups never miss.
        if valid_subtrees is not None and int(hero_tree) not in valid_subtrees:
            continue
        overall_counts[hero_tree] = overall_counts.get(hero_tree, 0) + int(count)
        total_count += int(count)
        dungeon_counts[dungeon] = dungeon_counts.get(dungeon, 0) + int(count)
        data[dungeon] = data.get(dungeon, {})
        data[dungeon][hero_tree] = data[dungeon].get(hero_tree, 0) + int(count)

    enriched_data = {}
    for hero_tree in overall_counts:
        enriched_data["overall"] = enriched_data.get("overall", {})
        count = enriched_data["overall"].get(hero_tree, 0) + overall_counts[hero_tree]
        enriched_data["overall"][hero_tree] = {
            "count": count,
            "pct": (count / total_count if total_count > 0 else 1) * 100,
        }
    for dungeon, hero_trees in data.items():
        for hero_tree in hero_trees:
            enriched_data["dungeons"] = enriched_data.get("dungeons", {})
            enriched_data["dungeons"][dungeon] = enriched_data["dungeons"].get(
                dungeon, {}
            )
            count = (
                enriched_data["dungeons"][dungeon].get(hero_tree, 0)
                + hero_trees[hero_tree]
            )
            enriched_data["dungeons"][dungeon][hero_tree] = {
                "count": count,
                "pct": (count / dungeon_counts[dungeon]) * 100
                if dungeon_counts[dungeon] > 0
                else 1,
                "diff": (count / dungeon_counts[dungeon]) * 100
                - enriched_data["overall"][hero_tree]["pct"],
            }

    return enriched_data
