import databaseConnector
import json
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
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


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


def get_hero_trees(conn, cursor, spec_id, current_season_id):
    top_hero_trees = databaseConnector.fetch_hero_tree_overview(
        conn, cursor, spec_id, current_season_id
    )
    overall_hero_trees = []
    for hero_tree_id, count in top_hero_trees:
        overall_hero_trees.append({"id": hero_tree_id, "count": int(count)})
    return overall_hero_trees


def get_enchants_for_slot(conn, cursor, spec_id, current_season_id, slot_group):
    top_enchants = databaseConnector.fetch_top_enchant_for_slot(
        conn, cursor, spec_id, current_season_id, slot_group, 10
    )
    overall_enchants = []
    for enchant_item, count in top_enchants:
        overall_enchants.append({"id": enchant_item, "count": int(count)})
    overall_enchants.sort(key=lambda x: x["count"], reverse=True)
    return overall_enchants


def get_sockets(conn, cursor, spec_id, current_season_id):
    top_sockets = databaseConnector.fetch_top_sockets(
        conn, cursor, spec_id, current_season_id
    )
    overall_sockets = []
    for socket, count in top_sockets:
        overall_sockets.append({"id": socket, "count": int(count)})

    return overall_sockets


def get_loadout(conn, cursor, spec_id, current_season_id):
    top_loadouts = databaseConnector.fetch_top_loadout(
        conn, cursor, spec_id, current_season_id
    )
    overall_loadouts = {}
    for hero_talent_id, loadout, count in top_loadouts:
        overall_loadouts[hero_talent_id] = {"loadout": loadout, "count": int(count)}
    return overall_loadouts


def get_talent_differences(talent_diffs, points_available, valid_talents):
    overall_talent_diffs = {}
    dungeon_counts = {}
    total_count = 0
    talent_counts = {}
    dungeon_talent_counts = {}
    for hero_talent_id, dungeon, talent_id, count in talent_diffs:
        if int(talent_id) not in valid_talents:
            continue
        dungeon_counts[dungeon] = dungeon_counts.get(dungeon, 0) + int(count)
        talent_counts[talent_id] = talent_counts.get(talent_id, 0) + int(count)
        total_count += int(count)
        dungeon_talent_counts[dungeon] = dungeon_talent_counts.get(dungeon, {})
        dungeon_talent_counts[dungeon][talent_id] = dungeon_talent_counts[dungeon].get(
            talent_id, 0
        ) + int(count)
    overall_talent_diffs["total_count"] = total_count
    data_count = total_count / points_available if points_available else 1
    overall_talent_diffs["data_count"] = data_count
    enriched_talent_counts = []
    for talent, count in talent_counts.items():
        enriched_talent_counts.append(
            {"id": talent, "count": int(count), "pct": (int(count) / data_count) * 100}
        )
    overall_talent_diffs["overall_dungeon_talents"] = enriched_talent_counts

    enriched_dungeon_talent_counts = {}
    for dungeon, talents in dungeon_talent_counts.items():
        enriched_dungeon_talents = []
        for talent, count in talents.items():
            enriched_dungeon_talents.append(
                {
                    "id": talent,
                    "count": int(count),
                    "pct": (
                        int(count)
                        / (
                            dungeon_counts[dungeon] / points_available
                            if points_available
                            else 1
                        )
                    )
                    * 100,
                }
            )
        enriched_dungeon_talent_counts[dungeon] = enriched_dungeon_talents
    overall_talent_diffs["dungeon_talent_counts"] = enriched_dungeon_talent_counts
    return overall_talent_diffs


def get_hero_talent_differences(conn, cursor, spec_id, current_season_id, valid_talents):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_hero_talent_diffs = databaseConnector.fetch_hero_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    hero_talent_points_available = spendable_talents.get("hero", 0)

    return get_talent_differences(top_hero_talent_diffs, hero_talent_points_available, valid_talents)


def get_spec_talent_differences(conn, cursor, spec_id, current_season_id, valid_talents):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_spec_talent_diffs = databaseConnector.fetch_spec_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    spec_talent_points_available = spendable_talents.get("spec", 0)

    return get_talent_differences(top_spec_talent_diffs, spec_talent_points_available, valid_talents)


def get_class_talent_differences(conn, cursor, spec_id, current_season_id, valid_talents):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_class_talent_diffs = databaseConnector.fetch_class_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    class_talent_points_available = spendable_talents.get("class", 0)

    return get_talent_differences(top_class_talent_diffs, class_talent_points_available, valid_talents)


def biggest_deviations_per_dungeon(data, top_n=3):
    """
    Returns for each dungeon the top N gains and top N losses compared to overall distribution.
    Output format:
    {
      "<dungeon_id>": {
         "gains": [ {talent_id, overall_pct, dungeon_pct, pct_point_diff, rel_pct_change_percent}, ... ],
         "losses": [ { ... }, ... ]
      }, ...
    }
    """
    # build map of overall pct by talent id
    overall_map = {
        int(item["id"]): float(item["pct"])
        for item in data.get("overall_dungeon_talents", [])
    }

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
            ) # signed difference in percentage points
            rel_change = None
            if overall_pct != 0:
                rel_change = (pct_point_diff / overall_pct) * 100.0
            rows.append(
                {
                    "talent_id": tid,
                    "overall_pct": overall_pct,
                    "dungeon_pct": dungeon_pct,
                    "pct_point_diff": pct_point_diff,
                    "rel_pct_change_percent": rel_change,
                }
            )

        # gains: positive pct_point_diff, sorted descending
        gains = [r for r in rows if r["pct_point_diff"] > 0]
        gains_sorted = sorted(gains, key=lambda r: r["pct_point_diff"], reverse=True)[
            :top_n
        ]

        # losses: negative pct_point_diff, sorted ascending (most negative first)
        losses = [r for r in rows if r["pct_point_diff"] < 0]
        losses_sorted = sorted(losses, key=lambda r: r["pct_point_diff"])[:top_n]

        results[str(dungeon)] = {"gains": gains_sorted, "losses": losses_sorted}

    return results


def get_hero_tree_differences(conn, cursor, spec_id, current_season_id):
    top_hero_tree_differences = databaseConnector.fetch_hero_tree_differences(
        conn, cursor, spec_id, current_season_id
    )
    overall_counts = {}
    total_count = 0
    dungeon_counts = {}
    data = {}
    for hero_tree, dungeon, count in top_hero_tree_differences:
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
            "pct": (count / total_count if total_count > 0 else 1 )* 100,
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
                "pct": (count / dungeon_counts[dungeon] )* 100
                if dungeon_counts[dungeon] > 0
                else 1,
                "diff": (count / dungeon_counts[dungeon] )* 100
                - enriched_data["overall"][hero_tree]["pct"],
            }

    return enriched_data
