import databaseConnector
import json
from pathlib import Path
import requests
API_BASE = "https://{region}.api.blizzard.com"
OAUTH_BASE = "https://oauth.battle.net/token"
LOCALE = "en_US"
NAMESPACE_DYNAMIC = "dynamic-{region}"

SPEC_PATH = Path('data', 'static', 'specs.json')
AGGREGATED_PATH = Path('data', 'aggregated')

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
    item_list = databaseConnector.fetch_top_items_for_slot(conn, cursor, spec_id, current_season_id, slot)
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(conn, cursor, spec_id, current_season_id, item)
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append({
            "item": item,
            "count": int(count),
            "bonus": {
                "ids": bonus_ids,
                "count": int(bonus_count)
            } if bonus_list and bonus_list[0] else None

        })
    return item_data

def get_items_for_slot_group(conn, cursor, spec_id, current_season_id, slot_group: str):
    item_list = databaseConnector.fetch_top_items_for_slot_group(conn, cursor, spec_id, current_season_id, slot_group)
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(conn, cursor, spec_id, current_season_id, item)
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append({
            "item": item,
            "count": int(count),
            "bonus": {
                "ids": bonus_ids,
                "count": int(bonus_count)
            } if bonus_list and bonus_list[0] else None

        })
    return item_data


def get_hero_trees(conn, cursor, spec_id, current_season_id):
    top_hero_trees = databaseConnector.fetch_hero_tree_overview(conn, cursor, spec_id, current_season_id)
    overall_hero_trees = []
    for hero_tree_id, count in top_hero_trees:
        overall_hero_trees.append({"id": hero_tree_id, "count": int(count)})
    return overall_hero_trees

def get_enchants_for_slot(conn, cursor, spec_id, current_season_id, slot_group):
    top_enchants = databaseConnector.fetch_top_enchant_for_slot(conn, cursor, spec_id, current_season_id, slot_group, 10)
    overall_enchants = []
    for enchant_item, count in top_enchants:
        overall_enchants.append({"id": enchant_item, "count": int(count)})
    overall_enchants.sort(key=lambda x: x['count'], reverse=True)
    return overall_enchants

def get_sockets(conn, cursor, spec_id, current_season_id):
    top_sockets = databaseConnector.fetch_top_sockets(conn, cursor, spec_id, current_season_id)
    overall_sockets = []
    for socket, count in top_sockets:
        overall_sockets.append({"id": socket, "count": int(count)})
        
    return overall_sockets

def get_loadout(conn, cursor, spec_id, current_season_id):
    top_loadouts = databaseConnector.fetch_top_loadout(conn, cursor, spec_id, current_season_id)
    overall_loadouts = {}
    for hero_talent_id, loadout, count in top_loadouts:
        overall_loadouts[hero_talent_id] = {"loadout": loadout, "count": int(count)}
    return overall_loadouts
