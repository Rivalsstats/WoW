import os
import json
import time
import requests
import databaseConnector
from contextlib import closing
from aggregateData import get_current_season_id, get_access_token , fetch_json

# config
CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]
RAIDERIO_API_KEY = os.environ["RAIDERIO_API_KEY"]
NAMESPACE = "dynamic-us"  
API_BASE = "https://us.api.blizzard.com"
ICON_DIR = "data/icons"

databaseConnector.init_connection_pool(
    os.environ['DATABASE_HOST'], os.environ['DATABASE_USER'], os.environ['DATABASE_PASSWORD'], os.environ['DATABASE_NAME'], 1
)


meta_url = "https://www.raidbots.com/static/data/live/metadata.json"
mresp = requests.get(meta_url)
mresp.raise_for_status()
wow_build = mresp.json().get("wowBuild")  # e.g. "11.1.7.61559"
major = int(wow_build.split(".", 1)[0])  # e.g. 11
expansion_id = major - 1  # → 10

print(f"Derived expansion_id = {expansion_id}")

static_url = "https://raider.io/api/v1/mythic-plus/static-data"
static_params = {"access_key": RAIDERIO_API_KEY, "expansion_id": expansion_id}

sresp = requests.get(static_url, params=static_params)
sresp.raise_for_status()
raider_data = sresp.json()

print("Raider.IO static dungeons:")
for d in raider_data["dungeons"]:
    print(
        f"  journal-id={d['id']}, keystone-id={d['challenge_mode_id']}, short='{d['short_name']}'"
    )
token = get_access_token(CLIENT_ID, CLIENT_SECRET)
seasons = raider_data.get("seasons", [])

short_name_map = {}

slug_map = {}
current_season = get_current_season_id(token)

for season in seasons:
    if season.get("blizzard_season_id",0) == current_season:
        for d in season.get("dungeons", []):
            short_name_map[d["challenge_mode_id"]] = d["short_name"]
            slug_map[d["challenge_mode_id"]] = d["slug"]


print(short_name_map)

out = {}
headers = {"Authorization": f"Bearer {token}"}
for dungeon_id in short_name_map:
    print(f"Fetching data for dungeon {dungeon_id}...")
    url = f"{API_BASE}/data/wow/mythic-keystone/dungeon/{dungeon_id}"
    params = {"namespace": NAMESPACE}
    resp = requests.get(url, headers=headers, params=params)
    # handle rate‐limit / transient errors
    if resp.status_code == 429:
        retry = int(resp.headers.get("Retry-After", 1))
        time.sleep(retry)
        resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    upgrades = {
        str(u["upgrade_level"]): {
            "upgrade_level": u["upgrade_level"],
            "qualifying_duration": u["qualifying_duration"],
        }
        for u in data.get("keystone_upgrades", [])
    }

    journal_id = data["dungeon"]["id"]

    media_url = f"{API_BASE}/data/wow/media/journal-instance/{journal_id}"
    media_params = {"namespace": "static-us"}
    mresp = requests.get(media_url, headers=headers, params=media_params)
    mresp.raise_for_status()
    media = mresp.json()

    tile_asset = next(
        (a for a in media.get("assets", []) if a.get("key") == "tile"), None
    )
    icon_filename = None
    if tile_asset and tile_asset.get("value"):
        icon_url = tile_asset["value"]
        icon_filename = icon_url.rsplit("/", 1)[-1]
        try:
            img_resp = requests.get(icon_url)
            img_resp.raise_for_status()
            with open(os.path.join(ICON_DIR, icon_filename), "wb") as imgf:
                imgf.write(img_resp.content)
        except requests.RequestException as e:
            print(f"Error fetching icon for dungeon {dungeon_id}: {e}")

    out[dungeon_id] = {
        "name": data["name"],
        "slug": slug_map.get(dungeon_id),
        "keystone_upgrades": upgrades,
        "icon": icon_filename,
        "raiderio_short_name": short_name_map.get(dungeon_id),
    }
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        databaseConnector.insert_dungeon_data(
            conn, cursor,
            dungeon_id=dungeon_id,
            slug=slug_map.get(dungeon_id),
            name_en_us=data["name"]["en_US"],
            up1=upgrades.get("1", {}).get("qualifying_duration"),
            up2=upgrades.get("2", {}).get("qualifying_duration"),
            up3=upgrades.get("3", {}).get("qualifying_duration"),
        )
        databaseConnector.commit_changes(conn)

os.makedirs("data/static", exist_ok=True)
with open("data/static/dungeons.json", "w") as f:
    json.dump(out, f, indent=2)
