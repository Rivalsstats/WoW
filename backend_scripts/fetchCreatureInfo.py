from __future__ import annotations
import os
import time
import json
import tempfile
from contextlib import closing
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from databaseConnector import fetch_top_hunter_pets, init_connection_pool, get_connection
from aggregateData import get_access_token
# Constants
CREATURE_URL_TPL = "https://us.api.blizzard.com/data/wow/creature/{creature_id}"
MEDIA_URL_TPL = "https://us.api.blizzard.com/data/wow/media/creature-display/{display_id}"
NAMESPACE = "static-us"
LOCALE = "en_US"

DATA_DIR = Path("data")
STATIC_CREATURES_PATH = DATA_DIR / "static" / "creatures.json"
CREATURE_IMG_DIR = DATA_DIR / "creature_img"

REQUEST_TIMEOUT = 10
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.2

init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    1,
)

def request_with_retries(url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None, stream: bool = False) -> requests.Response:
    params = params or {}
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT, stream=stream)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return resp  # caller handles 404 specially
            # raise for other HTTP errors to trigger retry logic
            resp.raise_for_status()
        except requests.HTTPError as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF_BASE
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF_BASE
    raise ScriptError(f"Failed to GET {url}")


# ---------------------------
# File helpers
# ---------------------------
def safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def atomic_write_json(path: Path, data: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        shutil.move(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def download_image(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = request_with_retries(url, headers={}, params=None, stream=True)
    with dest.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)


def pick_localized(d: Dict[str, Any], locale_key: str = LOCALE) -> Optional[str]:
    if not isinstance(d, dict):
        return None
    return d.get(locale_key) or d.get("en_US") or next(iter(d.values()), None)


def process_creatures(rows: List[Dict[str, Any]]):
    if not rows:
        print("No rows to process.")
        return

    token = get_access_token(os.environ.get("BLIZ_CLIENT_ID"), os.environ.get("BLIZ_CLIENT_SECRET"))
    headers = {"Authorization": f"Bearer {token}"}

    STATIC_CREATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREATURE_IMG_DIR.mkdir(parents=True, exist_ok=True)

    existing = safe_read_json(STATIC_CREATURES_PATH) or {}
    if not isinstance(existing, dict):
        existing = {}

    for row in rows:
        try:
            creature_id = int(row.get("creature_id"))
        except Exception:
            print(f"Skipping malformed row: {row}")
            continue

        print(f"Processing creature {creature_id} ...")
        existing_entry = existing.get(str(creature_id), {})

        # fetch creature details
        creature_url = CREATURE_URL_TPL.format(creature_id=creature_id)
        params = {"namespace": NAMESPACE,}
        try:
            resp = request_with_retries(creature_url, headers=headers, params=params)
        except Exception as e:
            print(f"  Error fetching creature {creature_id}: {e}")
            continue

        if resp.status_code == 404:
            print(f"  Creature {creature_id} not found (404). Skipping.")
            continue

        try:
            creature_json = resp.json()
        except Exception as e:
            print(f"  Failed to decode JSON for creature {creature_id}: {e}")
            continue
        name = creature_json["name"]
        type_name = creature_json["type"]["name"]
        family_name = creature_json["family"]["name"]
        family_id = creature_json["family"]["id"]

        display_id = None
        image_path = existing_entry.get("image")
        displays = creature_json.get("creature_displays") or []
        if displays:
            for d in displays:
                if isinstance(d, dict) and d.get("id"):
                    display_id = int(d["id"])
                    break

        if display_id:
            media_url = MEDIA_URL_TPL.format(display_id=display_id)
            media_params = {"namespace": NAMESPACE, "locale": LOCALE}
            try:
                media_resp = request_with_retries(media_url, headers=headers, params=media_params)
                if media_resp.status_code == 200:
                    media_json = media_resp.json()
                    assets = media_json.get("assets") or []
                    asset_url = None
                    # prefer "zoom" key, else first asset value
                    for asset in assets:
                        if asset.get("key") == "zoom" and asset.get("value"):
                            asset_url = asset["value"]
                            break
                    if not asset_url and assets:
                        asset_url = assets[0].get("value")
                    if asset_url:
                        # infer extension
                        rawpath = asset_url.split("?")[0]
                        ext = Path(rawpath).suffix or ".jpg"
                        filename = f"{creature_id}{ext}"
                        dest = CREATURE_IMG_DIR / filename
                        if not dest.exists():
                            try:
                                print(f"  Downloading image for creature {creature_id} -> {dest}")
                                download_image(asset_url, dest)
                            except Exception as e:
                                print(f"  Warning: failed to download image {asset_url}: {e}")
                        image_path = str(dest.as_posix())
                elif media_resp.status_code == 404:
                    print(f"  No media for display id {display_id}")
            except Exception as e:
                print(f"  Warning: failed to fetch media for display {display_id}: {e}")

        # Build/merge entry
        entry = {
            "name": name,
            "type": type_name,
            "family": family_name,
            "family_id": family_id,
            "image": image_path,
        }
        print(entry)
        # Merge: keep existing fields unless overwritten by non-None values in entry
        merged = dict(existing_entry)  # copy
        for k, v in entry.items():
            if v is not None:
                merged[k] = v
        existing[str(creature_id)] = merged

    # atomic write
    atomic_write_json(STATIC_CREATURES_PATH, existing)
    print(f"Saved metadata to {STATIC_CREATURES_PATH}")
    print(f"Images stored under {CREATURE_IMG_DIR}")


# ---------------------------
# Main
# ---------------------------
def main():
    with closing(get_connection()) as conn:
        cursor = conn.cursor()
        rows = fetch_top_hunter_pets(conn, cursor)

    # rows expected like: [{"creature_id": 42722, "run_count": 5}, ...]
    if not rows:
        print("fetch_top_hunter_pets returned no rows. Nothing to do.")
        return

    try:
        process_creatures(rows)
    except Exception as e:
        print(f"Unhandled error: {e}")


if __name__ == "__main__":
    main()
