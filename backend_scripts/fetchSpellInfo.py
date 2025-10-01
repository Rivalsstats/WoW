from __future__ import annotations
import os
import time
import json
from contextlib import closing
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests

# project imports (assumes these exist in your repo)
from databaseConnector import (
    fetch_distinct_spell_ids,
    init_connection_pool,
    get_connection,
)
from aggregateData import get_access_token

# Config
SPELL_URL_TPL = "https://us.api.blizzard.com/data/wow/spell/{spell_id}"
NAMESPACE = "static-us"  # keep the namespace but omit locale so the API returns all locales
OUT_PATH = Path("data") / "static" / "spells.json"

# Simple retry helper
def get_with_retries(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, str]] = None,
    retries: int = 3,
    timeout: int = 15,
    backoff_base: float = 0.5,
) -> Optional[requests.Response]:
    params = params or {}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            return resp
        except requests.RequestException as exc:
            if attempt == retries:
                print(f"Network error for {url}: {exc}")
                return None
            time.sleep(backoff_base * attempt)
    return None

def process_spell_ids(spell_ids: List[int]):
    if not spell_ids:
        print("No spell IDs provided.")
        return

    client_id = os.environ.get("BLIZ_CLIENT_ID")
    client_secret = os.environ.get("BLIZ_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("BLIZ_CLIENT_ID and BLIZ_CLIENT_SECRET must be set in environment")

    token = get_access_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}

    out: Dict[str, Any] = {}
    total = len(spell_ids)

    for idx, raw_id in enumerate(spell_ids, start=1):
        try:
            spell_id = int(raw_id)
        except Exception:
            print(f"Skipping malformed id: {raw_id}")
            continue

        print(f"[{idx}/{total}] Fetching spell {spell_id} ...")
        url = SPELL_URL_TPL.format(spell_id=spell_id)
        params = {"namespace": NAMESPACE}  # intentionally omit 'locale' to get all locales in response
        resp = get_with_retries(url, headers=headers, params=params)
        if resp is None:
            print(f"  Failed to fetch spell {spell_id} due to network errors.")
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                print(f"  Failed to decode JSON for spell {spell_id}: {e}")
                continue

            # Only keep name and description fields as requested (may be dicts keyed by locale)
            entry = {}
            if "name" in data:
                entry["name"] = data["name"]
            else:
                entry["name"] = None

            if "description" in data:
                entry["description"] = data["description"]
            else:
                entry["description"] = None

            # Only include the entry if at least one of the fields is present
            if entry.get("name") is not None or entry.get("description") is not None:
                out[str(spell_id)] = entry
                print(f"  OK: stored name/description for {spell_id}")
            else:
                print(f"  No name/description found for {spell_id}; skipping entry.")

        elif resp.status_code == 404:
            print(f"  Spell {spell_id} not found (404). Skipping.")
        else:
            print(f"  HTTP {resp.status_code} for spell {spell_id}. Skipping.")

        # be polite to the API
        time.sleep(0.05)

    # write simple JSON file
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

    print(f"Wrote {len(out)} spells to {OUT_PATH}")

def main():
    # initialize DB pool (matches your other scripts)
    init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        1,
    )

    with closing(get_connection()) as conn:
        cursor = conn.cursor()
        ids = fetch_distinct_spell_ids(conn, cursor)

    if not ids:
        print("fetch_distinct_spell_ids returned no rows. Nothing to do.")
        return

    process_spell_ids(ids)

if __name__ == "__main__":
    main()
