import requests
from collections import defaultdict
from typing import Dict
import os
import json
from contextlib import closing
import databaseConnector

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)


def get_npc_names_retail(timeout: int = 10) -> Dict[str, Dict[int, str]]:
    """
    Download Wowhead NPC names for the 'retail' environment (dataEnv=1)
    and return a mapping: locale (e.g. 'en_US') -> { npc_id: npc_name }.

    Raises RuntimeError on network / JSON errors.
    """
    url = "https://nether.wowhead.com/data/npc-names?dataEnv=1"
    session = requests.Session()
    # polite UA; some sites reject empty/unknown user agents
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (compatible; npc-names-collector/1.0)"}
    )

    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        raise RuntimeError(f"HTTP request failed: {e}")
    except ValueError as e:
        raise RuntimeError(f"Failed to decode JSON: {e}")

    result: Dict[str, Dict[int, str]] = defaultdict(dict)

    for entry in data:
        # ensure expected shape
        npc_id = entry.get("id")
        if not isinstance(npc_id, int):
            # skip malformed entries
            continue

        for k, v in entry.items():
            if k == "id":
                continue
            if not k.startswith("name_"):
                continue

            # k examples: 'name_enus', 'name_frfr', 'name_zhcn', 'name_esmx'
            locale_raw = k[len("name_") :]  # e.g., 'enus'
            # convert to e.g. 'en_US'
            if len(locale_raw) == 4:
                lang = locale_raw[:2]
                region = locale_raw[2:].upper()
                locale = f"{lang}_{region}"
            else:
                # fallback: split into two parts (first two chars + rest)
                lang = locale_raw[:2]
                region = locale_raw[2:].upper() if len(locale_raw) > 2 else ""
                locale = f"{lang}_{region}" if region else lang

            # skip null/empty names
            if v is None:
                continue

            result[locale][npc_id] = str(v)

    # convert defaultdict to normal dict for return
    return {loc: ids for loc, ids in result.items()}


def save_npc_names_for_db_ids(connection, cursor):
    npc_ids = databaseConnector.fetch_distinct_npc_ids(
        connection, cursor
    )  # returns list[int]
    npc_id_set = set(int(i) for i in npc_ids)

    # ensure output dir exists
    os.makedirs(os.path.join("data", "static"), exist_ok=True)
    out_path = os.path.join(os.path.join("data", "static"), "npcs.json")

    if not npc_id_set:
        # write empty mapping
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        print(f"No NPC IDs found. Wrote empty file to {out_path}")
        return

    # fetch all retail names (function you have from the earlier example)
    names_by_locale = get_npc_names_retail()  # -> Dict[str, Dict[int, str]]

    # filter to only IDs we care about; convert keys to strings for JSON
    filtered = {}
    for locale, id_map in names_by_locale.items():
        # id_map: dict[int, str]
        filtered_map = {
            str(npc_id): name
            for npc_id, name in id_map.items()
            if int(npc_id) in npc_id_set
        }
        if filtered_map:
            filtered[locale] = filtered_map

    # write to disk
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2, ensure_ascii=False)

    print(f"Wrote NPC names for {len(filtered)} locales to {out_path}")


if __name__ == "__main__":
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        print("fetching data..")
        save_npc_names_for_db_ids(conn, cursor)
