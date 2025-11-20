import csv
import json
import os
from aggregateData import (
    get_access_token,
    fetch_json,
    API_BASE,
    NAMESPACE_DYNAMIC,
    LOCALE,
)

# Paths
INPUT_CSV = "data/static/specs.csv"
OUTPUT_JSON = "data/static/specs.json"

# Names and classIDs to exclude
EXCLUDE_NAMES = {"Initial", "Adventurer"}
EXCLUDE_CLASSIDS = {"0"}
# Fields we care about (header names in CSV)
# The CSV is expected to have these exact headers
CSV_FIELD_NAMES = [
    "Name_lang",
    "Description_lang",
    "ID",
    "ClassID",
    "Role",
    "SpellIconFileID",
]

CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]


def get_primary_stat(token: str, specialization_id: int) -> int:
    region = "us"
    url = f"{API_BASE.format(region=region)}/data/wow/playable-specialization/{specialization_id}?namespace=static-us&locale=en_US"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = fetch_json(url, params, token)
    if not data or not data.get("primary_stat_type"):
        return None
    return data["primary_stat_type"]["type"]


def main():
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

    specs = {}
    with open(INPUT_CSV, newline="", encoding="utf-8") as csvfile:
        # Let DictReader detect headers from first line
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Get ID, name, and classID, stripping whitespace
            id_value = row.get("ID", "").strip()
            name = row.get("Name_lang", "").strip()
            class_id = row.get("ClassID", "").strip()

            # Skip rows without an ID, excluded names, or excluded classIDs
            if not id_value or name in EXCLUDE_NAMES or class_id in EXCLUDE_CLASSIDS:
                continue

            # Build spec entry
            specs[id_value] = {
                "name": name,
                "description": row.get("Description_lang", "").strip(),
                "classID": class_id,
                "role": row.get("Role", "").strip(),
                "SpellIconFileId": row.get("SpellIconFileID", "").strip(),
                "primary_stat": get_primary_stat(
                    get_access_token(CLIENT_ID, CLIENT_SECRET), id_value
                ),
            }

    # Write JSON file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(specs, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
