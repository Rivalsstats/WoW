import csv
import json
import os

# Paths
INPUT_CSV = "data/static/classes.csv"
OUTPUT_JSON = "data/static/classes.json"

# Names and classIDs to exclude
EXCLUDE_NAMES = {"Adventurer"}
EXCLUDE_CLASSIDS = {14}

# Fields we care about (header names in CSV)
# The CSV is expected to have these exact headers
CSV_FIELD_NAMES = [
    "Name_lang",
    "Description_lang",
    "ID",
    "ClassColorR",
    "ClassColorG",
    "ClassColorB",
    "IconFileDataID",
]


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

            # Skip rows without an ID, excluded names, or excluded classIDs
            if not id_value or name in EXCLUDE_NAMES or id_value in EXCLUDE_CLASSIDS:
                continue

            # Build spec entry
            specs[id_value] = {
                "name": name,
                "description": row.get("Description_lang", "").strip(),
                "icon_id": row.get("IconFileDataID", "").strip(),
                "color": {
                    "r": row.get("ClassColorR", "").strip(),
                    "g": row.get("ClassColorG", "").strip(),
                    "b": row.get("ClassColorB", "").strip(),
                },
            }

    # Write JSON file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(specs, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
