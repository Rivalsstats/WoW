import os
import json

# Path to the local talents data
TALENTS_PATH = os.path.join("data", "static", "talents.json")
# Output directory
OUTPUT_DIR = os.path.join("data", "static", "talents")
NODE_TYPES = ["classNodes", "specNodes", "heroNodes"]


def fetch_talents():
    with open(TALENTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def build_lookup(talents_data):
    """
    Given the full JSON, returns a dict:
      spec_id -> { entry_id: { 'name': ..., 'icon': ... }, ... }
    """
    lookup = {}
    for spec in talents_data:
        spec_id = spec["specId"]
        mapping = {}
        mapping["specName"] = spec.get("specName", "")
        mapping["className"] = spec.get("className", "")
        mapping["talents"] = {}
        mapping["subTrees"] = {}
        for node_key in NODE_TYPES:
            for node in spec.get(node_key, []):
                node_id = node["id"]
                if node.get("freeNode"):
                    continue
                for entry in node.get("entries", []):
                    # Avoid overwriting if duplicate across node types
                    if node_id not in mapping["talents"]:
                        mapping["talents"][node_id] = {
                            "name": entry.get("name", node.get("name")),
                            "icon": entry.get("icon", node.get("icon", "")),
                            "spellId": entry.get("spellId", node.get("spellId", 0)),
                        }
        for subtree in spec.get("subTreeNodes", []):
            for entry in subtree.get("entries", []):
                ts_id = entry["traitSubTreeId"]
                mapping["subTrees"][ts_id] = {
                    "name": entry.get("name", ""),
                    "icon": entry.get("atlasMemberName", ""),
                }
        lookup[spec_id] = mapping
    return lookup


def write_spec_files(lookup):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for spec_id, mapping in lookup.items():
        out_path = os.path.join(OUTPUT_DIR, f"{spec_id}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(mapping)} entries for spec {spec_id} → {out_path}")


def main():
    print("Loading talents data…")
    data = fetch_talents()
    print(f"Loaded {len(data)} specs.")
    lookup = build_lookup(data)
    print("Building lookup and writing files…")
    write_spec_files(lookup)
    print("Done.")


if __name__ == "__main__":
    main()
