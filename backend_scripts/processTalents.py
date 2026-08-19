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


def build_decode_nodes(spec):
    """Ordered decode data for a spec's talent loadout string.

    ``fullNodeOrder`` is the node id sequence the Blizzard loadout bitstream walks
    (verbatim from Raidbots' talents.json). ``nodes`` maps every node id to the
    metadata the client-side decoder needs: its ``entries`` in *choice order* (the
    2-bit choice index in the stream indexes into this list), plus ``type`` /
    ``maxRanks`` and, for hero nodes, the ``subTreeId`` so the client can tell
    which hero tree a decoded build sits in.

    Unlike the ``talents`` display map below, this includes free/granted nodes:
    the decoder reads bits for *every* node in ``fullNodeOrder`` regardless of
    whether it is shown, so its lookup must be complete.
    """
    nodes = {}
    # ``g`` tags the tree a node belongs to so the client can group the decoded
    # build into Class / Spec / Hero panes; "sub" is the hero-tree selection node.
    groups = {
        "classNodes": "class", "specNodes": "spec",
        "heroNodes": "hero", "subTreeNodes": "sub",
    }
    for node_key, group in groups.items():
        for node in spec.get(node_key, []):
            entries = []
            for entry in node.get("entries", []):
                entries.append({
                    "name": entry.get("name", node.get("name", "")),
                    # subTree selection entries carry atlasMemberName, not icon.
                    "icon": entry.get("icon", entry.get("atlasMemberName", "")),
                    "spellId": entry.get("spellId", 0),
                    # active vs passive drives the client node shape (square vs circle).
                    "type": entry.get("type", ""),
                    "subTreeId": entry.get("traitSubTreeId"),
                })
            nodes[node["id"]] = {
                "name": node.get("name", ""),
                "g": group,
                "type": node.get("type", "single"),
                "maxRanks": node.get("maxRanks", 1),
                "subTreeId": node.get("subTreeId"),
                "free": bool(node.get("freeNode")),
                # Grid position + child links so the client can lay the tree out
                # and draw connector edges exactly like the spec page does.
                "x": node.get("posX", 0),
                "y": node.get("posY", 0),
                "next": node.get("next", []),
                "entries": entries,
            }
    return spec.get("fullNodeOrder", []), nodes


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
        mapping["fullNodeOrder"], mapping["nodes"] = build_decode_nodes(spec)
        for node_key in NODE_TYPES:
            for node in spec.get(node_key, []):
                node_id = node["id"]
                if node.get("freeNode"):
                    continue
                for entry in node.get("entries", []):
                    # Avoid overwriting if duplicate across node types
                    if node_id not in mapping["talents"]:
                        node_icon = entry.get("icon", node.get("icon", ""))
                        node_spell = entry.get("spellId", node.get("spellId", 0))
                        # Drop nameless selection placeholders that carry neither an
                        # icon nor a spell (e.g. the hero-tree selection stubs). They
                        # never render in the talent display and would only produce a
                        # broken data/icons/.png lookup in the spec overview image.
                        if node_icon or node_spell:
                            mapping["talents"][node_id] = {
                                "name": entry.get("name", node.get("name")),
                                "icon": node_icon,
                                "spellId": node_spell,
                            }

                    e_id = entry.get("definitionId")
                    if e_id and e_id not in mapping["talents"]:
                        e_icon = entry.get("icon", "")
                        e_spell = entry.get("spellId", 0)
                        if e_icon or e_spell:
                            mapping["talents"][e_id] = {
                                "name": entry.get("name", ""),
                                "icon": e_icon,
                                "spellId": e_spell,
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
