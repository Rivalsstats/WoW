import re

ROLE_FOLDERS = {
    "0": "Tank",
    "1": "Healer",
    "2": "Dps",
}


def slugify(text):
    """Turn an item name into a URL slug (lowercase, hyphen-separated).

    Mirrors the dungeon slug style: apostrophes are dropped (so "Flarendo's"
    -> "flarendos"), every other run of non-alphanumeric characters collapses
    to a single hyphen, and leading/trailing hyphens are trimmed.
    """
    text = (text or "").lower().replace("'", "").replace("’", "")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def build_item_slug_map(item_lookup):
    """Map every item id to a URL slug derived from its name.

    Item names are not guaranteed unique. When a slug is shared by more than one
    item, *all* of the colliding items get ``<slug>-<id>`` so the result is
    unambiguous and independent of iteration order (a stable map for a given set
    of items). Built purely from item names, so any generator that loads the same
    equippable-items lookup produces an identical map regardless of run order.
    """
    base = {}
    counts = {}
    for iid, item in item_lookup.items():
        slug = slugify(item.get("name", "")) or str(iid)
        base[iid] = slug
        counts[slug] = counts.get(slug, 0) + 1
    return {
        iid: (f"{slug}-{iid}" if counts[slug] > 1 else slug)
        for iid, slug in base.items()
    }

def generateDungeonNav(dungeons):
    dungeon_nav = []
    for d_id, d_data in dungeons.items():
        dungeon_nav.append({
            "name": d_data["name"]["en_US"],
            "url": f"/dungeons/{d_data['slug']}",
            "icon": d_data.get("icon", None),
        })
    dungeon_nav.sort(key=lambda x: x["name"])
    return dungeon_nav

def generateSpecNav(spec_lookup, class_lookup):
    # Build a dict mapping role names to lists of specs
    spec_nav = {role_name: [] for role_name in ROLE_FOLDERS.values()}

    for sid, sdata in spec_lookup.items():
        role_key = str(sdata.get("role", 2))
        role_name = ROLE_FOLDERS.get(role_key, "Other")
        class_data = class_lookup.get(str(sdata.get("classID", "")), {})
        filename = f"{sdata['name']}_{class_data.get('name')}"
        spec_nav[role_name].append(
            {
                "name": f"{sdata['name']} {class_data.get('name')}",
                "url": f"/classes/{role_name}/{filename}",
                "icon": sdata.get("SpellIconFileId"),
                "class": class_data.get("name", "Unknown").replace(" ", ""),
            }
        )

    # Optionally sort each list by name:
    for lst in spec_nav.values():
        lst.sort(key=lambda x: x["name"])

    return spec_nav
