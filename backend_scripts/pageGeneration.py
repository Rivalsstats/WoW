ROLE_FOLDERS = {
    "0": "Tank",
    "1": "Healer",
    "2": "Dps",
}

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