import os, json, requests

# ensure output dir exists
os.makedirs("data/icons", exist_ok=True)

# load enchantments
with open("data/static/enchantments.json", "r") as f:
    enchants = json.load(f)

icon_names = {}
quality_names = {}

for e in enchants:
    eid = e.get("id")
    # Enchants
    icon_name = e.get("spellIcon")
    if icon_name:
        icon_names[icon_name] = icon_name

    # Gems
    item_icon = e.get("itemIcon")
    if item_icon:
        icon_names[item_icon] = item_icon

    quality = e.get("craftingQuality")
    if quality:
        quality_name = f"crafting-quality-{quality}"
        if quality_name not in quality_names:
            quality_names[quality_name] = quality_name

for icon_name in icon_names:
    url = f"https://www.raidbots.com/static/images/icons/56/{icon_name}.png"
    resp = requests.get(url)
    if resp.status_code == 200:
        print(f"Saved {icon_name}")
        with open(f"data/icons/{icon_name}.png", "wb") as out:
            out.write(resp.content)

for icon_name in quality_names:
    url = f"https://www.raidbots.com/images/{icon_name}.png"
    resp = requests.get(url)
    if resp.status_code == 200:
        print(f"Saved {icon_name}")
        with open(f"data/icons/{icon_name}.png", "wb") as out:
            out.write(resp.content)
