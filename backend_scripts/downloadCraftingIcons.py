import os
import json
import requests

# Ensure output dir exists
os.makedirs("data/icons", exist_ok=True)

# Load crafting data
with open("data/static/crafting.json", "r") as f:
    crafting = json.load(f)

# Collect all unique icon names from reagents
icon_names = set()

for reagent in crafting.get("reagents", []):
    icon = reagent.get("icon")
    if icon:
        icon_names.add(icon)

# Download all icons
for icon_name in icon_names:
    url = f"https://www.raidbots.com/static/images/icons/56/{icon_name}.png"
    resp = requests.get(url)
    if resp.status_code == 200:
        print(f"Saved {icon_name}")
        with open(f"data/icons/{icon_name}.png", "wb") as out:
            out.write(resp.content)
    else:
        print(f"Failed to download {icon_name} (status {resp.status_code})")
