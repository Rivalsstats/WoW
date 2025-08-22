import os
import json
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load talents
with open("data/static/talents.json", "r") as f:
    talents = json.load(f)


# Recursively collect atlasMemberName and icon values
def collect_icon_names(obj):
    atlas_names = set()
    spell_icons = set()
    if isinstance(obj, dict):
        if "atlasMemberName" in obj:
            atlas_names.add(obj["atlasMemberName"])
        if "icon" in obj:
            spell_icons.add(obj["icon"])
        for v in obj.values():
            a, s = collect_icon_names(v)
            atlas_names.update(a)
            spell_icons.update(s)
    elif isinstance(obj, list):
        for item in obj:
            a, s = collect_icon_names(item)
            atlas_names.update(a)
            spell_icons.update(s)
    return atlas_names, spell_icons


atlas_names, spell_icons = collect_icon_names(talents)

# Prepare output directories
os.makedirs("data/icons", exist_ok=True)


# Download functions
def download_atlas_icon(name):
    url = f"https://www.raidbots.com/static/images/TalentFrame/orig/elements/{name}.png"
    dest = f"data/icons/{name}.png"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as response, open(dest, "wb") as out_file:
            out_file.write(response.read())
        return f"Atlas Downloaded: {name}"
    except urllib.error.HTTPError as e:
        return f"Atlas Failed ({e.code}) for {name}"
    except Exception as e:
        return f"Atlas Error for {name}: {e}"


def download_spell_icon(name):
    url = f"https://www.raidbots.com/static/images/icons/56/{name}.png"
    dest = f"data/icons/{name}.png"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as response, open(dest, "wb") as out_file:
            out_file.write(response.read())
        return f"Spell Downloaded: {name}"
    except urllib.error.HTTPError as e:
        return f"Spell Failed ({e.code}) for {name}"
    except Exception as e:
        return f"Spell Error for {name}: {e}"


# Parallel download of all icons
with ThreadPoolExecutor(max_workers=30) as executor:
    # submit atlas downloads
    atlas_futures = {
        executor.submit(download_atlas_icon, name): name for name in atlas_names
    }
    # submit spell downloads
    spell_futures = {
        executor.submit(download_spell_icon, name): name for name in spell_icons
    }

    for future in as_completed(list(atlas_futures) + list(spell_futures)):
        print(future.result())
