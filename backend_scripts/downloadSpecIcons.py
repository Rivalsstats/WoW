import json
import csv
import os
import requests

with open("data/static/specs.json", "r") as f:
    specs = json.load(f)

iface_map = {}
with open("data/static/interfacedata.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # row["ID"], row["FileName"]
        iface_map[row["ID"]] = row["FileName"]

os.makedirs("data/icons", exist_ok=True)
for spec_id, info in specs.items():
    file_id = info.get("SpellIconFileId")
    if not file_id or file_id not in iface_map:
        print(f"No interface entry for ID {file_id}")
        continue

    raw_name = iface_map[file_id]
    jpg_name = raw_name.lower().replace(".blp", ".jpg")
    url = f"{os.environ['CDN_BASE']}/{jpg_name}"

    dest = f"data/icons/{file_id}.jpg"
    if os.path.exists(dest):
        continue

    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        with open(dest, "wb") as out:
            for chunk in resp.iter_content(1024):
                out.write(chunk)
        print(f"downloaded {file_id}.jpg")
    else:
        print(f"failed {file_id} → {url} (HTTP {resp.status_code})")
