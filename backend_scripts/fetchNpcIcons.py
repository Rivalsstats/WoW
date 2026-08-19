"""Fetch per-NPC portrait icons for the dungeon page.

Two hops, both over CDNs that are NOT Cloudflare-blocked server-side:

1. npc_id -> displayId, from Mythic Dungeon Tools' static dungeon Lua files
   (github.com/Nnoggie/MythicDungeonTools). Every enemy entry ships `["id"]` and
   `["displayId"]`, covering ~100% of current-season NPCs, read off GitHub's raw CDN.

2. displayId -> image, from Wowhead's zamimg model-thumbnail CDN:
   https://wow.zamimg.com/modelviewer/live/webthumbs/npc/<displayId % 256>/<displayId>.png
   These are Wowhead's own offline model renders (300x300 transparent PNG) and cover
   100% of current displayIds, including the newest creatures that Blizzard's official
   render.worldofwarcraft.com CDN does NOT have. Downloaded into data/icons/npc_<npc_id>.png
   (keyed by npc_id so the template references it straight from the npc id it has).

Missing thumbs (non-200) are skipped; the template falls back to text-only.
"""

import os
import re
import json
import asyncio
from io import BytesIO
import aiohttp
import requests
from PIL import Image

STATIC_DIR = os.path.join("data", "static")
ICON_DIR = os.path.join("data", "icons")
NPCS_PATH = os.path.join(STATIC_DIR, "npcs.json")
DISPLAY_IDS_PATH = os.path.join(STATIC_DIR, "npc_display_ids.json")

MDT_REPO = "Nnoggie/MythicDungeonTools"
MDT_BRANCH = "master"
TREE_URL = f"https://api.github.com/repos/{MDT_REPO}/git/trees/{MDT_BRANCH}?recursive=1"
RAW_URL = "https://raw.githubusercontent.com/{repo}/{branch}/{path}"
# Wowhead zamimg model thumbnail, bucketed by displayId % 256.
WEBTHUMB_URL = "https://wow.zamimg.com/modelviewer/live/webthumbs/npc/{bucket}/{display_id}.png"

# Enemy tables carry `["id"]` and `["displayId"]`; clones/spells never use those keys.
ID_RE = re.compile(r'\["id"\]\s*=\s*(\d+)')
DISPLAY_RE = re.compile(r'\["displayId"\]\s*=\s*(\d+)')
LOAD_XML_RE = re.compile(r'^[^/]+/load_[^/]*\.xml$')
TEXTURES_RE = re.compile(r'^[^/]+/Textures/')

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
SEM_LIMIT = 50  # concurrent image downloads


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_display_ids(display_ids):
    os.makedirs(STATIC_DIR, exist_ok=True)
    with open(DISPLAY_IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(display_ids, f, indent=2, sort_keys=True)


def parse_lua(text):
    """Extract { "<npc_id>": <display_id> } from one MDT dungeon Lua file.

    For each `["id"] = N` we take the `["displayId"] = M` that appears before the
    next `["id"]`, so an enemy missing a displayId never steals the next one's.
    """
    out = {}
    ids = list(ID_RE.finditer(text))
    for i, m in enumerate(ids):
        end = ids[i + 1].start() if i + 1 < len(ids) else len(text)
        dm = DISPLAY_RE.search(text, m.end(), end)
        if dm:
            out[m.group(1)] = int(dm.group(1))
    return out


def mdt_lua_urls():
    """Raw URLs of every dungeon Lua in MDT's active expansion folder(s).

    An expansion data folder is a top-level dir that has both a `load_*.xml` and a
    `Textures/` subfolder (Midnight today); framework folders (libs, Modules, ...)
    have a load xml but no Textures, so they are excluded. This auto-follows MDT
    when it renames the folder for the next expansion.
    """
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "mythistone-npc-icons"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    resp = requests.get(TREE_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    tree = resp.json()
    if tree.get("truncated"):
        raise RuntimeError("MDT git tree response was truncated; cannot enumerate dungeon files.")

    paths = [t["path"] for t in tree.get("tree", []) if t.get("type") == "blob"]
    dirs_with_load = {p.split("/")[0] for p in paths if LOAD_XML_RE.match(p)}
    dirs_with_textures = {p.split("/")[0] for p in paths if TEXTURES_RE.match(p)}
    exp_dirs = dirs_with_load & dirs_with_textures
    if not exp_dirs:
        raise RuntimeError("Could not locate any MDT expansion dungeon folder.")

    lua_paths = [
        p for p in paths
        if p.split("/")[0] in exp_dirs and p.endswith(".lua") and p.count("/") == 1
    ]
    print(f"MDT expansion folder(s): {sorted(exp_dirs)} -> {len(lua_paths)} dungeon Lua files")
    return [RAW_URL.format(repo=MDT_REPO, branch=MDT_BRANCH, path=p) for p in lua_paths]


def build_display_map(npc_id_set=None):
    """npc_id -> displayId harvested from MDT dungeon files.

    Pass an npc_id_set to keep only those npcs; pass None to return the full map
    (used by on-demand callers that filter afterwards).
    """
    result = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "mythistone-npc-icons"})
    for url in mdt_lua_urls():
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        for npc_id, display_id in parse_lua(resp.text).items():
            if npc_id_set is None or npc_id in npc_id_set:
                result[npc_id] = display_id
    return result


async def fetch_image(session, sem, npc_id, display_id, prev):
    out_path = os.path.join(ICON_DIR, f"npc_{npc_id}.png")
    # Skip only when we already have the file for this exact displayId; a changed
    # displayId (MDT updated the model) re-downloads.
    if os.path.exists(out_path) and prev.get(npc_id) == display_id:
        return
    url = WEBTHUMB_URL.format(bucket=display_id % 256, display_id=display_id)
    async with sem:
        try:
            async with session.get(url, timeout=20) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    # The webthumb is a 300x300 frame with lots of transparent padding
                    # (the model fills a median ~43% of it), so trim to the model's
                    # bounding box; the template then shows it much larger at the same
                    # box size via object-fit: contain.
                    img = Image.open(BytesIO(content)).convert("RGBA")
                    bbox = img.getchannel("A").getbbox()
                    if bbox:
                        img = img.crop(bbox)
                    img.save(out_path, "PNG")
                    print(f"  saved npc_{npc_id}.png (display {display_id})")
                else:
                    print(f"  no thumb for npc {npc_id} (display {display_id}): HTTP {resp.status}")
        except Exception as e:
            print(f"  error downloading npc {npc_id} (display {display_id}): {e}")


async def download_images(display_ids, prev):
    os.makedirs(ICON_DIR, exist_ok=True)
    print(f"Downloading portraits for {len(display_ids)} npcs with display ids...")
    sem = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        tasks = [
            asyncio.create_task(fetch_image(session, sem, npc_id, display_id, prev))
            for npc_id, display_id in display_ids.items()
        ]
        await asyncio.gather(*tasks)


def main():
    npcs = load_json(NPCS_PATH, {})
    npc_ids = set(npcs.get("en_US", {}).keys())
    if not npc_ids:
        print(f"No npc ids in {NPCS_PATH}; nothing to do.")
        return

    prev = load_json(DISPLAY_IDS_PATH, {})
    display_ids = build_display_map(npc_ids)
    print(f"Mapped {len(display_ids)}/{len(npc_ids)} npcs to display ids from MDT.")
    save_display_ids(display_ids)
    asyncio.run(download_images(display_ids, prev))


if __name__ == "__main__":
    main()
