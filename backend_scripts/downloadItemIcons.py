import os
import json
import asyncio
import aiohttp
import aiofiles

# ensure output dir exists
os.makedirs("data/icons", exist_ok=True)

# load equippable-items list
with open("data/static/equippable-items.json", "r") as f:
    items = json.load(f)

icon_names = {itm["icon"] for itm in items if itm.get("icon")}

SEM_LIMIT = 100  # max concurrent fetches


async def fetch_and_save(
    session: aiohttp.ClientSession, sem: asyncio.Semaphore, icon: str
):
    url = f"https://www.raidbots.com/static/images/icons/56/{icon}.png"
    out_path = os.path.join("data", "icons", f"{icon}.png")
    async with sem:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    async with aiofiles.open(out_path, "wb") as out_f:
                        await out_f.write(content)
                    print(f"Saved {icon}")
                else:
                    print(f"⚠️  Failed to fetch {icon}: HTTP {resp.status}")
        except Exception as e:
            print(f"⚠️  Error fetching {icon}: {e}")


async def main():
    sem = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(fetch_and_save(session, sem, icon))
            for icon in icon_names
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
