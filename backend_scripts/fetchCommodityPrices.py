import os
import time
from aiohttp import ClientSession, BasicAuth
import asyncio
import json
import math
from collections import defaultdict


REGIONS = os.environ.get("REGIONS", "us,eu,kr,tw").split(",")
API_BASE = "https://{region}.api.blizzard.com"
OAUTH_BASE = "https://eu.battle.net/oauth/token"
NAMESPACE_DYNAMIC = "dynamic-{region}"
LOCALE = os.environ.get("LOCALE", "en_US")

CLIENT_ID = os.getenv("BLIZ_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLIZ_CLIENT_SECRET")
OUTPUT_DIR = os.path.join("data", "static", "commodities")

if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError(
        "BLIZ_CLIENT_ID and BLIZ_CLIENT_SECRET must be set in the environment."
    )

_token_cache = {}


async def get_access_token(session: ClientSession, region: str) -> str:
    cache = _token_cache.get(region)
    if cache and cache["expires_at"] > time.time() + 3600:
        return cache["access_token"]
    url = OAUTH_BASE
    async with session.post(
        url,
        auth=BasicAuth(CLIENT_ID, CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        token = data["access_token"]
        expires = data.get("expires_in", 0)
        _token_cache[region] = {
            "access_token": token,
            "expires_at": time.time() + expires,
        }
        return token


async def fetch_and_save(session: ClientSession, region: str):
    # ensure output folder exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # get OAuth token
    token = await get_access_token(session, region)

    # build URL
    namespace = NAMESPACE_DYNAMIC.format(region=region)
    url = (
        f"{API_BASE.format(region=region)}/data/wow/auctions/commodities"
        f"?namespace={namespace}&locale={LOCALE}"
    )

    # fetch raw data
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(url, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()

    # group prices by item_id
    groups = defaultdict(list)
    for auction in data.get("auctions", []):
        item_id = auction["item"]["id"]
        price = auction["unit_price"]
        qty = auction["quantity"]
        groups[item_id].append((price, qty))

    # compute average of cheapest 5% per item
    avg_prices = {}
    for item_id, pq_list in groups.items():
        # sort by unit_price ascending
        pq_list.sort(key=lambda x: x[0])

        # total items available
        total_qty = sum(qty for _, qty in pq_list)
        target = max(1, math.ceil(total_qty * 0.05))

        acc_qty = 0
        acc_value = 0  # sum of price * qty for the slice

        for price, qty in pq_list:
            take = min(qty, target - acc_qty)
            acc_value += price * take
            acc_qty += take
            if acc_qty >= target:
                break

        # weighted average price, rounded to nearest int
        avg_prices[item_id] = round(acc_value / acc_qty)

    # write out the final mapping
    out_path = os.path.join(OUTPUT_DIR, f"{region}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(avg_prices, f, ensure_ascii=False, indent=2)

    print(f"→ Saved averaged commodities for {region} to {out_path}")


async def main():
    async with ClientSession() as session:
        # spawn one task per region, run them concurrently
        tasks = [fetch_and_save(session, region) for region in REGIONS]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
