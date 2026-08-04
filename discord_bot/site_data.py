"""TTL-cached fetch of the published site JSON artifacts.

The comp/route/item/spec-meta features are backed by JSON the CI pipeline
computes and publishes to mythistone.com, so the bot fetches those rather than
recomputing the heavy math. Each artifact is cached for its own TTL and refreshed
with a conditional request (If-None-Match). Per the fail-loudly rule, a fetch with
no cached copy raises SiteDataError; a fetch that fails while a stale copy exists
returns the stale copy (real data, logged once) rather than a silent empty.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from . import config
from .errors import SiteDataError

log = logging.getLogger("mythistone.bot")


@dataclass
class RouteIndexes:
    """Inverted indexes over compRoutes, mirroring comp-routes-worker.js."""

    route_meta: dict = field(default_factory=dict)          # route_key -> meta dict
    spec_index: dict = field(default_factory=dict)          # spec_id(int) -> {route_key}
    dungeon_index: dict = field(default_factory=dict)       # dungeon(str) -> {route_key}


class SiteData:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        # name -> (fetched_at_monotonic, data, etag)
        self._cache: dict[str, tuple[float, object, str | None]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._stale_warned: dict[str, float] = {}
        self._route_indexes: RouteIndexes | None = None
        self._route_indexes_stamp: float | None = None
        self._item_by_id: dict | None = None
        self._item_by_id_stamp: float | None = None

    async def get(self, name: str):
        url, ttl = config.artifact_url_ttl(name)
        now = time.monotonic()
        cached = self._cache.get(name)
        if cached is not None and (now - cached[0]) < ttl:
            return cached[1]

        lock = self._locks.setdefault(name, asyncio.Lock())
        async with lock:
            cached = self._cache.get(name)
            now = time.monotonic()
            if cached is not None and (now - cached[0]) < ttl:
                return cached[1]
            return await self._fetch(name, url, ttl, cached)

    async def _fetch(self, name, url, ttl, cached):
        etag = cached[2] if cached else None
        headers = {"If-None-Match": etag} if etag else {}
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 304 and cached is not None:
                    self._cache[name] = (time.monotonic(), cached[1], etag)
                    return cached[1]
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    new_etag = resp.headers.get("ETag")
                    self._cache[name] = (time.monotonic(), data, new_etag)
                    return data
                # Any other status: fall through to stale/raise handling.
                raise SiteDataError(
                    f"Couldn't load {name}: the site returned HTTP {resp.status}."
                )
        except SiteDataError:
            if cached is not None:
                self._warn_stale(name)
                return cached[1]
            raise
        except Exception as exc:
            if cached is not None:
                self._warn_stale(name)
                return cached[1]
            raise SiteDataError(f"Couldn't reach {name} data right now.") from exc

    def _warn_stale(self, name):
        last = self._stale_warned.get(name, 0.0)
        now = time.monotonic()
        if now - last > 3600:
            log.warning("serving stale site artifact %s (refresh failed)", name)
            self._stale_warned[name] = now

    # --- typed accessors ---------------------------------------------------
    async def comps_index(self) -> list:
        return await self.get("comps_index")

    async def comp_routes(self) -> dict:
        return await self.get("comp_routes")

    async def items_index(self) -> list:
        return await self.get("items_index")

    async def gem_enchant_index(self) -> dict:
        return await self.get("gem_enchant_index")

    async def simdps_tierlist(self) -> dict:
        return await self.get("simdps_tierlist")

    async def spec_meta(self, spec_id) -> dict:
        return await self.get(f"spec_meta/{spec_id}")

    async def item_by_id(self) -> dict:
        items = await self.items_index()
        stamp = self._cache.get("items_index", (None,))[0]
        if self._item_by_id is None or self._item_by_id_stamp != stamp:
            self._item_by_id = {int(it["id"]): it for it in items}
            self._item_by_id_stamp = stamp
        return self._item_by_id

    async def comp_routes_indexes(self) -> RouteIndexes:
        routes = await self.comp_routes()
        stamp = self._cache.get("comp_routes", (None,))[0]
        if self._route_indexes is not None and self._route_indexes_stamp == stamp:
            return self._route_indexes

        idx = RouteIndexes()
        for key, meta in routes.items():
            idx.route_meta[key] = meta
            for spec_id in meta.get("specs", []):
                idx.spec_index.setdefault(int(spec_id), set()).add(key)
            dungeon = str(meta.get("dungeon"))
            idx.dungeon_index.setdefault(dungeon, set()).add(key)
        self._route_indexes = idx
        self._route_indexes_stamp = stamp
        return idx
