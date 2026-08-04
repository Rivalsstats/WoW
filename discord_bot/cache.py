"""In-memory TTL cache for DB results and disk-cache paths for rendered charts.

Aggregated data only changes once per nightly pipeline run, so a short in-process
TTL on the ``_get_*`` DB helpers collapses a burst of identical commands into one
query, and rendered charts are keyed by the nightly ``data_date`` so each unique
chart is drawn at most once per day.
"""

import asyncio
import functools
import os
import time

from . import config

_locks: dict = {}
_store: dict = {}


def ttl_cache(ttl: float):
    """Decorator for async functions returning immutable-by-convention data.

    Single-flight per key via an asyncio.Lock, so concurrent identical calls do a
    single underlying query. Callers must not mutate returned lists/dicts.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            key = (func.__qualname__, args, tuple(sorted(kwargs.items())))
            now = time.monotonic()
            cached = _store.get(key)
            if cached is not None and cached[0] > now:
                return cached[1]
            lock = _locks.setdefault(key, asyncio.Lock())
            async with lock:
                # Re-check after acquiring the lock: another waiter may have
                # populated it while we blocked.
                cached = _store.get(key)
                now = time.monotonic()
                if cached is not None and cached[0] > now:
                    return cached[1]
                value = await func(*args, **kwargs)
                _store[key] = (now + ttl, value)
                return value

        return wrapper

    return decorator


def chart_path(chart: str, params: str) -> str:
    """Filesystem path for a rendered chart, keyed to the nightly data cycle.

    ``params`` must already be a filesystem-safe slug built from validated ids.
    """
    return os.path.join(
        config.CHART_CACHE_DIR, f"{chart}_{params}_{config.data_date()}.png"
    )


def prune_charts(keep_days: int = 3) -> None:
    """Delete cached chart PNGs older than keep_days (by mtime)."""
    cutoff = time.time() - keep_days * 86400
    directory = config.CHART_CACHE_DIR
    try:
        entries = os.listdir(directory)
    except FileNotFoundError:
        return
    for name in entries:
        path = os.path.join(directory, name)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
        except OSError:
            continue
