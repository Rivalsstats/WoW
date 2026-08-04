"""Async bridge over the synchronous ``databaseConnector`` module.

discord.py is async; databaseConnector is a synchronous ``mysql.connector`` pool.
Every DB call is run on a bounded thread pool as a single checkout -> configure ->
query -> release unit, so a pooled connection is *never* held across an await. The
mandatory ``configure_read_session`` is applied on every checkout — without it the
bot's idle connections would hold shared metadata locks that stall the nightly
``sp_swap_public_table`` RENAMEs (and get killed by ``sp_kill_lock_holders``).
"""

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

import databaseConnector

from .errors import DatabaseUnavailable

_executor: ThreadPoolExecutor | None = None
_sem: asyncio.Semaphore | None = None


def init_pool(env: dict) -> None:
    """Initialise the connection pool and executor. Call once before login."""
    global _executor, _sem
    pool_size = int(env.get("BOT_DB_POOL_SIZE", 5))
    databaseConnector.init_connection_pool(
        env["DATABASE_HOST"],
        env["DATABASE_USER"],
        env["DATABASE_PASSWORD"],
        env["DATABASE_NAME"],
        env["DATABASE_PORT"],
        pool_size=pool_size,
    )
    _executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="botdb")
    # Global brake: queue pile-ups in-process rather than at MySQL, which shares
    # a 2-core host with the collector.
    _sem = asyncio.Semaphore(8)


def _call(fn, args, kwargs, dictionary: bool):
    """Synchronous body executed on an executor thread.

    Full checkout -> configure -> query -> release. The connection is returned to
    the pool in the finally, so it is never held across the surrounding await.
    """
    conn = databaseConnector.get_live_connection()
    try:
        cursor = conn.cursor(dictionary=dictionary)
        try:
            databaseConnector.configure_read_session(conn, cursor)
            return fn(conn, cursor, *args, **kwargs)
        finally:
            cursor.close()
    finally:
        conn.close()  # returns the connection to the pool


async def run(fn, *args, dictionary: bool = False, timeout: float = 25.0, **kwargs):
    """Run a databaseConnector ``fetch_*`` function off the event loop.

    ``dictionary=True`` selects a dict cursor — required only for the dungeon-domain
    fetchers written against ``conn.cursor(dictionary=True)`` (e.g.
    ``fetch_dungeon_top_routes``); everything else indexes tuple rows.

    Any mysql.connector error or a timeout is re-raised as DatabaseUnavailable so
    the global error handler can render it. Note ``wait_for`` cannot cancel the
    worker thread; the session's lock_wait_timeout bounds the thread itself.
    """
    if _executor is None or _sem is None:
        raise DatabaseUnavailable("Database pool is not initialised.")
    loop = asyncio.get_running_loop()
    call = functools.partial(_call, fn, args, kwargs, dictionary)
    async with _sem:
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(_executor, call), timeout
            )
        except asyncio.TimeoutError as exc:
            raise DatabaseUnavailable("The database query timed out.") from exc
        except Exception as exc:  # mysql.connector.Error and friends
            raise DatabaseUnavailable() from exc
