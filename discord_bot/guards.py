"""Global pre-command guard for the pre-season gap.

When the current season has no runs yet (the 1-4 week gap between seasons, or the
window just after a season wipe), DB-backed commands would otherwise error on
empty data. This tree-wide ``interaction_check`` short-circuits every command to a
friendly "season hasn't started" embed (built in ``errors.on_app_command_error``)
instead. The check is cached briefly so it does not query the DB on every command.
"""

import logging

import databaseConnector

from . import cache, config, db
from .errors import SeasonNotStarted

log = logging.getLogger("mythistone.bot")


@cache.ttl_cache(300)
async def _season_has_runs() -> bool:
    return bool(await db.run(databaseConnector.season_has_runs, config.SEASON))


async def season_guard(interaction) -> bool:
    """Tree-wide interaction_check. Returns True to let the command run; raises
    SeasonNotStarted (routed to on_app_command_error) when the season has no runs
    yet. Fail-open on any check error so a transient DB blip does not block every
    command — the command's own db.run then surfaces the real DB error."""
    try:
        has_runs = await _season_has_runs()
    except Exception:
        log.debug("season guard check failed; allowing command through", exc_info=True)
        return True
    if not has_runs:
        raise SeasonNotStarted()
    return True
