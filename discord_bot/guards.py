"""Global pre-command guard for the pre-season gap.

When the current season has no runs yet (the 1-4 week gap between seasons, or the
window just after a season wipe), DB-backed commands would otherwise error on
empty data. This tree-wide ``interaction_check`` short-circuits every command to a
friendly "season hasn't started" embed (built in ``errors.on_app_command_error``)
instead. The check is cached briefly so it does not query the DB on every command.
"""

import logging

import season_gate
from image_generation.season_countdown import in_launch_window

from . import cache, config, db
from .errors import SeasonJustStarted, SeasonNotStarted

log = logging.getLogger("mythistone.bot")


@cache.ttl_cache(300)
async def _season_has_runs() -> bool:
    # Shared gate with the social auto-poster (backend_scripts/season_gate.py) so
    # both flip to their "season not started yet" presentations on the same query.
    return bool(await db.run(season_gate.season_has_started, config.SEASON))


async def season_guard(interaction) -> bool:
    """Tree-wide interaction_check. Returns True to let the command run; otherwise
    raises (routed to on_app_command_error) to short-circuit to a friendly embed:
    SeasonJustStarted during the season's first 24h (data still too sparse), or
    SeasonNotStarted when the season has no runs yet. Fail-open on any DB check
    error so a transient blip does not block every command — the command's own
    db.run then surfaces the real DB error."""
    # Launch-day window: pure time + seasonInfo (no DB), shared with the social
    # auto-poster's launch-day post. Checked first and outside the fail-open DB
    # try/except so it is never swallowed, and so it wins the moment a region is
    # live even though the first runs already flip _season_has_runs to True.
    try:
        launching = in_launch_window(config.SEASON_INFO)
    except Exception:
        log.debug("launch-window check failed; ignoring", exc_info=True)
        launching = False
    if launching:
        raise SeasonJustStarted()

    try:
        has_runs = await _season_has_runs()
    except Exception:
        log.debug("season guard check failed; allowing command through", exc_info=True)
        return True
    if not has_runs:
        raise SeasonNotStarted()
    return True
