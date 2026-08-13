"""Shared pre-season gate.

Two independent systems need to answer the exact same question — "has the current
season actually started, or are we in the pre-season gap / just-after-a-wipe
window where the ``runs`` table is still empty for it?":

* the Discord bot's season-not-started guard (``discord_bot/guards.py``), which
  short-circuits every command to a schedule embed; and
* the social-media auto-poster (``social_posts/pipeline.py``), which otherwise
  renders 0-runs-tracked empty cards during the gap and posts a release
  countdown instead.

Keeping the decision here, over the single ``season_has_runs`` query, means the
two can never drift apart on *when* they flip to their "season not started yet"
presentations. Both the bot (which runs with ``backend_scripts`` on its path) and
the poster import this module.
"""

import databaseConnector


def season_has_started(connection, cursor, season) -> bool:
    """True once the season has any recorded runs — i.e. it is underway. False
    during the pre-season gap or just after a season wipe, when the ``runs``
    table holds nothing for it yet. Thin semantic wrapper over
    ``databaseConnector.season_has_runs`` so every consumer gates on the
    identical query.
    """
    return bool(databaseConnector.season_has_runs(connection, cursor, season))
