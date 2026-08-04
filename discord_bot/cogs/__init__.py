"""Cog modules loaded by the bot, in load order.

Kept as an explicit list (no filesystem discovery) so the set of shipped commands
is auditable and a stray file can never register itself.
"""

COG_MODULES = [
    "season",
    "meta",
    "spec",
    "dungeon",
    "comps",
    "routes",
    "items",
    "stats",
    "analyze",
]
