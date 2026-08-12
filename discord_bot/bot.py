"""Bot entrypoint: intents, cog loading, command-tree sync and lifecycle."""

import hashlib
import logging
import os

import aiohttp
import discord
from discord.ext import commands, tasks

from . import cache, cogs, config, db, emojis, errors, guards, site_data

log = logging.getLogger("mythistone.bot")


class MythistoneBot(commands.Bot):
    def __init__(self, env: dict):
        intents = discord.Intents.none()
        intents.guilds = True  # slash commands only; no message content
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.env = env
        self.webhook_url = env.get("WEBHOOK_URL")
        self.site_data: site_data.SiteData | None = None

    async def setup_hook(self):
        self.site_data = site_data.SiteData(aiohttp.ClientSession())
        for module in cogs.COG_MODULES:
            await self.load_extension(f"discord_bot.cogs.{module}")
        self.tree.on_error = errors.on_app_command_error
        # Global gate: during the pre-season gap / just after a wipe, every command
        # short-circuits to the "season hasn't started" embed instead of erroring.
        self.tree.interaction_check = guards.season_guard
        await self._sync_tree()
        self.prune_loop.start()

    async def _sync_tree(self):
        """Sync globally, but only when the command surface actually changed.

        Global syncs are rate-limited by Discord, so we hash the command tree
        (names + parameter names) and skip the sync when it matches the last one.
        """
        signature = []
        for command in self.tree.walk_commands():
            params = sorted(p.name for p in getattr(command, "parameters", []))
            signature.append((command.qualified_name, tuple(params)))
        signature.sort()
        digest = hashlib.sha256(repr(signature).encode("utf-8")).hexdigest()

        previous = None
        try:
            with open(config.TREE_HASH_FILE, "r", encoding="utf-8") as fh:
                previous = fh.read().strip()
        except FileNotFoundError:
            pass

        if self.env.get("BOT_FORCE_SYNC") or digest != previous:
            synced = await self.tree.sync()
            os.makedirs(os.path.dirname(config.TREE_HASH_FILE), exist_ok=True)
            with open(config.TREE_HASH_FILE, "w", encoding="utf-8") as fh:
                fh.write(digest)
            log.info("synced %d global commands", len(synced))
        else:
            log.info("command tree unchanged; skipping sync")

    @tasks.loop(hours=6)
    async def prune_loop(self):
        cache.prune_charts()

    @prune_loop.before_loop
    async def _before_prune(self):
        await self.wait_until_ready()

    async def on_ready(self):
        log.info("logged in as %s (id=%s)", self.user, getattr(self.user, "id", "?"))
        session = self.site_data.session if self.site_data else None
        await emojis.populate(self, session=session, create_missing=True)

    async def close(self):
        if self.site_data is not None:
            await self.site_data.session.close()
        await super().close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    env = config.load_env()
    os.makedirs(config.CHART_CACHE_DIR, exist_ok=True)
    db.init_pool(env)
    bot = MythistoneBot(env)
    bot.run(env["DISCORD_BOT_TOKEN"], log_handler=None)
