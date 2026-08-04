"""Manual (re)provisioning of the bot's application emojis.

The running bot already self-provisions missing emojis on startup (see
``bot.on_ready`` → ``emojis.populate(..., create_missing=True)``). This CLI does the
same thing without starting the bot — handy for a one-off resync after adding new
role/meta images::

    python -m discord_bot.emoji_sync

Needs only ``DISCORD_BOT_TOKEN`` (no DB). Spec/class/buff images are downloaded from
the site's ``/data/icons`` endpoint; role/meta images are read from
``discord_bot/emoji_assets/``. Existing emojis are left untouched.
"""

import asyncio
import logging
import os

import discord

from . import emojis


def _load_token() -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is not set")
    return token


async def _run(token: str) -> None:
    client = discord.Client(intents=discord.Intents.none())
    async with client:
        await client.login(token)
        await emojis.populate(client, create_missing=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(_run(_load_token()))


if __name__ == "__main__":
    main()
