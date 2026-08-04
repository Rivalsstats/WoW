"""/season — current season overview."""

import datetime

import commonUtils
import databaseConnector
import discord
from discord import app_commands
from discord.ext import commands

from .. import cache, config, db, embeds


@cache.ttl_cache(3600)
async def _get_total_runs():
    return await db.run(databaseConnector.fetch_total_season_runs, config.SEASON)


@cache.ttl_cache(3600)
async def _get_max_key_run():
    return await db.run(databaseConnector.fetch_max_key_run, config.SEASON)


def _discord_ts(iso: str, style: str = "D") -> str:
    """ISO 8601 -> a Discord timestamp tag that renders in the viewer's timezone."""
    if not iso:
        return "—"
    ts = int(datetime.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    return f"<t:{ts}:{style}>"


def build_season_embed(total_runs, max_run) -> discord.Embed:
    info = config.SEASON_INFO
    title = config.SEASON_NAME
    if config.SEASON_SHORT:
        title = f"{title} ({config.SEASON_SHORT})"
    embed = embeds.base_embed(title, url=f"{config.SITE_BASE}/pages/dashboard")

    starts = info.get("starts", {})
    ends = info.get("ends", {})
    schedule = "\n".join(
        f"{region.upper()}: {_discord_ts(starts.get(region))} → {_discord_ts(ends.get(region))}"
        for region in ("us", "eu", "kr")
        if region in starts
    )

    pool = " · ".join(
        f"[{d['name']}]({config.SITE_BASE}/dungeons/{d['slug']})"
        for d in info.get("dungeons", [])
    )

    fields = [
        ("Total runs tracked", commonUtils.humanize_number(total_runs or 0), True),
        ("Schedule (start → end)", schedule, False),
        ("Dungeon pool", pool, False),
    ]
    if max_run:
        fields.append(("Highest key this season", embeds.run_lines(max_run, icon_comp=True), False))
    embeds.add_fields_capped(embed, fields)
    return embed


class SeasonCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    season = app_commands.Group(name="season", description="Current Mythic+ season info")

    @season.command(name="info", description="Overview of the current Mythic+ season")
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def info(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        total_runs = await _get_total_runs()
        max_run = await _get_max_key_run()
        await interaction.followup.send(embed=build_season_embed(total_runs, max_run))


async def setup(bot):
    await bot.add_cog(SeasonCog(bot))
