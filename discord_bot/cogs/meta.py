"""/meta — season-wide tier lists, sim rankings and activity charts."""

import functools
import json
import os

import databaseConnector
import discord
from discord import app_commands
from discord.ext import commands

from .. import cache, charts, config, db, embeds, social_render


def _load_periods():
    path = os.path.join(config.STATIC_DIR, "periods.json")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _chart_embed(title):
    embed = embeds.base_embed(title, url=f"{config.SITE_BASE}/pages/dashboard")
    embed.set_image(url="attachment://chart.png")
    return embed


@cache.ttl_cache(3600)
async def _get_spec_upgrades():
    return await db.run(databaseConnector.fetch_spec_upgrades)


@cache.ttl_cache(3600)
async def _get_runs_per_dungeon_per_level():
    return await db.run(databaseConnector.fetch_runs_per_dungeon_per_level, config.SEASON)


@cache.ttl_cache(3600)
async def _get_total_runs():
    return await db.run(databaseConnector.fetch_total_season_runs, config.SEASON)


SIMDPS_TARGET_CHOICES = [
    app_commands.Choice(name="1 target", value=1),
    app_commands.Choice(name="3 targets", value=3),
    app_commands.Choice(name="5 targets", value=5),
    app_commands.Choice(name="8 targets", value=8),
]


class MetaCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    meta = app_commands.Group(name="meta", description="Season meta: tier lists & rankings")

    @meta.command(name="specs", description="Spec tier list")
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def specs(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("spec_tierlist", "global")
        if path is None:
            spec_upgrades = await _get_spec_upgrades()
            total_runs = await _get_total_runs()
            path = await charts.render("spec_tierlist", "global", functools.partial(
                social_render.spec_tierlist_builder,
                spec_upgrades=spec_upgrades, total_runs=total_runs,
            ))
        await embeds.respond(
            interaction,
            _chart_embed("Spec tier list"),
            file=discord.File(path, filename="chart.png"),
        )

    @meta.command(name="dungeons", description="Dungeon difficulty tier list")
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def dungeons(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("dungeon_tierlist", "global")
        if path is None:
            dungeon_data = await _get_runs_per_dungeon_per_level()
            total_runs = await _get_total_runs()
            path = await charts.render("dungeon_tierlist", "global", functools.partial(
                social_render.dungeon_tierlist_builder,
                dungeon_data=dungeon_data, total_runs=total_runs,
            ))
        await embeds.respond(
            interaction,
            _chart_embed("Dungeon tier list"),
            file=discord.File(path, filename="chart.png"),
        )

    @meta.command(name="simdps", description="Simulated DPS tier list (by target count)")
    @app_commands.describe(targets="Number of enemies to sim against (default 1)")
    @app_commands.choices(targets=SIMDPS_TARGET_CHOICES)
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def simdps(self, interaction: discord.Interaction, targets: int = 1):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("simdps", f"t{targets}")
        if path is None:
            data = await self.bot.site_data.simdps_tierlist()
            rows = (data.get("tabs") or {}).get(str(targets)) or []
            if not rows:
                embed = embeds.base_embed(
                    "Simulated DPS tier list", url=f"{config.SITE_BASE}/pages/tierlist"
                )
                embed.description = f"No sim data for {targets} target(s) yet."
                await embeds.respond(interaction, embed)
                return
            path = await charts.render("simdps", f"t{targets}", functools.partial(
                social_render.simdps_builder, rows=rows, targets=targets,
            ))
        await embeds.respond(
            interaction,
            _chart_embed("Simulated DPS tier list"),
            file=discord.File(path, filename="chart.png"),
        )

    @meta.command(name="popularity", description="Spec popularity vs performance chart")
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def popularity(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("spec_perf", "global")
        if path is None:
            spec_upgrades = await _get_spec_upgrades()
            max_run = await db.run(databaseConnector.fetch_max_key_run, config.SEASON)
            path = await charts.render("spec_perf", "global", functools.partial(
                social_render.scatter_builder,
                spec_upgrades=spec_upgrades, highest_run=max_run or {},
            ))
        await embeds.respond(
            interaction,
            _chart_embed("Spec popularity vs performance"),
            file=discord.File(path, filename="chart.png"),
        )

    @meta.command(name="weekly", description="Keys completed per week")
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def weekly(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("weekly", "global")
        if path is None:
            rows = await db.run(databaseConnector.fetch_key_throughput, config.SEASON)
            periods = _load_periods()
            path = await charts.render(
                "weekly", "global", functools.partial(charts.build_keys_per_week, rows, periods)
            )
        await embeds.respond(
            interaction,
            _chart_embed("Keys completed per week"),
            file=discord.File(path, filename="chart.png"),
        )


async def setup(bot):
    await bot.add_cog(MetaCog(bot))
