"""/stats — notable season records (optionally per dungeon) + key throughput."""

import functools
import json
import os

import commonUtils
import databaseConnector
import discord
from discord import app_commands
from discord.ext import commands

from .. import charts, config, db, embeds, lookups

_ZWSP = "​"  # zero-width space: a nameless (label-less) field header


def _load_periods():
    with open(os.path.join(config.STATIC_DIR, "periods.json"), "r", encoding="utf-8") as fh:
        return json.load(fh)


def _run_dict(run):
    """Normalise a fetched run into a run_lines-shaped dict.

    Season fetchers (fetch_max_key_run/longest/shortest) already return a single run
    dict with ``members``; the per-dungeon fetchers return member-joined rows that
    ``_rows_to_run`` collapses. Either shape (or empty) is handled."""
    if not run:
        return {}
    if isinstance(run, dict) and "members" in run:
        return run
    return embeds._rows_to_run(run)


def build_run_card(title, run, extra_fields=None) -> discord.Embed:
    """A single-run record card: dungeon thumbnail, icon-only comp row and inline
    stat fields — the same visual language as the comps command."""
    embed = embeds.base_embed(title, url=f"{config.SITE_BASE}/pages/dashboard")
    run = _run_dict(run)
    if not run:
        embed.description = "No runs recorded yet."
        return embed

    did = str(run.get("dungeon_id"))
    embeds.set_dungeon_thumbnail(embed, did)
    spec_ids = [m.get("spec_id") for m in run.get("members", []) if m.get("spec_id")]
    level = run.get("keystone_level")
    duration = run.get("duration")
    region = (run.get("region") or "").upper()
    run_id = run.get("run_id")

    fields = []
    if spec_ids:  # icon-only comp row as a nameless header field (renders emoji)
        fields.append((_ZWSP, embeds.comp_line(spec_ids, with_names=False), False))
    fields.append(("Dungeon", lookups.dungeon_name(did), True))
    if level is not None:
        fields.append(("Key", f"+{level}", True))
    if duration:
        fields.append(("Time", commonUtils.format_duration(duration), True))
    if region:
        fields.append(("Region", region, True))
    for extra in extra_fields or []:
        fields.append(extra)
    if run_id is not None:
        fields.append(("Link", f"[View on Raider.IO]({embeds.raider_io_run_url(run_id)})", True))
    embeds.add_fields_capped(embed, fields)
    return embed


def build_highest_embed(run) -> discord.Embed:
    return build_run_card("Highest key this season", run)


def _margin_field(did, run):
    """Closest-call margin (qualifying time − clear time), matching /dungeon records."""
    run = _run_dict(run)
    upgrades = lookups.DUNGEONS.get(str(did), {}).get("keystone_upgrades", {})
    qualifying = upgrades.get("1", {}).get("qualifying_duration")
    duration = run.get("duration")
    if qualifying and duration is not None:
        return [("Margin", commonUtils.format_duration(max(int(qualifying) - int(duration), 0)), True)]
    return None


async def _record(did, dungeon_fn, season_fn):
    """Fetch a record run: per-dungeon (member-joined rows) when ``did`` is set, else
    the season-wide run dict."""
    if did:
        return await db.run(dungeon_fn, did, config.SEASON, dictionary=True)
    return await db.run(season_fn, config.SEASON)


class StatsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    stats = app_commands.Group(name="stats", description="Notable season records")

    @stats.command(name="highest", description="Highest key completed (optionally per dungeon)")
    @app_commands.describe(dungeon="Optional dungeon filter")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def highest(self, interaction: discord.Interaction, dungeon: str = None):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon) if dungeon else None
        run = await _record(did, databaseConnector.fetch_dungeon_max_key_run,
                            databaseConnector.fetch_max_key_run)
        await embeds.respond(interaction, build_run_card("Highest key this season", run))

    @stats.command(name="longest", description="Longest run (optionally per dungeon)")
    @app_commands.describe(dungeon="Optional dungeon filter")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def longest(self, interaction: discord.Interaction, dungeon: str = None):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon) if dungeon else None
        run = await _record(did, databaseConnector.fetch_dungeon_longest_run,
                            databaseConnector.fetch_longest_run)
        await embeds.respond(interaction, build_run_card("Longest run", run))

    @stats.command(name="shortest", description="Fastest clear by time (optionally per dungeon)")
    @app_commands.describe(dungeon="Optional dungeon filter")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def shortest(self, interaction: discord.Interaction, dungeon: str = None):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon) if dungeon else None
        run = await _record(did, databaseConnector.fetch_dungeon_shortest_run,
                            databaseConnector.fetch_shortest_run)
        await embeds.respond(interaction, build_run_card("Fastest clear", run))

    @stats.command(name="closest", description="Closest call at a dungeon's top keys")
    @app_commands.describe(dungeon="Dungeon (required)")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def closest(self, interaction: discord.Interaction, dungeon: str):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon)
        run = await db.run(databaseConnector.fetch_dungeon_closest_call_run, did,
                           config.SEASON, dictionary=True)
        await embeds.respond(interaction, build_run_card("Closest call", run, _margin_field(did, run)))

    @stats.command(name="fastest", description="Fastest run at a dungeon's top key levels")
    @app_commands.describe(dungeon="Dungeon (required)")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def fastest(self, interaction: discord.Interaction, dungeon: str):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon)
        run = await db.run(databaseConnector.fetch_dungeon_fastest_top_levels_run, did,
                           config.SEASON, dictionary=True)
        await embeds.respond(interaction, build_run_card("Fastest at top keys", run))

    @stats.command(name="keys", description="Key throughput per region (keys / minute)")
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def keys(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        path = charts.cached_path("keys", "global")
        if path is None:
            rows = await db.run(databaseConnector.fetch_key_throughput, config.SEASON)
            periods = _load_periods()
            path = await charts.render(
                "keys", "global", functools.partial(charts.build_key_throughput, rows, periods)
            )
        embed = embeds.base_embed(
            "Key throughput", url=f"{config.SITE_BASE}/pages/dashboard"
        )
        embed.set_image(url="attachment://chart.png")
        await embeds.respond(interaction, embed, file=discord.File(path, filename="chart.png"))


async def setup(bot):
    await bot.add_cog(StatsCog(bot))
