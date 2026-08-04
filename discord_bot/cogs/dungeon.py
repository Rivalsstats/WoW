"""/dungeon overview (preview image) and the top-level /lust command."""

import databaseConnector
import discord
from discord import app_commands
from discord.ext import commands

from .. import cache, db, embeds, lookups


# --- cached DB helpers -----------------------------------------------------
@cache.ttl_cache(3600)
async def _get_lust(did):
    return await db.run(databaseConnector.fetch_dungeon_lust_timeline, did, dictionary=True)


# --- embed builders --------------------------------------------------------
def build_overview_embed(did) -> discord.Embed:
    """Just the dungeon preview image (title/thumbnail via the header)."""
    embed = embeds.dungeon_embed_header(did)
    preview = lookups.dungeon_preview_url(did)
    if preview:
        embed.set_image(url=preview)
    return embed


def build_lust_embed(did, timeline) -> discord.Embed:
    """Which of the dungeon's bosses get Bloodlust, and how often. Non-boss pulls are
    dropped (they're noise); the site has the full pull-by-pull timeline."""
    embed = embeds.dungeon_embed_header(did)
    embed.title = f"{lookups.dungeon_name(did)} — Bosses to Bloodlust"
    boss_ids = {int(b) for b in lookups.DUNGEONS.get(str(did), {}).get("boss_npc_ids", [])}

    # highest lust rate seen for each boss (a boss can appear in several pull sigs)
    by_boss = {}
    for r in timeline or []:
        npc_ids = [int(n) for n in str(r["top_npcs"]).split(",") if n]
        bosses = [n for n in npc_ids if n in boss_ids]
        if not bosses:
            continue
        name = " & ".join(embeds.esc(lookups.npc_name(n)) for n in bosses)
        pct = float(r.get("lust_percentage") or 0)
        if pct > by_boss.get(name, -1):
            by_boss[name] = pct

    if not by_boss:
        embed.description = "No boss bloodlust data for this dungeon yet."
        return embed
    embed.description = (
        "How often groups Bloodlust each boss. "
        f"Full pull-by-pull timeline on the [website]({lookups.dungeon_site_url(did)})."
    )
    fields = [
        (name, f"**{pct:.0f}%** lusted", True)
        for name, pct in sorted(by_boss.items(), key=lambda kv: kv[1], reverse=True)
    ]
    embeds.add_fields_capped(embed, fields)
    return embed


class DungeonCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    dungeon = app_commands.Group(name="dungeon", description="Dungeon preview")

    @dungeon.command(name="overview", description="Dungeon preview image")
    @app_commands.describe(dungeon="Dungeon")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def overview(self, interaction, dungeon: str):
        await interaction.response.defer(thinking=True)
        await interaction.followup.send(embed=build_overview_embed(lookups.resolve_dungeon(dungeon)))

    @app_commands.command(name="lust", description="Which bosses get Bloodlust in a dungeon")
    @app_commands.describe(dungeon="Dungeon")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def lust(self, interaction, dungeon: str):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon)
        timeline = await _get_lust(did)
        await interaction.followup.send(embed=build_lust_embed(did, timeline))


async def setup(bot):
    await bot.add_cog(DungeonCog(bot))
