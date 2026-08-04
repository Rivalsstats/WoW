"""/item — per-item usage summary, backed by the published items index."""

import commonUtils
import discord
from discord import app_commands
from discord.ext import commands

from .. import config, embeds, lookups
from ..errors import ValidationError

QUALITY_COLOURS = {
    5: discord.Colour(0xFF8000),  # legendary
    4: discord.Colour(0xA335EE),  # epic
    3: discord.Colour(0x0070DD),  # rare
    2: discord.Colour(0x1EFF00),  # uncommon
}
DEFAULT_QUALITY_COLOUR = discord.Colour(0x9D9D9D)


def resolve_item(name, item_by_id, items_index):
    """Accept the autocomplete value (numeric id) or an exact/unique name."""
    key = str(name).strip()
    if key.isdigit() and int(key) in item_by_id:
        return item_by_id[int(key)]
    lowered = key.casefold()
    matches = [it for it in items_index if it["name"].casefold() == lowered]
    if len(matches) == 1:
        return matches[0]
    raise ValidationError("Pick an item from the suggestions.")


def build_item_embed(item) -> discord.Embed:
    colour = QUALITY_COLOURS.get(item.get("quality"), DEFAULT_QUALITY_COLOUR)
    embed = embeds.base_embed(
        embeds.esc(item["name"]),
        url=f"{config.SITE_BASE}/items/{item.get('slug')}",
        colour=colour,
    )
    icon = item.get("icon")
    if icon:
        embed.set_thumbnail(url=lookups.asset_icon_url(icon))

    top_spec = item.get("top_spec")
    fields = [
        ("Slot", item.get("slot", "—"), True),
        ("Seen in", f"{commonUtils.humanize_number(item.get('runs', 0))} runs", True),
    ]
    if top_spec is not None:
        fields.append((
            "Most used by",
            f"[{lookups.spec_full_name(top_spec)}]({lookups.spec_site_url(top_spec)})",
            True,
        ))
    fields.append((
        "Links",
        f"[Mythistone item page]({config.SITE_BASE}/items/{item.get('slug')})",
        False,
    ))
    embeds.add_fields_capped(embed, fields)
    return embed


class ItemsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    item = app_commands.Group(name="item", description="Item usage stats")

    @item.command(name="info", description="Usage stats for an item")
    @app_commands.describe(name="Item name")
    @app_commands.autocomplete(name=lookups.item_autocomplete)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def info(self, interaction, name: str):
        await interaction.response.defer(thinking=True)
        items_index = await self.bot.site_data.items_index()
        item_by_id = await self.bot.site_data.item_by_id()
        item = resolve_item(name, item_by_id, items_index)
        await interaction.followup.send(embed=build_item_embed(item))


async def setup(bot):
    await bot.add_cog(ItemsCog(bot))
