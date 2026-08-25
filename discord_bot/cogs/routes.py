"""/routes — the single best stored keystone.guru route for a dungeon.

Backed by the published compRoutes.json artifact. Candidate selection mirrors
comp-routes-worker.js: intersect the per-dungeon and per-spec route-key index sets,
then rank by usage. More filtering and every route live on the website.
"""

import commonUtils
import discord
from discord import app_commands
from discord.ext import commands

from .. import config, embeds, lookups

_ZWSP = "​"  # zero-width space: a nameless (label-less) field header


def find_routes(indexes, did=None, spec_ids=None):
    """Return up to 5 route meta dicts matching the filters, best (most-used) first."""
    spec_ids = [int(s) for s in (spec_ids or [])]
    candidate_keys = None
    if did:
        candidate_keys = set(indexes.dungeon_index.get(str(did), set()))
    for spec_id in spec_ids:
        spec_keys = indexes.spec_index.get(spec_id, set())
        candidate_keys = spec_keys if candidate_keys is None else (candidate_keys & spec_keys)
    if candidate_keys is None:
        return []
    metas = [indexes.route_meta[k] for k in candidate_keys if k in indexes.route_meta]
    metas.sort(
        key=lambda m: (m.get("usage_count", 0), m.get("level", 0), -m.get("duration", 0)),
        reverse=True,
    )
    return metas[:5]


def build_routes_embed(did, spec_ids, indexes) -> discord.Embed:
    """The best stored route for the dungeon (optionally filtered to specs in the
    group), as a comps-style card. More filtering lives on the site."""
    embed = embeds.dungeon_embed_header(did)
    embed.title = f"{lookups.dungeon_name(did)} — Best route"
    deep = f"{config.SITE_BASE}/pages/routes?dungeons={did}"
    if spec_ids:
        deep += f"&specs={','.join(str(s) for s in spec_ids)}"

    routes = find_routes(indexes, did, spec_ids)
    if not routes:
        embed.description = (
            "No stored route matches those specs.\n"
            f"Try the [route finder]({deep}) for more options."
        )
        return embed

    meta = routes[0]
    fields = [
        (_ZWSP, embeds.comp_line(meta.get("specs", []), with_names=False), False),
        ("Key", f"+{meta.get('level')}", True),
        ("Time", commonUtils.format_duration(meta.get("duration")), True),
        ("Used", f"{int(meta.get('usage_count', 0)):,}×", True),
        ("Links",
         f"[keystone.guru]({embeds.keystone_guru_route_url(meta.get('route_key'), meta.get('dungeon'))}) · "
         f"[Raider.IO]({embeds.raider_io_run_url(meta.get('run_id'))})",
         False),
    ]
    embed.description = f"More routes & filtering on the [website]({deep})."
    embeds.add_fields_capped(embed, fields)
    return embed


class RoutesCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="routes", description="Best route for a dungeon (optionally filter by group specs)"
    )
    @app_commands.describe(
        dungeon="Dungeon", spec1="Spec 1", spec2="Spec 2", spec3="Spec 3", spec4="Spec 4",
    )
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.autocomplete(
        spec1=lookups.spec_full_autocomplete, spec2=lookups.spec_full_autocomplete,
        spec3=lookups.spec_full_autocomplete, spec4=lookups.spec_full_autocomplete,
    )
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def routes(self, interaction, dungeon: str, spec1: str = None, spec2: str = None,
                     spec3: str = None, spec4: str = None):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon)
        spec_ids = [lookups.resolve_spec_full(s) for s in (spec1, spec2, spec3, spec4) if s]
        indexes = await self.bot.site_data.comp_routes_indexes()
        await embeds.respond(interaction, build_routes_embed(did, spec_ids, indexes))


async def setup(bot):
    await bot.add_cog(RoutesCog(bot))
