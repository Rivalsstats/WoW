"""/comps — top comps, group filling ("The Perfect Fit") and buff coverage.

Powered entirely by the published comps_index.json artifact plus the static
groupbuffs.json; no SQL. The ranking, suggestion and buff-coverage logic mirror
templates/comps.html so the bot and site agree.
"""

import discord
from discord import app_commands
from discord.ext import commands

from .. import config, embeds, emojis, lookups
from ..errors import ValidationError

_BUFF_PROVIDERS = {b["id"]: set(b.get("specIDs", [])) for b in lookups.GROUP_BUFFS}


# --- shared logic ----------------------------------------------------------
def buff_coverage(spec_ids):
    """Return (covered, missing) lists of buff dicts for a set of spec ids."""
    ids = {int(s) for s in spec_ids}
    covered, missing = [], []
    for buff in lookups.GROUP_BUFFS:
        providers = _BUFF_PROVIDERS.get(buff["id"], set())
        (covered if ids & providers else missing).append(buff)
    return covered, missing


def buff_gain(spec_id, spec_ids):
    """Buffs a candidate spec newly adds on top of the current selection."""
    sid = int(spec_id)
    existing = {int(s) for s in spec_ids}
    gains = []
    for buff in lookups.GROUP_BUFFS:
        providers = _BUFF_PROVIDERS.get(buff["id"], set())
        if sid in providers and not (existing & providers):
            gains.append(buff)
    return gains


def buff_summary(missing):
    if not missing:
        return "Full buff coverage ✅"
    critical = [b for b in missing if b["id"] in lookups.CRITICAL_BUFF_IDS]
    parts = []
    if critical:
        parts.append("⚠️ **Missing " + " & ".join(b["name"] for b in critical) + "**")
    others = [b for b in missing if b["id"] not in lookups.CRITICAL_BUFF_IDS]
    if others:
        parts.append(f"{len(others)} raid buff{'s' if len(others) > 1 else ''} missing")
    return " · ".join(parts)


def _comp_stats(comp, did=None):
    """(runs, timed, depleted, best_key, avg_key) — per-dungeon when ``did`` is set."""
    if did and comp.get("dungeons", {}).get(did):
        d = comp["dungeons"][did]
        runs = d.get("runs", d.get("t", 0) + d.get("d", 0))
        return runs, d.get("t", 0), d.get("d", 0), d.get("mk", 0), d.get("avg_key", 0)
    return (
        comp.get("runs", 0), comp.get("t", 0), comp.get("d", 0),
        comp.get("mk", 0), comp.get("avg_key", 0),
    )


_ZWSP = "​"  # zero-width space: a nameless (label-less) field header


def _comp_fields(comp, did=None):
    """One comp's fields: an emoji-team header (no label) + Runs/Best/Timed inline."""
    runs, timed, depleted, mk, _ = _comp_stats(comp, did)
    win = (timed / (timed + depleted) * 100) if (timed + depleted) else 0
    row = embeds.comp_line(comp["c"], with_names=False)  # field values render emoji
    return [
        (_ZWSP, row, False),
        ("Runs", f"{int(runs):,}", True),
        ("Best Key", f"+{mk}", True),
        ("Timed", f"{win:.0f}%", True),
    ]


# --- embed builders --------------------------------------------------------
def build_top_embed(comps_index, did=None) -> discord.Embed:
    """A single embed; each top comp is a header field + its inline stat fields."""
    if did:
        pool = [
            c for c in comps_index
            if c.get("dungeons", {}).get(did)
            and (c["dungeons"][did].get("t", 0) + c["dungeons"][did].get("d", 0)) > 0
        ]
        pool.sort(key=lambda c: c["dungeons"][did].get("w", 0), reverse=True)
        title = f"Current top comps — {lookups.dungeon_name(did)}"
    else:
        pool = sorted(comps_index, key=lambda c: c.get("runs", 0), reverse=True)
        title = "Current top comps"
    embed = embeds.base_embed(title, url=f"{config.SITE_BASE}/pages/comps")
    embeds.set_dungeon_thumbnail(embed, did)
    if not pool:
        embed.description = "No comp data available yet."
        return embed
    fields = []
    for comp in pool[:5]:
        fields.extend(_comp_fields(comp, did))
    embeds.add_fields_capped(embed, fields)
    return embed


def build_fill_embed(comps_index, selected_ids, did=None) -> discord.Embed:
    selected = [int(s) for s in selected_ids]
    selected_set = set(selected)

    def _dungeon(c):
        return c.get("dungeons", {}).get(did) if did else None

    def _weight(c):
        d = _dungeon(c)
        return d.get("w", 0) if did else c.get("w", 0)

    possible = [
        c for c in comps_index
        if selected_set.issubset(set(c["c"]))
        and (not did or (_dungeon(c) and (_dungeon(c).get("t", 0) + _dungeon(c).get("d", 0)) > 0))
    ]
    possible.sort(key=_weight, reverse=True)

    # Accumulate weighted suggestions over completions (mirrors updateSuggestions),
    # using per-dungeon run/weight stats when a dungeon is selected.
    stats = {}
    for comp in possible:
        weight = _weight(comp)
        if weight <= 0:
            continue
        src = _dungeon(comp) if did else comp
        for spec in comp["c"]:
            if spec in selected_set:
                continue
            entry = stats.setdefault(spec, {"t": 0, "d": 0, "w": 0.0, "mk": 0})
            entry["t"] += src.get("t", 0)
            entry["d"] += src.get("d", 0)
            entry["w"] += weight
            entry["mk"] = max(entry["mk"], src.get("mk", 0))
    suggestions = sorted(stats.items(), key=lambda kv: kv[1]["w"], reverse=True)[:8]

    covered, missing = buff_coverage(selected)
    chosen = embeds.comp_line(selected)

    title = "The Perfect Fit" + (f" — {lookups.dungeon_name(did)}" if did else "")
    embed = embeds.base_embed(title, url=f"{config.SITE_BASE}/pages/comps")
    embeds.set_dungeon_thumbnail(embed, did)
    embed.description = f"**Group:** {chosen}\n{buff_summary(missing)}"

    fields = []
    if possible:
        lines = []
        for c in possible[:5]:
            runs, _, _, mk, _ = _comp_stats(c, did)
            lines.append(f"{embeds.comp_line(c['c'], with_names=False)} — {int(runs):,} runs · +{mk}")
        fields.append(("Best comps", "\n".join(lines), False))
    if suggestions:
        max_w = suggestions[0][1]["w"] or 1
        lines = []
        for spec, st in suggestions:
            gains = buff_gain(spec, selected)
            gain_icons = "".join(_buff_icon(b) for b in gains)
            gain_txt = f" (+{gain_icons})" if gains else ""
            icon = emojis.spec(spec)
            name = lookups.spec_full_name(spec)
            label = f"{icon} {name}" if icon else name
            bar = embeds.make_bar(st["w"] / max_w * 100)
            lines.append(f"`{bar}` {label}{gain_txt}")
        fields.append(("Strongest additions", "\n".join(lines), False))
    if not fields:
        fields.append((
            "No exact match",
            f"No aggregate data matches that group. "
            f"[Search routes]({config.SITE_BASE}/pages/routes?specs={','.join(str(s) for s in selected)})",
            False,
        ))
    embeds.add_fields_capped(embed, fields)
    return embed


def _buff_icon(buff) -> str:
    """Buff as its emoji; falls back to the name only when no emoji is provisioned."""
    return emojis.buff(buff["id"]) or buff["name"]


def build_buffs_embed(selected_ids) -> discord.Embed:
    selected = [int(s) for s in selected_ids]
    covered, missing = buff_coverage(selected)
    chosen = embeds.comp_line(selected)
    embed = embeds.base_embed("Group buff coverage", url=f"{config.SITE_BASE}/pages/comps")
    embed.description = f"**Group:** {chosen}\n{buff_summary(missing)}"
    fields = []
    if covered:
        fields.append(("Covered", " ".join(_buff_icon(b) for b in covered), False))
    if missing:
        fields.append(("Missing", " ".join(_buff_icon(b) for b in missing), False))
    embeds.add_fields_capped(embed, fields)
    return embed


# --- validation ------------------------------------------------------------
def _resolve_specs(*names):
    ids = []
    for name in names:
        if name:
            ids.append(lookups.resolve_spec_full(name))
    if len(ids) != len(set(ids)):
        raise ValidationError("Each spec can only be picked once.")
    tanks = sum(1 for s in ids if lookups.spec_role(s) == "0")
    healers = sum(1 for s in ids if lookups.spec_role(s) == "1")
    if tanks > 1 or healers > 1:
        raise ValidationError("A group can have at most one tank and one healer.")
    return ids


class CompsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    comps = app_commands.Group(name="comps", description="Team comps & group filling")

    @comps.command(name="top", description="Most popular comps (optionally per dungeon)")
    @app_commands.describe(dungeon="Optional dungeon filter")
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def top(self, interaction, dungeon: str = None):
        await interaction.response.defer(thinking=True)
        did = lookups.resolve_dungeon(dungeon) if dungeon else None
        comps_index = await self.bot.site_data.comps_index()
        await interaction.followup.send(embed=build_top_embed(comps_index, did))

    @comps.command(name="fill", description="Suggest specs to complete your group")
    @app_commands.describe(
        spec1="Spec 1", spec2="Spec 2", spec3="Spec 3", spec4="Spec 4",
        dungeon="Optional dungeon filter",
    )
    @app_commands.choices(dungeon=lookups.DUNGEON_CHOICES)
    @app_commands.autocomplete(
        spec1=lookups.spec_full_autocomplete, spec2=lookups.spec_full_autocomplete,
        spec3=lookups.spec_full_autocomplete, spec4=lookups.spec_full_autocomplete,
    )
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def fill(self, interaction, spec1: str, spec2: str = None, spec3: str = None,
                   spec4: str = None, dungeon: str = None):
        await interaction.response.defer(thinking=True)
        ids = _resolve_specs(spec1, spec2, spec3, spec4)
        did = lookups.resolve_dungeon(dungeon) if dungeon else None
        comps_index = await self.bot.site_data.comps_index()
        await interaction.followup.send(embed=build_fill_embed(comps_index, ids, did))

    @comps.command(name="buffs", description="Group buff & utility coverage")
    @app_commands.describe(
        spec1="Spec 1", spec2="Spec 2", spec3="Spec 3", spec4="Spec 4", spec5="Spec 5"
    )
    @app_commands.autocomplete(
        spec1=lookups.spec_full_autocomplete, spec2=lookups.spec_full_autocomplete,
        spec3=lookups.spec_full_autocomplete, spec4=lookups.spec_full_autocomplete,
        spec5=lookups.spec_full_autocomplete,
    )
    @app_commands.checks.cooldown(2, 10.0, key=lambda i: i.user.id)
    async def buffs(self, interaction, spec1: str, spec2: str = None, spec3: str = None,
                    spec4: str = None, spec5: str = None):
        await interaction.response.defer(thinking=True)
        ids = _resolve_specs(spec1, spec2, spec3, spec4, spec5)
        await interaction.followup.send(embed=build_buffs_embed(ids))


async def setup(bot):
    await bot.add_cog(CompsCog(bot))
