"""/analyze — the "Am I meta?" gear check.

Paste a SimulationCraft addon export; the bot parses the equipped gear/enchants/
gems, resolves the spec, and scores each against that spec's published ``spec_meta``
targets — the same logic as the website's analyzer.js (a slot passes if its item is
the SIM/TOP pick, or the popular ``common`` fallback where there's no SIM/TOP).
"""

import re

import discord
from discord import app_commands
from discord.ext import commands

from .. import config, embeds, emojis, lookups

_ZWSP = "​"  # zero-width space: a nameless (label-less) field header

# SimC class token -> WoW class id (generateAnalyzerPage.SIMC_CLASS_TOKENS).
_CLASS_TOKENS = {
    "deathknight": 6, "demonhunter": 12, "druid": 11, "evoker": 13, "hunter": 3,
    "mage": 8, "monk": 10, "paladin": 2, "priest": 5, "rogue": 4, "shaman": 7,
    "warlock": 9, "warrior": 1,
}
# SimC gear token -> spec_meta slot name (analyzer.js SLOT_MAP).
_SLOT_MAP = {
    "head": "HEAD", "neck": "NECK", "shoulder": "SHOULDER", "shoulders": "SHOULDER",
    "back": "BACK", "chest": "CHEST", "wrist": "WRIST", "wrists": "WRIST",
    "hands": "HANDS", "waist": "WAIST", "legs": "LEGS", "feet": "FEET",
    "finger1": "FINGER_1", "finger2": "FINGER_2",
    "trinket1": "TRINKET_1", "trinket2": "TRINKET_2",
    "main_hand": "MAIN_HAND", "off_hand": "OFF_HAND",
}
# Interchangeable pairs — a ring/trinket matches if it fits either slot's target.
_GROUPS = {
    "FINGER_1": ("FINGER_1", "FINGER_2"), "FINGER_2": ("FINGER_1", "FINGER_2"),
    "TRINKET_1": ("TRINKET_1", "TRINKET_2"), "TRINKET_2": ("TRINKET_1", "TRINKET_2"),
}
_SLOT_ORDER = [
    "HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST", "HANDS", "WAIST", "LEGS",
    "FEET", "FINGER_1", "FINGER_2", "TRINKET_1", "TRINKET_2", "MAIN_HAND", "OFF_HAND",
]
_SLOT_LABEL = {
    "HEAD": "Head", "NECK": "Neck", "SHOULDER": "Shoulder", "BACK": "Back",
    "CHEST": "Chest", "WRIST": "Wrist", "HANDS": "Hands", "WAIST": "Waist",
    "LEGS": "Legs", "FEET": "Feet", "FINGER_1": "Ring 1", "FINGER_2": "Ring 2",
    "TRINKET_1": "Trinket 1", "TRINKET_2": "Trinket 2",
    "MAIN_HAND": "Main Hand", "OFF_HAND": "Off Hand",
}
# Enchant group -> the gear slots it covers (analyzer.js ENCHANT_GROUP, reversed).
_ENCHANT_GROUP_SLOTS = {
    "HEAD": ["HEAD"], "SHOULDER": ["SHOULDER"], "BACK": ["BACK"], "CHEST": ["CHEST"],
    "WRIST": ["WRIST"], "LEGS": ["LEGS"], "FEET": ["FEET"],
    "FINGER": ["FINGER_1", "FINGER_2"], "WEAPON": ["MAIN_HAND", "OFF_HAND"],
}
# (class_id, spec-name-lower) -> spec_id, for SimC (class, spec) token resolution.
_SPEC_INDEX = {
    f"{m['classID']}|{(m.get('name') or '').lower()}": int(sid)
    for sid, m in lookups.SPECS.items()
}


# --- SimC parsing ----------------------------------------------------------
def _parse_gear(rest):
    m = re.search(r"(?:^|,)id=(\d+)", rest)
    if not m:
        return None
    ench = re.search(r"(?:^|,)enchant_id=(\d+)", rest)
    gem = re.search(r"(?:^|,)gem_id=([\d/]+)", rest)
    return {
        "id": int(m.group(1)),
        "enchant": int(ench.group(1)) if ench else None,
        "gems": [int(g) for g in gem.group(1).split("/") if g] if gem else [],
    }


def parse_simc(text):
    """Return {class_id, spec_token, slots:{SLOT:{id,enchant,gems}}} from an export.
    Only equipped gear is read; commented '# gear from bags' lines are skipped."""
    out = {"class_id": None, "spec_token": None, "slots": {}}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line[0] == "#":
            continue
        if "=" not in line:
            continue
        key, rest = line.split("=", 1)
        key = key.strip().lower()
        if out["class_id"] is None and key in _CLASS_TOKENS:
            out["class_id"] = str(_CLASS_TOKENS[key])
        elif key == "spec":
            out["spec_token"] = rest.replace('"', "").strip().lower()
        elif key in _SLOT_MAP:
            gear = _parse_gear(rest)
            if gear:
                out["slots"][_SLOT_MAP[key]] = gear
    return out


def resolve_spec(parsed):
    if not parsed.get("class_id") or not parsed.get("spec_token"):
        return None
    return _SPEC_INDEX.get(f"{parsed['class_id']}|{parsed['spec_token']}")


# --- scoring (mirrors analyzer.js) -----------------------------------------
def _acceptable(meta_slots, slot):
    sim, top, common = set(), set(), set()
    for n in _GROUPS.get(slot, (slot,)):
        m = meta_slots.get(n) or {}
        s = m.get("sim")
        if isinstance(s, dict) and s.get("id") is not None:
            sim.add(s["id"])
        for t in m.get("top") or []:
            if t.get("id") is not None:
                top.add(t["id"])
        c = m.get("common")
        if isinstance(c, dict) and c.get("id") is not None:
            common.add(c["id"])
    return sim, top, common


def _score_slot(meta_slots, slot, user_id):
    sim, top, common = _acceptable(meta_slots, slot)
    has_ideal = bool(sim or top)
    if not has_ideal and not common:
        return None  # no meta data for this slot
    if user_id in sim or user_id in top:
        return "match"
    if has_ideal:
        return "off"  # has a SIM/TOP target the user misses
    return "match" if user_id in common else "off"


def _suggestion(meta_slots, slot, spec_id):
    for n in _GROUPS.get(slot, (slot,)):
        s = (meta_slots.get(n) or {}).get("sim")
        if isinstance(s, dict):
            return _item_link(s, spec_id)
    for n in _GROUPS.get(slot, (slot,)):
        top = (meta_slots.get(n) or {}).get("top") or []
        if top:
            return _item_link(top[0], spec_id)
    for n in _GROUPS.get(slot, (slot,)):
        c = (meta_slots.get(n) or {}).get("common")
        if isinstance(c, dict):
            return _item_link(c, spec_id)
    return None


def _item_link(pick, spec_id):
    name = embeds.esc(pick.get("name", "?"))
    slug = pick.get("slug")
    link = f"[{name}]({config.SITE_BASE}/items/{slug}?spec={spec_id})" if slug else name
    icon = emojis.item(pick.get("id")) if pick.get("id") is not None else ""
    return f"{icon} {link}".strip()


def _enchant_issues(spec_meta, parsed):
    expected = spec_meta.get("enchant_group_expected") or {}
    budget = {e["id"]: e.get("qty", 1) for e in (spec_meta.get("enchant_combo") or {}).get("entries", [])}
    missing = []
    for group, exp in expected.items():
        gslots = [s for s in _ENCHANT_GROUP_SLOTS.get(group, []) if s in parsed["slots"]]
        enchanted = [s for s in gslots if parsed["slots"][s].get("enchant")]
        bare = [s for s in gslots if not parsed["slots"][s].get("enchant")]
        for s in bare[: max(0, exp - len(enchanted))]:
            missing.append(_SLOT_LABEL[s])
    off = {s["enchant"] for s in parsed["slots"].values()
           if s.get("enchant") and budget.get(s["enchant"], 0) == 0}
    return missing, len(off)


def _gem_issues(spec_meta, parsed):
    budget = {e["id"]: e.get("qty", 1) for e in (spec_meta.get("gem_combo") or {}).get("entries", [])}
    gems = [g for s in parsed["slots"].values() for g in (s.get("gems") or [])]
    return sum(1 for g in gems if budget.get(g, 0) == 0)


def build_analyze_embed(spec_id, spec_meta, parsed) -> discord.Embed:
    embed = embeds.spec_embed_header(spec_id)
    embed.title = f"{lookups.spec_full_name(spec_id)} — Meta Check"
    meta_slots = (spec_meta or {}).get("slots", {})

    off, scored = [], 0
    for slot in _SLOT_ORDER:
        user = parsed["slots"].get(slot)
        if not user:
            continue
        status = _score_slot(meta_slots, slot, user["id"])
        if status is None:
            continue
        scored += 1
        if status == "off":
            off.append(slot)
    if scored == 0:
        embed.description = "No meta data available for this spec yet (or no gear read from the export)."
        return embed

    missing_ench, off_ench = _enchant_issues(spec_meta, parsed)
    bad_gems = _gem_issues(spec_meta, parsed)
    matched = scored - len(off)

    if not off and not missing_ench and not off_ench and not bad_gems:
        embed.description = "✅ **Fully meta** — every slot, enchant and gem matches the top build."
        return embed

    embed.description = f"**{matched}/{scored}** gear slots match the meta. Details below."
    fields = []
    if off:
        fields.append((
            "Off-meta gear",
            "\n".join(
                f"**{_SLOT_LABEL[s]}** → {_suggestion(meta_slots, s, spec_id) or 'off-meta'}"
                for s in off
            ),
            False,
        ))
    ench_notes = []
    if missing_ench:
        ench_notes.append("Missing enchant: " + ", ".join(missing_ench))
    if off_ench:
        ench_notes.append(f"{off_ench} off-meta enchant(s)")
    if ench_notes:
        fields.append(("Enchants", "\n".join(ench_notes), False))
    if bad_gems:
        fields.append(("Gems", f"{bad_gems} gem(s) aren't in the popular combo", True))
    # Rendered last so the site link is the final line before the footer.
    fields.append((_ZWSP, f"Full breakdown on the [website]({config.SITE_BASE}/pages/analyzer).", False))
    embeds.add_fields_capped(embed, fields)
    return embed


# --- cog -------------------------------------------------------------------
class AnalyzeModal(discord.ui.Modal, title="Am I meta? — paste your /simc string"):
    simc = discord.ui.TextInput(
        label="SimulationCraft addon export",
        style=discord.TextStyle.paragraph,
        placeholder="In-game: /simc → copy the whole thing → paste here",
        max_length=4000,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        parsed = parse_simc(str(self.simc))
        spec_id = resolve_spec(parsed)
        if spec_id is None:
            embed = embeds.base_embed("Meta Check", url=f"{config.SITE_BASE}/pages/analyzer")
            embed.description = (
                "Couldn't read a class/spec from that export. Paste the full output of "
                "the in-game `/simc` command (it starts with your class and `spec=`)."
            )
            await interaction.followup.send(embed=embed)
            return
        spec_meta = await interaction.client.site_data.spec_meta(spec_id)
        await interaction.followup.send(embed=build_analyze_embed(spec_id, spec_meta, parsed))


class AnalyzeCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="analyze",
        description="Am I meta? Check your gear/enchants/gems from a /simc export",
    )
    @app_commands.checks.cooldown(2, 30.0, key=lambda i: i.user.id)
    async def analyze(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AnalyzeModal())


async def setup(bot):
    await bot.add_cog(AnalyzeCog(bot))
