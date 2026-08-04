"""Static entity lookups, whitelisted Choice lists, autocomplete and resolvers.

This is the injection-safety boundary: every entity a user can name (class, spec,
dungeon, item) is resolved here against the baked ``data/static`` lookups. Choices
and autocomplete values are validated on the way back in (they can be forged over
raw HTTP), and nothing free-typed ever reaches SQL — only resolved ids, which the
databaseConnector functions bind as parameters.
"""

import json
import os
import urllib.parse

import commonUtils
from discord import app_commands

from . import config
from .errors import ValidationError

# Role int/str -> the /classes/<folder>/ page bucket (mirrors
# pageGeneration.ROLE_FOLDERS; redefined here so the bot image need not ship that
# generator-only module).
ROLE_FOLDERS = {"0": "Tank", "1": "Healer", "2": "Dps"}
ROLE_NAMES = {"0": "Tank", "1": "Healer", "2": "DPS"}

SPECS = commonUtils.get_spec_lookup()      # {str spec_id: {name, classID, role, SpellIconFileId, ...}}
CLASSES = commonUtils.get_class_lookup()   # {str class_id: {name, icon_id, color{r,g,b}}}
DUNGEONS = commonUtils.get_dungeon_lookup()  # {str challenge_mode_id: {name{en_US}, slug, ...}}


def _load_group_buffs() -> list:
    path = os.path.join(config.STATIC_DIR, "groupbuffs.json")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


GROUP_BUFFS = _load_group_buffs()          # [{id, icon, name, classIDs[], specIDs[]}]
CRITICAL_BUFF_IDS = {2825, 20484}          # Bloodlust, Battle Rez (see templates/comps.html)

# --- derived indexes -------------------------------------------------------
PLAYABLE_CLASS_IDS = sorted(
    {str(meta["classID"]) for meta in SPECS.values()}, key=int
)

# class_id -> [(spec_id, spec_name), ...]
SPECS_BY_CLASS: dict[str, list[tuple[str, str]]] = {}
SPEC_FULL_NAMES: dict[str, str] = {}       # spec_id -> "Arcane Mage"
SPEC_BY_FULL_NAME: dict[str, str] = {}     # casefolded "arcane mage" -> spec_id
_SPEC_BY_NAME_COUNT: dict[str, int] = {}   # casefolded bare spec name -> count (ambiguity check)
_SPEC_BY_NAME: dict[str, str] = {}         # casefolded bare spec name -> spec_id

for _sid, _meta in SPECS.items():
    _cid = str(_meta["classID"])
    _cname = CLASSES.get(_cid, {}).get("name", "")
    _sname = _meta.get("name", "")
    SPECS_BY_CLASS.setdefault(_cid, []).append((_sid, _sname))
    _full = f"{_sname} {_cname}".strip()
    SPEC_FULL_NAMES[_sid] = _full
    SPEC_BY_FULL_NAME[_full.casefold()] = _sid
    _bare = _sname.casefold()
    _SPEC_BY_NAME_COUNT[_bare] = _SPEC_BY_NAME_COUNT.get(_bare, 0) + 1
    _SPEC_BY_NAME[_bare] = _sid

for _cid in SPECS_BY_CLASS:
    SPECS_BY_CLASS[_cid].sort(key=lambda pair: pair[1])


def _class_choices():
    return [
        app_commands.Choice(name=CLASSES[cid]["name"], value=cid)
        for cid in sorted(PLAYABLE_CLASS_IDS, key=lambda c: CLASSES[c]["name"])
    ]


def _dungeon_choices():
    return [
        app_commands.Choice(name=DUNGEONS[did]["name"]["en_US"], value=did)
        for did in sorted(DUNGEONS, key=lambda d: DUNGEONS[d]["name"]["en_US"])
    ]


CLASS_CHOICES = _class_choices()           # 13 <= 25
DUNGEON_CHOICES = _dungeon_choices()       # 8
ROLE_CHOICES = [
    app_commands.Choice(name="Tank", value="0"),
    app_commands.Choice(name="Healer", value="1"),
    app_commands.Choice(name="DPS", value="2"),
]


# --- resolvers (raise ValidationError on bad input) ------------------------
def resolve_class(class_id: str) -> str:
    class_id = str(class_id)
    if class_id not in PLAYABLE_CLASS_IDS:
        raise ValidationError("Pick a class from the list.")
    return class_id


def resolve_spec(class_id: str, spec_name: str) -> str:
    class_id = resolve_class(class_id)
    wanted = str(spec_name).strip().casefold()
    for sid, sname in SPECS_BY_CLASS[class_id]:
        if sname.casefold() == wanted or sid == wanted:
            return sid
    options = ", ".join(sname for _sid, sname in SPECS_BY_CLASS[class_id])
    raise ValidationError(
        f"Unknown spec for {CLASSES[class_id]['name']}. Choose one of: {options}."
    )


def resolve_spec_full(name: str) -> str:
    key = str(name).strip().casefold()
    if key in SPEC_BY_FULL_NAME:
        return SPEC_BY_FULL_NAME[key]
    # numeric spec id passed straight through (autocomplete value)
    if key.isdigit() and key in SPECS:
        return key
    # bare, unambiguous spec name ("Mistweaver")
    if _SPEC_BY_NAME_COUNT.get(key) == 1:
        return _SPEC_BY_NAME[key]
    raise ValidationError("Pick a spec from the suggestions (e.g. 'Frost Mage').")


def resolve_dungeon(dungeon_id: str) -> str:
    dungeon_id = str(dungeon_id)
    if dungeon_id not in DUNGEONS:
        raise ValidationError("Pick a dungeon from the list.")
    return dungeon_id


# --- names / urls ----------------------------------------------------------
_npc_names_cache = {}


def npc_name(npc_id) -> str:
    """en_US NPC name from npcs.json (lazy-loaded, large file)."""
    if not _npc_names_cache:
        path = os.path.join(config.STATIC_DIR, "npcs.json")
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            _npc_names_cache.update(data.get("en_US", {}))
        except FileNotFoundError:
            _npc_names_cache["__loaded__"] = True
    name = _npc_names_cache.get(str(npc_id))
    return name or f"NPC {npc_id}"


def spec_full_name(spec_id) -> str:
    return SPEC_FULL_NAMES.get(str(spec_id), f"Spec {spec_id}")


def spec_site_url(spec_id) -> str:
    sid = str(spec_id)
    meta = SPECS.get(sid, {})
    cid = str(meta.get("classID", ""))
    role = ROLE_FOLDERS.get(str(meta.get("role", "2")), "Dps")
    filename = f"{meta.get('name', '')}_{CLASSES.get(cid, {}).get('name', '')}"
    return f"{config.SITE_BASE}/classes/{role}/{urllib.parse.quote(filename)}"


def spec_preview_url(spec_id) -> str:
    sid = str(spec_id)
    meta = SPECS.get(sid, {})
    cid = str(meta.get("classID", ""))
    filename = f"{meta.get('name', '')}_{CLASSES.get(cid, {}).get('name', '')}"
    return f"{config.SITE_BASE}/assets/img/previews/{urllib.parse.quote(filename)}.png"


def spec_icon_url(spec_id) -> str:
    meta = SPECS.get(str(spec_id), {})
    return f"{config.SITE_BASE}/data/icons/{meta.get('SpellIconFileId')}.jpg"


def dungeon_site_url(dungeon_id) -> str:
    meta = DUNGEONS.get(str(dungeon_id), {})
    return f"{config.SITE_BASE}/dungeons/{meta.get('slug', '')}"


def dungeon_name(dungeon_id) -> str:
    meta = DUNGEONS.get(str(dungeon_id), {})
    return meta.get("name", {}).get("en_US", f"Dungeon {dungeon_id}")


def dungeon_icon_url(dungeon_id):
    """Dungeon icon token already carries its extension (e.g. '...-small.jpg')."""
    meta = DUNGEONS.get(str(dungeon_id), {})
    icon = meta.get("icon")
    if not icon:
        return None
    return f"{config.SITE_BASE}/data/icons/{icon}"


def dungeon_preview_url(dungeon_id):
    meta = DUNGEONS.get(str(dungeon_id), {})
    slug = meta.get("slug")
    if not slug:
        return None
    return f"{config.SITE_BASE}/assets/img/previews/{slug}.png"


def asset_icon_url(icon_token) -> str:
    """Item/gem/enchant icons are wowhead-style slugs served as PNG."""
    return f"{config.SITE_BASE}/data/icons/{icon_token}.png"


def spec_role(spec_id) -> str:
    return str(SPECS.get(str(spec_id), {}).get("role", "2"))


# --- autocomplete providers (never raise; cap 25) --------------------------
def _limit(choices, n=25):
    return choices[:n]


async def spec_autocomplete(interaction, current: str):
    """Suggest specs scoped to the class already chosen in the same command."""
    try:
        class_id = getattr(interaction.namespace, "class_name", None) or getattr(
            interaction.namespace, "class", None
        )
        current = (current or "").casefold()
        if class_id and str(class_id) in SPECS_BY_CLASS:
            pool = SPECS_BY_CLASS[str(class_id)]
        else:
            pool = [(sid, SPECS[sid]["name"]) for sid in SPECS]
        out = [
            app_commands.Choice(name=sname, value=sid)
            for sid, sname in pool
            if current in sname.casefold()
        ]
        return _limit(out)
    except Exception:
        return []


async def spec_full_autocomplete(interaction, current: str):
    """Suggest any of the 40 specs by full 'Spec Class' name."""
    try:
        current = (current or "").casefold()
        out = [
            app_commands.Choice(name=SPEC_FULL_NAMES[sid], value=sid)
            for sid in SPECS
            if current in SPEC_FULL_NAMES[sid].casefold()
        ]
        out.sort(key=lambda c: c.name)
        return _limit(out)
    except Exception:
        return []


async def item_autocomplete(interaction, current: str):
    """Suggest items from the published items index, most-used first."""
    try:
        items = await interaction.client.site_data.items_index()
        current = (current or "").casefold()
        matches = [it for it in items if current in it["name"].casefold()]
        matches.sort(key=lambda it: it.get("runs", 0), reverse=True)
        return _limit(
            [
                app_commands.Choice(name=it["name"][:100], value=str(it["id"]))
                for it in matches
            ]
        )
    except Exception:
        return []
