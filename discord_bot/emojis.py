"""Application-owned custom emoji layer.

The bot owns a full icon set as *application emojis* (spec, class, buff, role and
a ``meta`` badge). ``populate`` is called on every ``on_ready`` and rebuilds an
in-memory ``name -> "<:name:id>"`` registry from ``bot.fetch_application_emojis()``.

Every render helper returns the emoji markdown or an empty string when the emoji is
absent, so the pure ``build_*_embed`` functions stay renderable offline (the smoke
test runs with an empty registry and simply falls back to text). Provisioning the
emojis is a separate, one-time ops task — see ``discord_bot/emoji_sync.py``.

Only embed *descriptions* and *field values* render custom emoji on Discord — titles
and field names do not — so callers must keep emoji out of titles/field names.
"""

import logging
import os

import aiohttp
import discord

from . import config, lookups

log = logging.getLogger("mythistone.bot")

# emoji name -> rendered "<:name:id>" markdown; empty until populate() runs.
_REGISTRY: dict[str, str] = {}

# Directory holding the locally-vendored role/meta source images (user-supplied).
EMOJI_ASSET_DIR = os.path.join(os.path.dirname(__file__), "emoji_assets")

ROLE_EMOJI_NAMES = {"0": "role_tank", "1": "role_healer", "2": "role_dps"}
META_EMOJI_NAME = "meta"


# --- name derivation -------------------------------------------------------
def _spec_name(spec_id) -> str | None:
    meta = lookups.SPECS.get(str(spec_id))
    if not meta or not meta.get("SpellIconFileId"):
        return None
    return f"spec_{meta['SpellIconFileId']}"


def _class_name(class_id) -> str | None:
    meta = lookups.CLASSES.get(str(class_id))
    if not meta or not meta.get("icon_id"):
        return None
    return f"class_{meta['icon_id']}"


def _buff_name(buff_id) -> str:
    return f"buff_{buff_id}"


def _item_name(item_id) -> str:
    return f"item_{item_id}"


# --- render helpers (return "" when the emoji is missing) ------------------
def _get(name) -> str:
    return _REGISTRY.get(name, "") if name else ""


def spec(spec_id) -> str:
    return _get(_spec_name(spec_id))


def class_(class_id) -> str:
    return _get(_class_name(class_id))


def buff(buff_id) -> str:
    return _get(_buff_name(buff_id))


def item(item_id) -> str:
    return _get(_item_name(item_id))


def role(role_id) -> str:
    return _get(ROLE_EMOJI_NAMES.get(str(role_id)))


def meta() -> str:
    return _get(META_EMOJI_NAME)


# --- provisioning catalogue ------------------------------------------------
def expected() -> list[dict]:
    """Every emoji the bot wants, as ``{name, url|path}`` provisioning entries.

    Spec/class/buff images are served by the site under ``/data/icons``; role/meta
    images are vendored locally in ``emoji_assets/``. Shared by ``populate`` (to warn
    about what is missing) and ``emoji_sync`` (to create what is missing).
    """
    entries: list[dict] = []
    for sid, m in lookups.SPECS.items():
        icon = m.get("SpellIconFileId")
        if icon:
            entries.append({"name": f"spec_{icon}", "url": f"{config.SITE_BASE}/data/icons/{icon}.jpg"})
    # NB: class icons are intentionally *not* provisioned — the site's icon pipeline
    # only downloads spec (SpellIconFileId) icons, so class `icon_id`s 404 at
    # /data/icons. `class_()` stays in the API for when a class-icon source exists.
    for b in lookups.GROUP_BUFFS:
        icon = b.get("icon")
        if icon:
            entries.append({"name": _buff_name(b["id"]), "url": f"{config.SITE_BASE}/data/icons/{icon}"})
    for role_name in ROLE_EMOJI_NAMES.values():
        entries.append({"name": role_name, "path": os.path.join(EMOJI_ASSET_DIR, f"{role_name}.png")})
    entries.append({"name": META_EMOJI_NAME, "path": os.path.join(EMOJI_ASSET_DIR, f"{META_EMOJI_NAME}.png")})

    # De-duplicate by name (icon ids can repeat across specs/classes/buffs).
    seen, unique = set(), []
    for e in entries:
        if e["name"] not in seen:
            seen.add(e["name"])
            unique.append(e)
    return unique


async def spec_item_emojis(site_data) -> list[dict]:
    """``{name, url}`` for the item icons the bot renders — the most-popular
    (``common``) pick ``/spec gear`` shows plus the SIM/TOP picks the ``/analyze``
    meta-check suggests (NOT every item). Fetched from the published per-spec
    ``spec_meta`` artifacts; failures for one spec are skipped."""
    entries, seen = [], set()

    def add(pick):
        if not isinstance(pick, dict):
            return
        iid, icon = pick.get("id"), pick.get("icon")
        if iid is None or not icon or iid in seen:
            return
        seen.add(iid)
        entries.append({"name": _item_name(iid), "url": lookups.asset_icon_url(icon)})

    for sid in lookups.SPECS:
        try:
            meta = await site_data.spec_meta(sid)
        except Exception:  # noqa: BLE001 - one missing spec_meta shouldn't block provisioning
            continue
        for slot in (meta or {}).get("slots", {}).values():
            add(slot.get("common"))  # /spec gear
            add(slot.get("sim"))     # /analyze suggestion (preferred)
            top = slot.get("top") or []
            if top:
                add(top[0])          # /analyze suggestion (TOP fallback)
    return entries


# --- image sourcing ---------------------------------------------------------
async def _image_bytes(session: aiohttp.ClientSession, entry: dict) -> bytes | None:
    """Bytes for an emoji's source image (local file or site download), or None."""
    if entry.get("path"):
        try:
            with open(entry["path"], "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            log.warning("skip %s: missing local image %s", entry["name"], entry["path"])
            return None
    try:
        async with session.get(entry["url"]) as resp:
            if resp.status != 200:
                log.warning("skip %s: %s returned HTTP %s", entry["name"], entry["url"], resp.status)
                return None
            return await resp.read()
    except aiohttp.ClientError as exc:
        log.warning("skip %s: download failed: %s", entry["name"], exc)
        return None


# --- runtime population -----------------------------------------------------
async def populate(bot, *, session: aiohttp.ClientSession | None = None,
                   create_missing: bool = False) -> None:
    """Rebuild the registry from the bot's application emojis. Never raises.

    When ``create_missing`` is set, any expected emoji the app doesn't have yet is
    uploaded first (images downloaded from the site or read from ``emoji_assets/``),
    so a fresh bot self-provisions its full icon set on startup. Already-present
    emojis are left untouched, so subsequent starts are fast no-ops.
    """
    try:
        found = await bot.fetch_application_emojis()
    except Exception:  # noqa: BLE001 - a fetch failure must not take the bot down
        log.exception("failed to fetch application emojis; icons will fall back to text")
        return

    registry = {e.name: e for e in found}

    # Full wanted set = the static catalogue + the gear items /spec gear needs
    # (gathered from published spec_meta; only present when the bot has site_data).
    wanted = list(expected())
    if getattr(bot, "site_data", None) is not None:
        try:
            wanted += await spec_item_emojis(bot.site_data)
        except Exception:  # noqa: BLE001 - item-emoji gathering must not take the bot down
            log.exception("failed to gather spec item emojis")

    if create_missing:
        pending = [e for e in wanted if e["name"] not in registry]
        if pending:
            log.info("provisioning %d missing application emojis…", len(pending))
            owns_session = session is None
            session = session or aiohttp.ClientSession()
            created = failed = 0
            try:
                for entry in pending:
                    image = await _image_bytes(session, entry)
                    if image is None:
                        failed += 1
                        continue
                    try:
                        # discord.py's HTTP client serialises + waits on rate limits,
                        # so no manual throttle is needed (a per-emoji sleep made the
                        # ~few-hundred item icons take many minutes on a cold boot).
                        registry[entry["name"]] = await bot.create_application_emoji(
                            name=entry["name"], image=image
                        )
                        created += 1
                        log.info("created emoji %s (%d/%d)", entry["name"], created, len(pending))
                    except discord.HTTPException as exc:
                        failed += 1
                        log.warning("failed to create %s: %s", entry["name"], exc)
            finally:
                if owns_session:
                    await session.close()
            log.info("emoji provisioning: %d created, %d failed", created, failed)

    _REGISTRY.clear()
    _REGISTRY.update({name: str(e) for name, e in registry.items()})

    if not _REGISTRY:
        log.error("no application emojis available; icons will fall back to text")
        return

    missing = [e["name"] for e in wanted if e["name"] not in _REGISTRY]
    if missing:
        log.warning(
            "%d/%d expected emojis missing (falling back to text): %s%s",
            len(missing), len(wanted), ", ".join(missing[:15]),
            " …" if len(missing) > 15 else "",
        )
    else:
        log.info("loaded %d application emojis", len(_REGISTRY))
