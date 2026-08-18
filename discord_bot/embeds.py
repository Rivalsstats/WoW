"""House-style embed builders, unicode bar charts and Discord-limit clamping.

Everything that assembles fields goes through ``add_fields_capped`` so no embed
can exceed Discord's limits (25 fields, 1024 chars/field, 6000 total). Any string
sourced from outside the static whitelists (NPC names, item names) is markdown-
escaped via ``esc`` before display.
"""

import datetime

import discord

import commonUtils

from . import config, emojis, lookups

BRAND_COLOR = discord.Colour(0x11151E)  # image_generation.config.BG_HEX
BRAND_NAME = "Mythistone"
BRAND_ICON = f"{config.SITE_BASE}/assets/img/favicon/web-app-manifest-192x192.png"

_BAR_FULL = "█"
_BAR_EMPTY = "░"

# Discord hard limits.
MAX_FIELDS = 25
MAX_FIELD_VALUE = 1024
MAX_FIELD_NAME = 256
MAX_TOTAL = 6000
MAX_TITLE = 256
MAX_DESC = 4096


def class_colour(class_id) -> discord.Colour:
    meta = lookups.CLASSES.get(str(class_id), {})
    colour = meta.get("color") or {}
    try:
        r = int(colour.get("r", 0x96))
        g = int(colour.get("g", 0x9E))
        b = int(colour.get("b", 0xAC))
        return discord.Colour.from_rgb(r, g, b)
    except (TypeError, ValueError):
        return BRAND_COLOR


def make_bar(pct: float, width: int = 12) -> str:
    try:
        pct = max(0.0, min(100.0, float(pct)))
    except (TypeError, ValueError):
        pct = 0.0
    filled = round(pct / 100.0 * width)
    filled = max(0, min(width, filled))
    return _BAR_FULL * filled + _BAR_EMPTY * (width - filled)


def esc(text) -> str:
    return discord.utils.escape_markdown(str(text)) if text is not None else ""


def clamp(text, limit: int) -> str:
    text = "" if text is None else str(text)
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return text[: limit - 1].rstrip() + "…"


def base_embed(title, *, url=None, colour=BRAND_COLOR, description=None) -> discord.Embed:
    embed = discord.Embed(
        title=clamp(title, MAX_TITLE),
        url=url,
        colour=colour,
        description=clamp(description, MAX_DESC) if description else None,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )
    brand_footer(embed)
    return embed


def brand_footer(embed: discord.Embed, prefix: str | None = None) -> discord.Embed:
    """Set the standard branded footer: optional ``prefix`` + logo + 'Mythistone'.

    Discord footers are plain text (no hyperlink), so this is the brand's bottom-of-
    embed home; the logo shows as the footer icon.
    """
    text = f"{prefix} • {BRAND_NAME}" if prefix else BRAND_NAME
    embed.set_footer(text=text, icon_url=BRAND_ICON)
    return embed


def add_fields_capped(embed: discord.Embed, fields) -> None:
    """Add (name, value, inline) tuples while respecting every embed limit.

    Empty values are skipped. Trailing fields are dropped (with a note appended
    to the description) if they would push the embed past 6000 characters.
    """
    for name, value, inline in fields:
        if value is None or value == "":
            continue
        if len(embed.fields) >= MAX_FIELDS:
            _note_truncated(embed)
            break
        name = clamp(name, MAX_FIELD_NAME)
        value = clamp(value, MAX_FIELD_VALUE)
        if len(embed) + len(name) + len(value) > MAX_TOTAL:
            _note_truncated(embed)
            break
        embed.add_field(name=name, value=value, inline=inline)


def _note_truncated(embed: discord.Embed):
    note = "… some details were truncated to fit."
    desc = embed.description or ""
    if note not in desc and len(desc) + len(note) + 1 <= MAX_DESC:
        embed.description = (desc + "\n" + note).strip()


def discord_ts(iso: str, style: str = "F") -> str:
    """ISO 8601 -> a Discord timestamp tag that renders in the viewer's own
    timezone (e.g. ``<t:1699999999:F>``). Style ``F`` = full date-time, ``R`` =
    relative ("in 3 days"), ``D`` = date only."""
    if not iso:
        return "—"
    ts = int(datetime.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    return f"<t:{ts}:{style}>"


def season_not_started_embed() -> discord.Embed:
    """Shown in place of any command output during the pre-season gap / just after
    a season wipe, when the current season has no runs yet. Surfaces the per-region
    start times as live Discord timestamps so each viewer sees their local time."""
    info = config.SEASON_INFO
    title = config.SEASON_NAME
    if config.SEASON_SHORT:
        title = f"{title} ({config.SEASON_SHORT})"
    embed = base_embed(
        title,
        url=f"{config.SITE_BASE}/pages/dashboard",
        description=(
            "This season hasn't started yet, so there are no Mythic+ runs to show. "
            "Stats will light up once the first keys are logged."
        ),
    )
    starts = (info or {}).get("starts", {})
    lines = [
        f"{region.upper()}: {discord_ts(starts.get(region), 'F')} "
        f"({discord_ts(starts.get(region), 'R')})"
        for region in ("us", "eu", "kr")
        if starts.get(region)
    ]
    if lines:
        add_fields_capped(embed, [("Season starts", "\n".join(lines), False)])
    return embed


def season_started_embed() -> discord.Embed:
    """Shown in place of any command output during the season's first 24h (launch
    day), when at least one region is live but the data is still too sparse for the
    normal command output to be meaningful (barely any runs yet, later regions not
    even started). Splits the regions into those already live and those still to
    come, surfacing the latter as live Discord timestamps so each viewer sees their
    own local time. Mirrors the social auto-poster's launch-day post
    (backend_scripts/social_posts/posts.create_season_launch)."""
    info = config.SEASON_INFO
    title = config.SEASON_NAME
    if config.SEASON_SHORT:
        title = f"{title} ({config.SEASON_SHORT})"
    embed = base_embed(
        title,
        url=f"{config.SITE_BASE}/pages/dashboard",
        description=(
            "This season has started! The first runs are only just coming in, so "
            "the stats will fill out over the next few hours as the data lands. "
            "See you in the dungeons."
        ),
    )
    starts = (info or {}).get("starts", {})
    now = datetime.datetime.now(datetime.timezone.utc)
    live, upcoming = [], []
    for region in ("us", "eu", "kr"):
        iso = starts.get(region)
        if not iso:
            continue
        started = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00")) <= now
        if started:
            live.append(f"{region.upper()}: live now")
        else:
            upcoming.append(
                f"{region.upper()}: {discord_ts(iso, 'F')} ({discord_ts(iso, 'R')})"
            )
    fields = []
    if live:
        fields.append(("Live now", "\n".join(live), False))
    if upcoming:
        fields.append(("Starts soon", "\n".join(upcoming), False))
    if fields:
        add_fields_capped(embed, fields)
    return embed


def spec_embed_header(spec_id) -> discord.Embed:
    sid = str(spec_id)
    cid = str(lookups.SPECS.get(sid, {}).get("classID", ""))
    embed = base_embed(
        lookups.spec_full_name(sid),
        url=lookups.spec_site_url(sid),
        colour=class_colour(cid),
    )
    icon = lookups.spec_icon_url(sid)
    if icon:
        embed.set_thumbnail(url=icon)
    return embed


def dungeon_embed_header(dungeon_id) -> discord.Embed:
    did = str(dungeon_id)
    embed = base_embed(lookups.dungeon_name(did), url=lookups.dungeon_site_url(did))
    icon = lookups.dungeon_icon_url(did)
    if icon:
        embed.set_thumbnail(url=icon)
    return embed


def set_dungeon_thumbnail(embed, dungeon_id) -> None:
    """Set the dungeon icon as the embed thumbnail — used when a comp or run is tied
    to a single dungeon (comps top/fill with a dungeon filter, the stats run cards)."""
    if not dungeon_id:
        return
    icon = lookups.dungeon_icon_url(dungeon_id)
    if icon:
        embed.set_thumbnail(url=icon)


def pct_row(label, count, total, width: int = 12) -> str:
    total = total or 0
    pct = (count / total * 100.0) if total else 0.0
    return f"`{make_bar(pct, width)}` {label} — {int(count):,} ({pct:.1f}%)"


def comp_line(spec_ids, *, with_names: bool = True) -> str:
    """Role-sorted comp, each spec prefixed with its emoji (when available).

    ``with_names=True`` renders ``<emoji> Spec Class`` joined by ' · ' (the readable
    default used everywhere comps are listed). ``with_names=False`` renders a compact
    icon-only row. Emojis silently degrade to text when the registry is empty.
    """
    ids = [str(s) for s in spec_ids if s is not None]
    ordered = commonUtils.sort_spec_ids_by_role(ids, lookups.SPECS)
    parts = []
    for sid in ordered:
        icon = emojis.spec(sid)
        if with_names:
            name = lookups.spec_full_name(sid)
            parts.append(f"{icon} {name}" if icon else name)
        else:
            parts.append(icon or lookups.spec_full_name(sid))
    return (" · " if with_names else " ").join(parts)


def raider_io_run_url(run_id) -> str:
    return f"https://raider.io/mythic-plus-runs/{config.SEASON_SLUG}/{run_id}"


def keystone_guru_route_url(route_key, dungeon_id) -> str:
    """keystone.guru route link, matching the site's format
    ``/route/<dungeon-slug>/<route_key>/<dungeon-slug>`` (see find_routes.html)."""
    slug = lookups.DUNGEONS.get(str(dungeon_id), {}).get("slug", "")
    return f"https://keystone.guru/route/{slug}/{route_key}/{slug}"


def _upgrade_text(dungeon_id, duration, keystone_level) -> str:
    upgrade_map = lookups.DUNGEONS.get(str(dungeon_id), {}).get("keystone_upgrades", {})
    if not upgrade_map:
        return f"+{keystone_level}" if keystone_level is not None else ""
    return commonUtils.upgrade_info(duration, upgrade_map, keystone_level)["text"]


def run_lines(top_run: dict, icon_comp: bool = False) -> str:
    """Render a run dict (fetch_max_key_run / fetch_longest_run shape).

    ``icon_comp`` renders the team as an icon-only row (like the comps command)
    instead of ``<emoji> Spec Class`` names."""
    if not top_run:
        return ""
    did = top_run.get("dungeon_id")
    level = top_run.get("keystone_level")
    duration = top_run.get("duration")
    region = (top_run.get("region") or "").upper()
    run_id = top_run.get("run_id")
    spec_ids = [m.get("spec_id") for m in top_run.get("members", []) if m.get("spec_id")]

    key = _upgrade_text(did, duration, level)
    parts = [f"**{key} {lookups.dungeon_name(did)}** — {commonUtils.format_duration(duration)}"]
    if region:
        parts[0] += f" · {region}"
    if spec_ids:
        parts.append(comp_line(spec_ids, with_names=not icon_comp))
    if run_id is not None:
        parts.append(f"[View on Raider.IO]({raider_io_run_url(run_id)})")
    return "\n".join(parts)


def _rows_to_run(rows) -> dict:
    """Collapse dungeon-domain member-joined dict rows into a run_lines-shaped dict."""
    rows = list(rows or [])
    if not rows:
        return {}
    first = rows[0]

    def col(row, key, idx):
        return row.get(key) if isinstance(row, dict) else row[idx]

    seen = set()
    members = []
    for row in rows:
        member = col(row, "member", 8)
        spec_id = col(row, "spec_id", 9)
        if member is None or member in seen:
            continue
        seen.add(member)
        members.append({"member_id": member, "spec_id": spec_id})
    return {
        "run_id": col(first, "run_id", 5),
        "dungeon_id": col(first, "dungeon_id", 0),
        "keystone_level": col(first, "keystone_level", 1),
        "duration": col(first, "duration", 2),
        "timestamp": col(first, "timestamp", 3),
        "faction": col(first, "faction", 4),
        "region": col(first, "region", 6),
        "season": col(first, "season", 7),
        "members": members,
    }


def dungeon_record_lines(rows) -> str:
    return run_lines(_rows_to_run(rows))
