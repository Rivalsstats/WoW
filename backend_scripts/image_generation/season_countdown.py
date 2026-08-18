"""Renderer for the pre-season release-countdown card.

Shown by the social auto-poster during the gap between seasons (DB wiped, no runs
logged yet) in place of the normal data cards, which would otherwise render as
empty "0 total runs tracked" images. Pure PIL + seasonInfo.json — no DB, no
Blizzard API, no network. The headline countdown is computed at render time, so a
card rendered each day of the gap counts down on its own.
"""

import os
from datetime import datetime, timezone

from PIL import ImageDraw, ImageFont

from image_generation import config
from image_generation.pil_helpers import (
    apply_watermark_to_canvas,
    draw_header,
    fit_font_to_width,
    random_background_canvas,
)

# Regions shown in the per-region start list, matching the Discord bot's
# season-not-started embed (tw/cn share kr's time, so listing the three primary
# regions keeps the card readable and the two presentations aligned). Only
# regions actually present in seasonInfo.json's "starts" are drawn.
REGION_ORDER = ("us", "eu", "kr")


def _parse_iso(value):
    """Parse an ISO 8601 start time (…Z) into an aware UTC datetime, or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _humanize_until(secs):
    """Compact relative time for an upcoming start, e.g. "in 13 hours", "in 1 day",
    "in 45 minutes". Coarse (single largest unit) to keep region rows readable."""
    secs = int(secs)
    if secs <= 0:
        return "now"
    days = secs // 86400
    hours = (secs % 86400) // 3600
    if days >= 1:
        return f"in {days} day{'s' if days != 1 else ''}"
    if hours >= 1:
        return f"in {hours} hour{'s' if hours != 1 else ''}"
    mins = max(1, (secs % 3600) // 60)
    return f"in {mins} minute{'s' if mins != 1 else ''}"


def launch_fields(season_info, now=None):
    """Split the shown regions into those already live and those still upcoming.

    Returns ``(live, upcoming, earliest)`` where ``live`` and ``upcoming`` are
    lists of ``(REGION, aware-UTC-start)`` tuples (in REGION_ORDER) and
    ``earliest`` is the soonest start (aware UTC) or None.
    """
    now = now or datetime.now(timezone.utc)
    starts = season_info.get("starts", {}) or {}
    parsed = [(r.upper(), dt) for r in REGION_ORDER
              if (dt := _parse_iso(starts.get(r))) is not None]
    live = [(r, dt) for r, dt in parsed if dt <= now]
    upcoming = [(r, dt) for r, dt in parsed if dt > now]
    earliest = min((dt for _, dt in parsed), default=None)
    return live, upcoming, earliest


def in_launch_window(season_info, now=None):
    """True during the first 24h after the earliest regional start: at least one
    region is live but the season is fresh enough that the normal data cards would
    be empty or error. Pure time + seasonInfo, no DB. See social_posts.pipeline."""
    now = now or datetime.now(timezone.utc)
    _, _, earliest = launch_fields(season_info, now)
    if earliest is None:
        return False
    delta = (now - earliest).total_seconds()
    return 0 <= delta < 86400


def countdown_fields(season_info, now=None):
    """Derive the countdown headline and per-region rows from seasonInfo.

    Returns ``(headline_big, headline_small, region_rows, earliest)`` where
    ``region_rows`` is a list of ``(REGION, "Mon DD, HH:MM UTC")`` tuples and
    ``earliest`` is the soonest start (aware UTC) or None.
    """
    now = now or datetime.now(timezone.utc)
    starts = season_info.get("starts", {}) or {}
    parsed = [(r.upper(), dt) for r in REGION_ORDER
              if (dt := _parse_iso(starts.get(r))) is not None]
    earliest = min((dt for _, dt in parsed), default=None)

    if earliest is None:
        headline_big, headline_small = "SOON", "STAY TUNED"
    else:
        secs = (earliest - now).total_seconds()
        if secs <= 0:
            # First region is already live but no runs have been logged yet.
            headline_big, headline_small = "LAUNCHING", "KEYS GO LIVE"
        else:
            days = int(secs // 86400)
            hours = int((secs % 86400) // 3600)
            if days >= 1:
                headline_big = f"{days} DAY{'S' if days != 1 else ''}"
                headline_small = "UNTIL LAUNCH"
            elif hours >= 1:
                headline_big = f"{hours} HOUR{'S' if hours != 1 else ''}"
                headline_small = "UNTIL LAUNCH"
            else:
                headline_big, headline_small = "MINUTES", "UNTIL LAUNCH"

    region_rows = [(region, dt.strftime("%b %d, %H:%M UTC")) for region, dt in parsed]
    return headline_big, headline_small, region_rows, earliest


def create_season_countdown_img(out_path, season_info):
    """Render the release-countdown card to ``out_path``.

    Returns a dict of the computed fields (season name, countdown text, per-region
    starts) so the caller can build post copy without recomputing them.
    """
    WIDTH, HEIGHT = config.WIDTH, config.HEIGHT

    name = season_info.get("name") or season_info.get("slug") or "New Season"
    short = season_info.get("short_name")
    title = f"{name} ({short})" if short else name

    headline_big, headline_small, region_rows, earliest = countdown_fields(season_info)

    canvas = random_background_canvas(WIDTH, HEIGHT, alpha=config.BG_ALPHA, base=config.BG)
    draw = ImageDraw.Draw(canvas, "RGBA")

    content_top = draw_header(draw, title, "Mythic+ season release countdown", WIDTH, margin=50)
    cx = WIDTH // 2

    # Big countdown headline, centred and shrunk to fit the canvas width.
    big_font = fit_font_to_width(draw, headline_big, WIDTH - 160, start_size=190, min_size=48)
    big_cy = content_top + 130
    draw.text((cx, big_cy), headline_big, font=big_font, fill=config.TEXT, anchor="mm")
    big_bbox = draw.textbbox((cx, big_cy), headline_big, font=big_font, anchor="mm")

    # Sub-label under the headline.
    label_font = ImageFont.truetype(config.FONT_FILE, config.SUBTITLE_SIZE)
    label_y = big_bbox[3] + 14
    draw.text((cx, label_y), headline_small, font=label_font, fill=config.MUTED, anchor="ma")
    label_bottom = draw.textbbox((cx, label_y), headline_small, font=label_font, anchor="ma")[3]

    # Per-region start table, centred below the headline.
    if region_rows:
        heading_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
        row_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE + 6)
        block_y = label_bottom + 34
        draw.text((cx, block_y), "SEASON STARTS", font=heading_font,
                  fill=config.MUTED, anchor="ma")
        row_y = draw.textbbox((cx, block_y), "SEASON STARTS", font=heading_font, anchor="ma")[3] + 14
        line_h = config.SMALL_SIZE + 18
        for region, when in region_rows:
            draw.text((cx, row_y), f"{region}  ·  {when}", font=row_font,
                      fill=config.TEXT, anchor="ma")
            row_y += line_h

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=20)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    return {
        "season_name": name,
        "season_short": short or "",
        "countdown": f"{headline_big} {headline_small}".strip(),
        "starts": region_rows,
        "earliest_start": earliest.isoformat() if earliest else "",
    }


def create_season_launch_img(out_path, season_info, now=None):
    """Render the launch-day "season has started" card to ``out_path``.

    Shown by the social auto-poster on launch day (first 24h after the earliest
    regional start), when at least one region is live but the data is still too
    sparse for the normal cards. Pure PIL + seasonInfo.json — no DB, no Blizzard
    API, no network. Returns a dict of the computed fields (live regions, upcoming
    regions with a relative "in X" string) so the caller can build post copy
    without recomputing them.
    """
    WIDTH, HEIGHT = config.WIDTH, config.HEIGHT
    now = now or datetime.now(timezone.utc)

    name = season_info.get("name") or season_info.get("slug") or "New Season"
    short = season_info.get("short_name")
    title = f"{name} ({short})" if short else name

    live, upcoming, earliest = launch_fields(season_info, now)

    canvas = random_background_canvas(WIDTH, HEIGHT, alpha=config.BG_ALPHA, base=config.BG)
    draw = ImageDraw.Draw(canvas, "RGBA")

    content_top = draw_header(draw, title, "Mythic+ season has started", WIDTH, margin=50)
    cx = WIDTH // 2

    # Big "IT'S LIVE" headline, centred and shrunk to fit the canvas width.
    headline_big = "IT'S LIVE"
    big_font = fit_font_to_width(draw, headline_big, WIDTH - 160, start_size=190, min_size=48)
    big_cy = content_top + 120
    draw.text((cx, big_cy), headline_big, font=big_font, fill=config.TEXT, anchor="mm")
    big_bbox = draw.textbbox((cx, big_cy), headline_big, font=big_font, anchor="mm")

    # On-brand closer under the headline.
    label_font = ImageFont.truetype(config.FONT_FILE, config.SUBTITLE_SIZE)
    sub = "SEE YOU IN THE DUNGEONS"
    label_y = big_bbox[3] + 14
    draw.text((cx, label_y), sub, font=label_font, fill=config.MUTED, anchor="ma")
    row_y = draw.textbbox((cx, label_y), sub, font=label_font, anchor="ma")[3] + 30

    heading_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
    row_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE + 6)
    line_h = config.SMALL_SIZE + 18

    def _draw_group(y, heading, rows):
        draw.text((cx, y), heading, font=heading_font, fill=config.MUTED, anchor="ma")
        y = draw.textbbox((cx, y), heading, font=heading_font, anchor="ma")[3] + 12
        for text in rows:
            draw.text((cx, y), text, font=row_font, fill=config.TEXT, anchor="ma")
            y += line_h
        return y

    if live:
        row_y = _draw_group(
            row_y, "LIVE NOW",
            [f"{region}  ·  {dt.strftime('%b %d, %H:%M UTC')}" for region, dt in live],
        )
    if upcoming:
        row_y = _draw_group(
            row_y + 14, "STARTS SOON",
            [f"{region}  ·  {_humanize_until((dt - now).total_seconds())}"
             for region, dt in upcoming],
        )

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=20)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    return {
        "season_name": name,
        "season_short": short or "",
        "live": [region for region, _ in live],
        "upcoming": [(region, _humanize_until((dt - now).total_seconds()))
                     for region, dt in upcoming],
        "earliest_start": earliest.isoformat() if earliest else "",
    }
