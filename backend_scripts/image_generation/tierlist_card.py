"""Shared index-style tierlist card renderer (Pillow only).

Mirrors the landing page's tierlist rows (templates/index.html tier_row macro +
assets/css/tierlist.css): a large rounded tier badge on the left, item icon
tiles to its right, thin divider rules between rows — drawn on the dimmed
random background in the shared modern palette. Used by the spec and dungeon
tierlist social images; tier assignment itself comes from tierMath.
"""

import math
import os

from PIL import Image, ImageDraw, ImageFont

from image_generation import config
from image_generation.pil_helpers import (
    apply_watermark_to_canvas,
    blend_toward,
    cover_crop,
    draw_header,
    random_background_canvas,
    rounded_alpha,
)

TIER_LETTERS = ["S", "A", "B", "C", "D", "F"]

_GAP = 12       # horizontal gap between tiles
_ROW_GAP = 10   # vertical gap between wrapped tile rows within a tier
_PAD_V = 8      # vertical padding inside a tier band


def _hex_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _nominal_metrics():
    """Tile/badge sizes derived from the original fixed 1200x675 design, so a tier
    that fits on one row looks exactly as it always did; only tiers that overflow
    wrap onto extra rows (growing the canvas height)."""
    title_size = int(config.HEIGHT * 0.105)
    subtitle_size = int(config.HEIGHT * 0.046)
    content_top = 38 + title_size + 12 + subtitle_size + 22 + 18
    row_h = (config.HEIGHT - 52 - content_top) / len(TIER_LETTERS)
    badge_sz = min(56, int(row_h - 14))
    return content_top, badge_sz, int(row_h - 16), int(row_h - 10)  # spec_tile, dungeon_tile_h


def _tier_layout(entries, avail_w, spec_tile, dungeon_tile_h):
    """(tile_w, tile_h, cols, n_rows, band_h) for a tier, wrapping tiles to fit."""
    has_labels = any(e.get("label") for e in entries)
    tile_h = dungeon_tile_h if has_labels else spec_tile
    tile_w = min(300 if has_labels else spec_tile, avail_w)
    cols = max(1, (avail_w + _GAP) // (tile_w + _GAP))
    n_rows = max(1, math.ceil(len(entries) / cols)) if entries else 1
    band_h = max(56, n_rows * tile_h + (n_rows - 1) * _ROW_GAP) + 2 * _PAD_V
    return tile_w, tile_h, cols, n_rows, band_h


def _draw_tile(canvas, draw, e, x, y, tile_w, tile_h, label_font, scrim_h):
    x = int(x)
    icon_path = e.get("icon_path")
    if icon_path and os.path.exists(icon_path):
        try:
            ic = cover_crop(Image.open(icon_path).convert("RGBA"), tile_w, tile_h)
            ic = rounded_alpha(ic, 6)
            canvas.paste(ic, (x, y), ic)
        except Exception:
            pass
    top_right = e.get("top_right") or ""
    right_pill_x0 = x + tile_w
    if top_right:
        pill_w = int(draw.textlength(top_right, font=label_font)) + 16
        if pill_w <= tile_w - 8:
            px1 = x + tile_w
            right_pill_x0 = px1 - pill_w
            draw.rounded_rectangle((right_pill_x0, y, px1, y + scrim_h), radius=6, fill=(11, 14, 20, 215))
            draw.text((px1 - 8, y + scrim_h / 2), top_right, font=label_font, fill=config.TEXT, anchor="rm")
    top_left = e.get("top_left") or ""
    if top_left:
        pill_w = int(draw.textlength(top_left, font=label_font)) + 16
        if x + pill_w <= right_pill_x0 - 8:
            draw.rounded_rectangle((x, y, x + pill_w, y + scrim_h), radius=6, fill=(11, 14, 20, 215))
            draw.text((x + 8, y + scrim_h / 2), top_left, font=label_font, fill=config.TEXT, anchor="lm")
    if e.get("label"):
        scrim_top = y + tile_h - scrim_h
        draw.rounded_rectangle((x, scrim_top, x + tile_w, y + tile_h), radius=6, fill=(11, 14, 20, 215))
        draw.rectangle((x, scrim_top, x + tile_w, scrim_top + 8), fill=(11, 14, 20, 215))
        text_y = scrim_top + scrim_h / 2
        caption = e.get("caption") or ""
        caption_w = draw.textlength(caption, font=label_font) if caption else 0
        if caption:
            draw.text((x + tile_w - 8, text_y), caption, font=label_font, fill=config.MUTED, anchor="rm")
        label = e["label"]
        max_label_w = tile_w - 16 - (caption_w + 12 if caption else 0)
        while label and draw.textlength(label + "…", font=label_font) > max_label_w and len(label) > 3:
            label = label[:-1]
        if label != e["label"]:
            label = label + "…"
        draw.text((x + 8, text_y), label, font=label_font, fill=config.TEXT, anchor="lm")
    border = e.get("border") or config.PANEL_OUTLINE
    draw.rounded_rectangle((x, y, x + tile_w, y + tile_h), radius=6, outline=border, width=2)


def render_tierlist_card(out_path, title, subtitle, tiers):
    """Render a tierlist card and save it to out_path.

    The canvas is 1200 wide; its height grows so every tier's tiles fit — a tier
    with more entries than fit on one row wraps onto extra rows instead of running
    off the edge. When every tier fits on a single row the result matches the
    original fixed 1200x675 layout.

    tiers: {"S": [entry, ...], ...} for TIER_LETTERS, each entry a dict with
      icon_path/border/label/caption/top_left/top_right (see _draw_tile).
    """
    W = config.WIDTH
    margin = 50
    content_top, badge_sz, spec_tile, dungeon_tile_h = _nominal_metrics()
    items_x0 = margin + badge_sz + 26
    avail_w = W - margin - items_x0

    # Pass 1: lay out every tier to learn the total height.
    layouts = [_tier_layout(tiers.get(L, []), avail_w, spec_tile, dungeon_tile_h) for L in TIER_LETTERS]
    total_bands = sum(lay[4] for lay in layouts)
    H = int(content_top + total_bands + 52)  # + footer room

    # Pass 2: draw.
    canvas = random_background_canvas(W, H, alpha=config.BG_ALPHA, base=config.BG)
    draw = ImageDraw.Draw(canvas, "RGBA")
    draw_header(draw, title, subtitle, W, margin=margin)

    badge_font = ImageFont.truetype(config.FONT_FILE, int(badge_sz * 0.62))
    label_font = ImageFont.truetype(config.FONT_FILE, config.VERY_SMALL_SIZE)
    scrim_h = config.VERY_SMALL_SIZE + 10

    band_top = content_top
    for i, letter in enumerate(TIER_LETTERS):
        entries = tiers.get(letter, [])
        tile_w, tile_h, cols, n_rows, band_h = layouts[i]
        cy = band_top + band_h / 2

        outline, txt = config.TIER_COLORS.get(letter, ("#9d9d9d", "#b8b8b8"))
        by0 = cy - badge_sz / 2
        draw.rounded_rectangle(
            [margin, by0, margin + badge_sz, by0 + badge_sz], radius=10,
            fill=blend_toward(_hex_rgb(outline), config.BG, 0.22),
            outline=_hex_rgb(outline), width=2,
        )
        draw.text((margin + badge_sz / 2, cy), letter, font=badge_font, fill=_hex_rgb(txt), anchor="mm")

        grid_top = band_top + _PAD_V
        for j, e in enumerate(entries):
            col, row = j % cols, j // cols
            x = items_x0 + col * (tile_w + _GAP)
            y = int(grid_top + row * (tile_h + _ROW_GAP))
            _draw_tile(canvas, draw, e, x, y, tile_w, tile_h, label_font, scrim_h)

        band_top += band_h
        if i < len(TIER_LETTERS) - 1:
            draw.line([(margin, band_top), (W - margin, band_top)], fill=config.DIVIDER, width=1)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=14)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)
