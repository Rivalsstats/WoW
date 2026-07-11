"""Shared index-style tierlist card renderer (Pillow only).

Mirrors the landing page's tierlist rows (templates/index.html tier_row macro +
assets/css/tierlist.css): a large rounded tier badge on the left, item icon
tiles to its right, thin divider rules between rows — drawn on the dimmed
random background in the shared modern palette. Used by the spec and dungeon
tierlist social images; tier assignment itself comes from tierMath.
"""

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


def _hex_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def render_tierlist_card(out_path, title, subtitle, tiers):
    """Render a 1200x675 tierlist card and save it to out_path.

    tiers: {"S": [entry, ...], ...} for TIER_LETTERS, each entry a dict with
      icon_path: image file for the tile (skipped silently when missing)
      border:    optional (r, g, b) tile outline (class color); None for a
                 neutral outline
      label:     optional name — labeled entries become wide art banners with
                 the name on a scrim inside the tile (dungeon tiles)
      caption:   optional muted stats text drawn right-aligned on the scrim
      top_left:  optional stats text drawn in a pill at the tile's top left
                 (skipped when the tile is too narrow for it)
      top_right: optional stats text drawn in a pill at the tile's top right
                 (skipped when the tile is too narrow for it)
    """
    W, H = config.WIDTH, config.HEIGHT
    margin = 50

    canvas = random_background_canvas(W, H, alpha=config.BG_ALPHA, base=config.BG)
    draw = ImageDraw.Draw(canvas, "RGBA")

    content_top = draw_header(draw, title, subtitle, W, margin=margin)
    content_bottom = H - 52  # leave room for the brand footer
    row_h = (content_bottom - content_top) / len(TIER_LETTERS)

    badge_sz = min(56, int(row_h - 14))
    badge_font = ImageFont.truetype(config.FONT_FILE, int(badge_sz * 0.62))
    label_font = ImageFont.truetype(config.FONT_FILE, config.VERY_SMALL_SIZE)
    items_x0 = margin + badge_sz + 26
    avail_w = W - margin - items_x0

    for i, letter in enumerate(TIER_LETTERS):
        row_top = content_top + row_h * i
        cy = row_top + row_h / 2
        entries = tiers.get(letter, [])

        # tier badge (outline + dim fill + tinted letter, like the preview badges)
        outline, txt = config.TIER_COLORS.get(letter, ("#9d9d9d", "#b8b8b8"))
        bx0, by0 = margin, cy - badge_sz / 2
        draw.rounded_rectangle(
            [bx0, by0, bx0 + badge_sz, by0 + badge_sz], radius=10,
            fill=blend_toward(_hex_rgb(outline), config.BG, 0.22),
            outline=_hex_rgb(outline), width=2,
        )
        draw.text((bx0 + badge_sz / 2, cy), letter, font=badge_font, fill=_hex_rgb(txt), anchor="mm")

        if not entries:
            continue

        # labeled entries (dungeons) become wide art banners with the name and
        # stats on a scrim inside the tile; unlabeled ones (specs) square icons
        has_labels = any(e.get("label") for e in entries)
        tile_h = int(row_h - 10) if has_labels else int(row_h - 16)
        gap = 12
        n = len(entries)
        tile_w = 300 if has_labels else tile_h
        if n * (tile_w + gap) - gap > avail_w:
            tile_w = max(60, (avail_w - (n - 1) * gap) // n)

        scrim_h = config.VERY_SMALL_SIZE + 10
        x = items_x0
        tile_top = int(cy - tile_h / 2)
        for e in entries:
            icon_path = e.get("icon_path")
            if icon_path and os.path.exists(icon_path):
                try:
                    ic = cover_crop(Image.open(icon_path).convert("RGBA"), tile_w, tile_h)
                    ic = rounded_alpha(ic, 6)
                    canvas.paste(ic, (int(x), tile_top), ic)
                except Exception:
                    pass
            # top corner pills: highest key left, timed stats right — each is
            # skipped when the tile is too narrow for it
            top_right = e.get("top_right") or ""
            right_pill_x0 = int(x) + tile_w  # left edge of the right pill (if drawn)
            if top_right:
                pill_w = int(draw.textlength(top_right, font=label_font)) + 16
                if pill_w <= tile_w - 8:
                    px1 = int(x) + tile_w
                    right_pill_x0 = px1 - pill_w
                    draw.rounded_rectangle(
                        (right_pill_x0, tile_top, px1, tile_top + scrim_h),
                        radius=6, fill=(11, 14, 20, 215),
                    )
                    draw.text(
                        (px1 - 8, tile_top + scrim_h / 2), top_right,
                        font=label_font, fill=config.TEXT, anchor="rm",
                    )
            top_left = e.get("top_left") or ""
            if top_left:
                pill_w = int(draw.textlength(top_left, font=label_font)) + 16
                if int(x) + pill_w <= right_pill_x0 - 8:
                    draw.rounded_rectangle(
                        (int(x), tile_top, int(x) + pill_w, tile_top + scrim_h),
                        radius=6, fill=(11, 14, 20, 215),
                    )
                    draw.text(
                        (int(x) + 8, tile_top + scrim_h / 2), top_left,
                        font=label_font, fill=config.TEXT, anchor="lm",
                    )
            if e.get("label"):
                # bottom scrim: name left, muted stats right
                scrim_top = tile_top + tile_h - scrim_h
                draw.rounded_rectangle(
                    (int(x), scrim_top, int(x) + tile_w, tile_top + tile_h),
                    radius=6, fill=(11, 14, 20, 215),
                )
                draw.rectangle(  # square off the scrim's top edge
                    (int(x), scrim_top, int(x) + tile_w, scrim_top + 8),
                    fill=(11, 14, 20, 215),
                )
                text_y = scrim_top + scrim_h / 2
                caption = e.get("caption") or ""
                caption_w = draw.textlength(caption, font=label_font) if caption else 0
                if caption:
                    draw.text(
                        (int(x) + tile_w - 8, text_y), caption,
                        font=label_font, fill=config.MUTED, anchor="rm",
                    )
                label = e["label"]
                max_label_w = tile_w - 16 - (caption_w + 12 if caption else 0)
                while label and draw.textlength(label + "…", font=label_font) > max_label_w and len(label) > 3:
                    label = label[:-1]
                if label != e["label"]:
                    label = label + "…"
                draw.text((int(x) + 8, text_y), label, font=label_font, fill=config.TEXT, anchor="lm")
            border = e.get("border") or config.PANEL_OUTLINE
            draw.rounded_rectangle(
                (int(x), tile_top, int(x) + tile_w, tile_top + tile_h), radius=6,
                outline=border, width=2,
            )
            x += tile_w + gap

        # thin rule between rows
        if i < len(TIER_LETTERS) - 1:
            ry = row_top + row_h
            draw.line([(margin, ry), (W - margin, ry)], fill=config.DIVIDER, width=1)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=14)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)
