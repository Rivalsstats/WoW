"""Shared Pillow helpers for the image renderers: fonts, watermark, icon and
panel drawing. Pure PIL + stdlib — no matplotlib, DB or network access."""

import os
import random
from datetime import datetime, timezone

from PIL import Image, ImageDraw, ImageFont

from image_generation import config

try:
    LANCZOS = Image.Resampling.LANCZOS
except AttributeError:
    LANCZOS = Image.LANCZOS


def format_timestamp(ms_string):
    ms = int(ms_string)
    dt = datetime.fromtimestamp(ms / 1000.0, timezone.utc)
    return dt.strftime("%b %d, %Y")


def fit_font_to_width(
    draw: ImageDraw.Draw,
    text: str,
    max_width: int,
    start_size: int = 200,
    min_size: int = 10,
    step: int = 2,
) -> ImageFont.ImageFont:
    """
    Try sizes from start_size down to min_size (stepping by `step`) and return
    the first truetype font whose rendered width <= max_width. If none fit
    or the TTF can't be loaded at all, falls back to the default font.
    """
    # If the TTF is missing entirely, this will immediately return default.
    for size in range(start_size, min_size - 1, -step):
        try:
            # force-load our one-and-only TTF
            font = ImageFont.truetype(config.FONT_FILE, size)
        except (OSError, IOError):
            # if we can't load Bebas at this size, skip it
            continue

        # measure and return as soon as it fits
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        if text_w <= max_width:
            return font

    # if we never found a fitting Bebas font, fall back once here
    return ImageFont.load_default()


def apply_watermark_to_canvas(canvas, position="top_right", padding_x=30, padding_y=20):
    try:
        canvas = canvas.convert("RGBA")
        draw = ImageDraw.Draw(canvas)
        logo_path = os.path.join("assets", "img", "favicon", "favicon-96x96.png")
        if os.path.exists(logo_path):
            logo = Image.open(logo_path).convert("RGBA").resize((40, 40), LANCZOS)
            logo_width, logo_height = logo.size
        else:
            logo = None
            logo_width, logo_height = 0, 0

        font = ImageFont.truetype(config.FONT_FILE, 36)

        text = "Mythistone.com"
        box = draw.textbbox((0, 0), text, font=font)
        text_width = box[2] - box[0]
        text_height = box[3] - box[1]

        gap = 10 if logo else 0
        total_width = logo_width + gap + text_width

        view_w, view_h = canvas.size

        if position == "top_right":
            start_x = view_w - total_width - padding_x
            start_y = padding_y
        elif position == "top_center":
            start_x = (view_w - total_width) // 2
            start_y = padding_y
        elif position == "top_left":
            start_x = padding_x
            start_y = padding_y
        elif position == "bottom_right":
            start_x = view_w - total_width - padding_x
            start_y = view_h - max(text_height, logo_height) - padding_y
        elif position == "bottom_left":
            start_x = padding_x
            start_y = view_h - max(text_height, logo_height) - padding_y
        else:
            start_x = view_w - total_width - padding_x
            start_y = padding_y

        start_x = int(start_x)
        start_y = int(start_y)

        if logo_height > text_height:
            text_y = int(start_y + (logo_height - text_height) // 2 - box[1])
            logo_y = start_y
        else:
            text_y = int(start_y - box[1])
            logo_y = int(start_y + (text_height - logo_height) // 2)

        # Draw stroke/highlight
        stroke_color = "black"
        stroke_width = 2
        for dx in range(-stroke_width, stroke_width + 1):
            for dy in range(-stroke_width, stroke_width + 1):
                draw.text((start_x + logo_width + gap + dx, text_y + dy), text, font=font, fill=stroke_color)

        # Draw real text
        draw.text((start_x + logo_width + gap, text_y), text, font=font, fill="white")

        if logo:
            canvas.paste(logo, (start_x, logo_y), logo)

        return canvas
    except Exception as e:
        import traceback
        print(f"Error applying watermark: {e}")
        traceback.print_exc()
        return canvas


def watermark_file(out_path, position="bottom_right", padding_x=30, padding_y=10):
    """Open a saved image, stamp the watermark onto it and write it back
    (the load→watermark→save roundtrip every chart renderer ends with)."""
    with Image.open(out_path) as tmp_img:
        img = tmp_img.convert("RGBA")
    img = apply_watermark_to_canvas(img, position=position, padding_x=padding_x, padding_y=padding_y)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        img = img.convert("RGB")
    img.save(out_path)


def cover_crop(img, width, height):
    """Scale `img` up/down so it covers width×height, then center-crop to
    exactly that size (CSS background-size: cover)."""
    bg_w, bg_h = img.size
    scale = max(width / bg_w, height / bg_h)
    new_w = int(bg_w * scale)
    new_h = int(bg_h * scale)
    resized = img.resize((new_w, new_h), LANCZOS)
    left = (new_w - width) // 2
    top = (new_h - height) // 2
    return resized.crop((left, top, left + width, top + height))


def random_background_canvas(width, height, bg_dir=os.path.join("data", "bg_imgs")):
    """Pick a random background image from bg_dir resized to width×height;
    flat dark canvas if the directory is empty or missing."""
    bg_files = [
        os.path.join(bg_dir, f)
        for f in os.listdir(bg_dir)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    ] if os.path.exists(bg_dir) else []
    if not bg_files:
        return Image.new("RGB", (width, height), "#222222")
    canvas = Image.open(random.choice(bg_files)).convert("RGB")
    if canvas.size != (width, height):
        canvas = canvas.resize((width, height), LANCZOS)
    return canvas


def rounded_alpha(img, radius):
    """Put a rounded-corner alpha mask on an RGBA image (in place) and return it."""
    mask = Image.new("L", img.size, 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle([(0, 0), img.size], radius=radius, fill=255)
    img.putalpha(mask)
    return img


def spec_icon_path(spec_meta):
    return os.path.join(config.ICON_DIR, f"{spec_meta['SpellIconFileId']}.jpg")


def load_spec_icon(spec_meta, size):
    """Load a spec's spell icon resized to size×size, or None if missing."""
    icon_file = spec_icon_path(spec_meta)
    if not os.path.exists(icon_file):
        return None
    return Image.open(icon_file).convert("RGBA").resize((size, size), LANCZOS)


def draw_panel(draw, box, radius=12, fill=(0, 0, 0, 200)):
    """Filled rounded-rectangle card with a plain-rectangle fallback for old
    Pillow versions without rounded_rectangle."""
    try:
        draw.rounded_rectangle(box, radius=radius, fill=fill)
    except Exception:
        draw.rectangle(box, fill=fill)


def parse_color(s):
    vals = s[s.find("(") + 1 : s.find(")")].split(",")
    r, g, b, a = map(float, vals)
    return (r / 255, g / 255, b / 255, a)


def compute_panel_width(draw, blocks, font, icon_sz, text_offset, pad):
    max_w = 0
    for blk in blocks:
        for t in blk:
            name = t["name"] if len(t["name"]) < 40 else t["name"][:38] + "..."
            label = (
                f"{name} [{t['pick_rate']:.1f}%]"
                if t["pick_rate"] < 100
                else f"{name} [100%]"
            )
            x0, y0, x1, y1 = draw.textbbox((0, 0), label, font=font)
            max_w = max(max_w, x1 - x0)
    return pad * 2 + icon_sz + text_offset + max_w


def draw_comp_rows(canvas, draw, rows, spec_lookup, font, x0=50, y0=250,
                   icon_sz=40, gap=45, text_offset=10, row_h=60):
    """Draw one comp per row: the spec icons side by side followed by a label.

    rows: iterable of (spec_ids, label) where spec_ids are role-sorted
    spec-id strings and label is the text drawn after the icons.
    Returns the y offset after the last row.
    """
    y_offset = y0
    for spec_ids, label in rows:
        x_offset = x0
        for sid in spec_ids:
            if sid in spec_lookup:
                icon = load_spec_icon(spec_lookup[sid], icon_sz)
                if icon is not None:
                    canvas.paste(icon, (x_offset, y_offset), icon)
            x_offset += gap
        draw.text(
            (x_offset + text_offset, y_offset + 5),
            label,
            font=font,
            fill=(255, 255, 255),
            stroke_width=1,
            stroke_fill=(0, 0, 0),
        )
        y_offset += row_h
    return y_offset
