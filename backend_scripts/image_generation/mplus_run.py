"""Renderer for the single M+ run card (highest/longest/fastest run posts).
Also used by the spec overview, which embeds a run card for the spec's best key.
"""

import os
from contextlib import closing

from PIL import Image, ImageDraw, ImageFont

import databaseConnector
from commonUtils import format_duration, get_class_lookup, get_dungeon_lookup, get_spec_lookup, upgrade_info
from image_generation import config
from image_generation.pil_helpers import (
    apply_watermark_to_canvas,
    cover_crop,
    fit_font_to_width,
    format_timestamp,
)


def create_MplusImage(
    active_run, run, donesocials, check_socials=True, add_region=True, add_season=True, add_watermark=True
):
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    dungeon_lookup = get_dungeon_lookup()

    dungeon_id = str(active_run["dungeon_id"])
    dungeon_meta = dungeon_lookup[dungeon_id]
    dungeon_name = dungeon_meta["name"]["en_US"]
    dungeon_icon = os.path.join(config.ICON_DIR, dungeon_meta["icon"])

    level = active_run["keystone_level"]
    duration_ms = active_run["duration"]
    duration_str = format_duration(duration_ms)
    timestamp = active_run["timestamp"]
    date_str = format_timestamp(timestamp)
    if add_region:
        region = active_run["region"].upper()
    else:
        region = ""
    if add_season:
        season = active_run["season"]
    else:
        season = ""

    members = active_run["members"]
    members = sorted(
        members,
        key=lambda m: (int(spec_lookup[str(m["spec_id"])]["role"]), int(m["spec_id"])),
    )
    out_path = os.path.join(
        config.OUTPUT_DIR, f"{run}_mplus_{dungeon_id}_{level}_{duration_ms}_{timestamp}.png"
    )
    if check_socials and out_path in donesocials:
        return None

    # dungeon background scaled to cover the canvas, then center-cropped
    img = Image.open(dungeon_icon).convert("RGBA")
    bg_crop = cover_crop(img, config.WIDTH, config.HEIGHT)

    # dark overlay for contrast
    overlay = Image.new("RGBA", (config.WIDTH, config.HEIGHT), (0, 0, 0, 120))
    canvas = Image.alpha_composite(bg_crop, overlay).convert("RGB")
    draw = ImageDraw.Draw(canvas)

    # --- header: dungeon icon + name ---
    header_text = f"{dungeon_name} {upgrade_info(duration=duration_ms, upgrade_map=dungeon_meta['keystone_upgrades'], keystone_level=level)['text']}"
    max_header_w = config.WIDTH * 0.8
    title_font = fit_font_to_width(
        draw, header_text, max_header_w, start_size=config.TITLE_SIZE, min_size=12, step=2
    )
    subtitle_font = ImageFont.truetype(config.FONT_FILE, config.SUBTITLE_SIZE)
    draw.text((50, 50), header_text, font=title_font, fill=(255, 255, 255))
    draw.text((50, 130), f"{duration_str}", font=subtitle_font, fill=(200, 200, 200))

    # --- footer: region / season / period ---
    footer_text = ""
    if add_region:
        footer_text = f"{footer_text}Region: {region} "
    if add_season:
        footer_text = f"{footer_text}Season: {season} "
    footer_text = f"{footer_text}Date: {date_str}"
    footer_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
    bbox = draw.textbbox((0, 0), footer_text, font=footer_font)
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    draw.text(
        ((config.WIDTH - w) // 2, config.HEIGHT - h - 20),
        footer_text,
        font=footer_font,
        fill=(180, 180, 180),
    )

    # --- member row ---
    # Each slot: spec icon, class border, spec name
    slot_w, y = 200, 260
    count = len(members)
    total_span = slot_w * (count - 1) if count > 1 else 0
    first_cx = (config.WIDTH // 2) - (total_span // 2)
    for idx, member in enumerate(members):
        spec_id = str(member["spec_id"])
        spec = spec_lookup[spec_id]
        class_id = str(spec["classID"])
        class_info = class_lookup[class_id]

        # load spell icon
        spell_icon_file = os.path.join(config.ICON_DIR, f"{spec['SpellIconFileId']}.jpg")
        icon_img = Image.open(spell_icon_file).resize((80, 80))

        # draw border circle
        cx = first_cx + idx * slot_w
        cy = y + 50
        color = (
            int(class_info["color"]["r"]),
            int(class_info["color"]["g"]),
            int(class_info["color"]["b"]),
        )
        draw.rectangle((cx - 45, cy - 45, cx + 45, cy + 45), outline=color, width=6)

        # paste spec icon
        canvas.paste(
            icon_img, (cx - 40, cy - 40), icon_img if icon_img.mode == "RGBA" else None
        )

        # spec name text
        small_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
        txt = spec["name"]
        bbox = draw.textbbox((0, 0), txt, font=small_font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        draw.text(
            (cx - tw / 2, cy + 50),
            txt,
            font=small_font,
            fill=color,
            stroke_width=2,
            stroke_fill=(0, 0, 0),
        )

    # --- save output ---
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    if add_watermark:
        canvas = apply_watermark_to_canvas(canvas, position="top_right", padding_x=30, padding_y=30)

    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path, format="PNG")
    return {
        "region": region,
        "timestamp": timestamp,
        "duration_str": duration_str,
        "level": level,
        "dungeon_name": dungeon_name,
        "out_path": out_path,
    }


def get_run_data(run_type, spec, season):
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        if spec:
            return databaseConnector.fetch_max_key_run_per_spec(
                conn, cursor, spec, season
            )
        if run_type == "longest_run":
            return databaseConnector.fetch_longest_run(conn, cursor, season)
        if run_type == "highest_run":
            return databaseConnector.fetch_max_key_run(conn, cursor, season)
        if run_type == "shortest_run":
            return databaseConnector.fetch_shortest_run(conn, cursor, season)
    return {}
