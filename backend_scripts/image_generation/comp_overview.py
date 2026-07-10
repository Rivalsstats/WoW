"""Renderer for the global top-comps overview image, used for the comp social
post and the comps page's OG preview."""

import os

from PIL import Image, ImageDraw, ImageFont

import databaseConnector
from commonUtils import (
    format_comp_names,
    get_class_lookup,
    get_spec_lookup,
    humanize_number,
    sort_spec_ids_by_role,
)
from image_generation import config
from image_generation.pil_helpers import (
    LANCZOS,
    apply_watermark_to_canvas,
    draw_comp_rows,
    random_background_canvas,
    spec_icon_path,
)


def createCompOverviewImg(tmpdir, out_path, season, conn=None, cursor=None, glue_specs=None):
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    WIDTH, HEIGHT = config.WIDTH, config.HEIGHT

    close_conn = False
    if conn is None or cursor is None:
        conn = databaseConnector.get_connection()
        cursor = conn.cursor(dictionary=True)
        close_conn = True

    try:
        # Fetch stats
        tot = databaseConnector.fetch_total_season_runs(conn, cursor, season)
        play_count = int(tot) if tot else 0

        # Top comps
        top_comps_data = databaseConnector.fetch_global_top_comps(conn, cursor, season)

    finally:
        if close_conn:
            cursor.close()
            conn.close()

    # canvas
    canvas = random_background_canvas(WIDTH, HEIGHT)

    draw = ImageDraw.Draw(canvas, "RGBA")
    font_big = ImageFont.truetype(config.FONT_FILE, config.TITLE_SIZE)
    font_sm = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)

    # Draw Title
    draw.text(
        (50, 30),
        "Global Top Comps",
        font=font_big,
        fill=(255, 255, 255),
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    # Draw Total Runs
    draw.text(
        (50, 130),
        f"{humanize_number(play_count)} total runs tracked across all dungeons",
        font=font_sm,
        fill=(200, 200, 200),
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    # Measure Box Widths to wrap tightly like Spec Page panels
    pad = 20
    icon_sz = 40
    text_offset = 10
    v_spacing = 20

    top_comps_w = 0
    # Measure Top Comps heading
    header_1 = "Top Comps:"
    box1 = draw.textbbox((0, 0), header_1, font=font_sm)
    top_comps_w = max(top_comps_w, box1[2] - box1[0])

    for r in top_comps_data[:5]:
        comp_str = r['comp'] if isinstance(r, dict) else r[0]
        if not comp_str: continue
        comp_cnt = int(r['comp_count'] if isinstance(r, dict) else r[1])
        # calculate row width
        num_icons = len(comp_str.split(','))
        runs_txt = f"Runs: {humanize_number(comp_cnt)}"
        r_box = draw.textbbox((0, 0), runs_txt, font=font_sm)
        # width = icons + offset + text width
        row_w = (num_icons * 45) + text_offset + (r_box[2]-r_box[0])
        top_comps_w = max(top_comps_w, row_w)

    valid_comps_count = sum(1 for r in top_comps_data[:5] if (r['comp'] if isinstance(r, dict) else r[0]))

    glue_comps_w = 0
    valid_glue_count = 0

    if glue_specs:
        header_2 = "Most Flexible Specs:"
        box2 = draw.textbbox((0, 0), header_2, font=font_sm)
        glue_comps_w = max(glue_comps_w, box2[2] - box2[0])

        valid_glue_count = min(5, len(glue_specs))
        for gs in glue_specs[:5]:
            comps_count = gs.get('comps', 0)
            sid = str(gs['spec_id'])
            if sid in spec_lookup:
                s_meta = spec_lookup[sid]
                c_meta = class_lookup.get(str(s_meta.get("classID", "")), {})
                s_name = f"{s_meta.get('name', '')} {c_meta.get('name', '')}"
                txt = f"{s_name} - {comps_count} Comps"
                g_box = draw.textbbox((0, 0), txt, font=font_sm)
                row_w = icon_sz + text_offset + (g_box[2]-g_box[0])
                glue_comps_w = max(glue_comps_w, row_w)

    panel_x1 = 30

    if valid_comps_count > 0:
        box_pw = top_comps_w + (pad * 2)
        # heading at y=200, first row at y=250, each row adds 60
        box_y1 = 180
        box_y2 = 250 + (valid_comps_count * 60)
        draw.rounded_rectangle(
            [(panel_x1, box_y1), (panel_x1 + box_pw, box_y2)],
            radius=15,
            fill=(0, 0, 0, 200)
        )

    if glue_specs and valid_glue_count > 0:
        box_pw = glue_comps_w + (pad * 2)
        panel_x2 = WIDTH // 2 + 30
        box_y1 = 180
        box_y2 = 250 + (valid_glue_count * 60)
        draw.rounded_rectangle(
            [(panel_x2, box_y1), (panel_x2 + box_pw, box_y2)],
            radius=15,
            fill=(0, 0, 0, 200)
        )

    # Top Comps
    draw.text(
        (50, 200),
        "Top Comps:",
        font=font_sm,
        fill=(255, 255, 255),
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    comp_rows = []
    for r in top_comps_data[:5]:
        comp_str = r['comp'] if isinstance(r, dict) else r[0]
        comp_cnt = r['comp_count'] if isinstance(r, dict) else r[1]
        if not comp_str:
            continue
        spec_ids = sort_spec_ids_by_role(comp_str.split(','), spec_lookup)
        comp_rows.append((spec_ids, f"Runs: {humanize_number(int(comp_cnt))}"))
    draw_comp_rows(canvas, draw, comp_rows, spec_lookup, font_sm)

    # Draw Most Flexible Specs (Glue Specs) if provided
    if glue_specs:
        draw.text(
            (WIDTH // 2 + 50, 200),
            "Most Flexible Specs:",
            font=font_sm,
            fill=(255, 255, 255),
            stroke_width=2,
            stroke_fill=(0, 0, 0),
        )

        g_y_offset = 250
        for gs in glue_specs[:5]:
            sid = str(gs['spec_id'])
            comps_count = gs.get('comps', 0)

            if sid in spec_lookup:
                spec_meta = spec_lookup[sid]
                class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
                spec_name = f"{spec_meta.get('name', '')} {class_meta.get('name', '')}"

                # Draw Icon
                icon_file = spec_icon_path(spec_meta)
                if os.path.exists(icon_file):
                    img = Image.open(icon_file).convert("RGBA").resize((40, 40), LANCZOS)
                    canvas.paste(img, (WIDTH // 2 + 50, g_y_offset), img)

                # Draw Text
                class_color_hex = class_meta.get("color", {"r": 255, "g": 255, "b": 255, "a": 1})
                class_color = (int(class_color_hex["r"]), int(class_color_hex["g"]), int(class_color_hex["b"]))

                draw.text(
                    (WIDTH // 2 + 100, g_y_offset + 5),
                    f"{spec_name} - {comps_count} Comps",
                    font=font_sm,
                    fill=class_color,
                    stroke_width=1,
                    stroke_fill=(0, 0, 0)
                )

            g_y_offset += 60

    top_comp_str = ""
    if top_comps_data:
        first_comp = top_comps_data[0]
        comp_str = first_comp['comp'] if isinstance(first_comp, dict) else first_comp[0]
        if comp_str:
            top_spec_ids = sort_spec_ids_by_role(comp_str.split(','), spec_lookup)
            spec_names = []
            for sid in top_spec_ids:
                if sid in spec_lookup:
                    spec_meta = spec_lookup[sid]
                    class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
                    spec_names.append(f"{spec_meta.get('name', '')} {class_meta.get('name', '')}")
            top_comp_str = ", ".join(spec_names)

    most_flexible_spec_str = ""
    if glue_specs and len(glue_specs) > 0:
        sid = str(glue_specs[0].get('spec_id'))
        if sid in spec_lookup:
            spec_meta = spec_lookup[sid]
            class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
            most_flexible_spec_str = f"{spec_meta.get('name', '')} {class_meta.get('name', '')}"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="top_right", padding_x=30, padding_y=30)

    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    runner_up_comp_str = ""
    if top_comps_data and len(top_comps_data) > 1:
        second_comp = top_comps_data[1]
        second_str = (
            second_comp["comp"] if isinstance(second_comp, dict) else second_comp[0]
        )
        runner_up_comp_str = format_comp_names(second_str)

    post_data = {
        "title": "Global Top Comps",
        "amount_data_source_runs": humanize_number(play_count),
        "top_comp": top_comp_str,
        "runner_up_comp": runner_up_comp_str,
        "most_flexible_spec": most_flexible_spec_str
    }

    return {"out_path": out_path, "post_data": post_data}
