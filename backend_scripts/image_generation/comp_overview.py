"""Renderer for the global top-comps overview image, used for the comp social
post and the comps page's OG preview."""

import os

from PIL import ImageDraw, ImageFont

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
    apply_watermark_to_canvas,
    draw_header,
    draw_panel,
    paste_bordered_spec_icon,
    random_background_canvas,
)


def createCompOverviewImg(tmpdir, out_path, season, conn=None, cursor=None, meta_comp=None,
                          top_comps=None):
    """Render the global comps card: top-5 comps left, meta comp spotlight right.

    meta_comp: optional dict from generateCompPage.compute_meta_comp —
    {"specs": [spec_ids], "runs": int, "timed": int, "max_key": int,
    "popularity_rank": int}. The spotlight is skipped when None; the
    popularity row is skipped when popularity_rank is absent.
    top_comps: optional list from generateCompPage.compute_top_comps (same key
    shape minus avg_key); when given, rows show Timed % and Max Key columns —
    otherwise the runs-only fetch_global_top_comps fallback is used."""
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

        # Top comps (runs-only fallback when the caller has no compiled stats)
        top_comps_data = None
        if top_comps is None:
            top_comps_data = databaseConnector.fetch_global_top_comps(conn, cursor, season)

    finally:
        if close_conn:
            cursor.close()
            conn.close()

    # normalize both sources into row dicts (timed/max_key may be None)
    rows = []
    if top_comps is not None:
        for c in top_comps[:5]:
            rows.append({
                "spec_ids": sort_spec_ids_by_role([str(s) for s in c["specs"]], spec_lookup),
                "runs": int(c.get("runs", 0)),
                "timed": int(c.get("timed", 0)),
                "max_key": int(c.get("max_key", 0)),
            })
    else:
        for r in top_comps_data[:5]:
            comp_str = r['comp'] if isinstance(r, dict) else r[0]
            comp_cnt = r['comp_count'] if isinstance(r, dict) else r[1]
            if not comp_str:
                continue
            rows.append({
                "spec_ids": sort_spec_ids_by_role(comp_str.split(','), spec_lookup),
                "runs": int(comp_cnt),
                "timed": None,
                "max_key": None,
            })

    # canvas: dimmed random background over the dark base
    canvas = random_background_canvas(WIDTH, HEIGHT, alpha=config.BG_ALPHA, base=config.BG)

    draw = ImageDraw.Draw(canvas, "RGBA")
    font_sm = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)

    draw_header(
        draw,
        "Global Top Comps",
        f"{humanize_number(play_count)} total runs tracked across all dungeons",
        WIDTH,
        margin=50,
    )

    def paste_spec_icon(sid, x, y, size):
        paste_bordered_spec_icon(canvas, draw, sid, x, y, size, spec_lookup, class_lookup)

    # --- Top comps table (left half) ---
    pad = 20
    panel_x1 = 50  # aligned with the header divider
    font_row = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE + 4)
    font_col = ImageFont.truetype(config.FONT_FILE, config.VERY_SMALL_SIZE + 2)

    left_x2 = left_y2 = None  # right/bottom edge of the table panel, mirrored by the meta card
    if rows:
        icon_sz, icon_gap = 48, 5
        icons_w = 5 * icon_sz + 4 * icon_gap
        has_stats = rows[0]["timed"] is not None
        # right-aligned value columns after the icons
        columns = [("Runs", 85)] + ([("Timed", 75), ("Max", 65)] if has_stats else [])
        col_gap = 16
        content_w = icons_w + 14 + sum(w for _, w in columns) + col_gap * (len(columns) - 1)
        box_pw = content_w + pad * 2

        heading_y, cols_y, rows_y0, row_h = 200, 242, 268, 64
        box_y2 = rows_y0 + len(rows) * row_h + pad - 10
        left_x2, left_y2 = panel_x1 + box_pw, box_y2
        draw_panel(draw, [(panel_x1, 180), (left_x2, box_y2)], radius=15)

        draw.text((panel_x1 + pad, heading_y), "Top Comps:", font=font_sm, fill=config.MUTED)

        # column headers, right-aligned over their value columns
        col_right = panel_x1 + pad + icons_w + 14
        col_rights = []
        for name, w in columns:
            col_right += w
            col_rights.append(col_right)
            draw.text((col_right, cols_y), name.upper(), font=font_col, fill=config.MUTED, anchor="ra")
            col_right += col_gap

        for i, row in enumerate(rows):
            ry = rows_y0 + i * row_h
            x_offset = panel_x1 + pad
            for sid in row["spec_ids"]:
                paste_spec_icon(sid, x_offset, ry, icon_sz)
                x_offset += icon_sz + icon_gap
            values = [humanize_number(row["runs"])]
            if has_stats:
                timed_pct = (row["timed"] / row["runs"] * 100) if row["runs"] else 0
                values += [f"{timed_pct:.0f}%", f"+{row['max_key']}"]
            vy = ry + icon_sz // 2
            for value, right in zip(values, col_rights):
                draw.text((right, vy), value, font=font_row, fill=config.TEXT, anchor="rm")

    # --- Meta comp spotlight (right, mirrors the table's vertical extent) ---
    if meta_comp and meta_comp.get("specs"):
        meta_spec_ids = sort_spec_ids_by_role([str(s) for s in meta_comp["specs"]], spec_lookup)
        meta_runs = int(meta_comp.get("runs", 0))
        timed_pct = (meta_comp.get("timed", 0) / meta_runs * 100) if meta_runs else 0
        stat_lines = [
            ("Timed", f"{timed_pct:.0f} %"),
            ("Max Key", f"+{meta_comp.get('max_key', 0)}"),
        ]
        if meta_comp.get("popularity_rank"):
            stat_lines.append(("Popularity", f"#{meta_comp['popularity_rank']}"))
        stat_lines.append(("Runs", humanize_number(meta_runs)))

        gutter = 40
        panel_x2 = left_x2 + gutter if left_x2 is not None else panel_x1
        box_x2 = WIDTH - 50  # mirror the left margin
        box_pw = box_x2 - panel_x2
        box_y2 = left_y2 if left_y2 is not None else 560
        draw_panel(draw, [(panel_x2, 180), (box_x2, box_y2)], radius=15)

        draw.text((panel_x2 + pad, 200), "Meta Comp (best for high keys):",
                  font=font_sm, fill=config.MUTED)

        meta_icon_gap = 10
        meta_icon_sz = min(72, (box_pw - 2 * pad - 4 * meta_icon_gap) // 5)
        icons_w = 5 * meta_icon_sz + 4 * meta_icon_gap
        icons_y = 250
        x_offset = panel_x2 + (box_pw - icons_w) // 2  # centered icon row
        for sid in meta_spec_ids:
            paste_spec_icon(sid, x_offset, icons_y, meta_icon_sz)
            x_offset += meta_icon_sz + meta_icon_gap

        # stat rows spread evenly between the icon row and the card bottom
        stats_y0 = icons_y + meta_icon_sz + 12
        avail_h = box_y2 - pad - stats_y0
        for i, (label, value) in enumerate(stat_lines):
            y = stats_y0 + (i + 0.5) * avail_h / len(stat_lines)
            draw.text((panel_x2 + pad, y), f"{label}:", font=font_row, fill=config.MUTED, anchor="lm")
            draw.text((box_x2 - pad, y), value, font=font_row, fill=config.TEXT, anchor="rm")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=20)

    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    top_comp_str = format_comp_names(",".join(rows[0]["spec_ids"])) if rows else ""
    runner_up_comp_str = format_comp_names(",".join(rows[1]["spec_ids"])) if len(rows) > 1 else ""

    post_data = {
        "title": "Global Top Comps",
        "amount_data_source_runs": humanize_number(play_count),
        "top_comp": top_comp_str,
        "runner_up_comp": runner_up_comp_str,
    }
    if meta_comp and meta_comp.get("specs"):
        meta_runs = int(meta_comp.get("runs", 0))
        post_data.update({
            "meta_comp": format_comp_names(",".join(str(s) for s in meta_comp["specs"])),
            "meta_comp_timed_pct": f"{(meta_comp.get('timed', 0) / meta_runs * 100) if meta_runs else 0:.0f}%",
            "meta_comp_max_key": f"+{meta_comp.get('max_key', 0)}",
        })
        if meta_comp.get("popularity_rank"):
            post_data["meta_comp_popularity"] = f"#{meta_comp['popularity_rank']} most played"

    return {"out_path": out_path, "post_data": post_data}
