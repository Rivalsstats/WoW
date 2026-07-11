"""Renderer for the dungeon overview image (top comps + keystone.guru route
map) used for dungeon social posts and the dungeon pages' OG previews."""

import io
import os
import time

import requests
from PIL import Image, ImageDraw, ImageFont

import databaseConnector
from commonUtils import (
    find_dungeon_meta,
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
    dimmed_cover_bg,
    draw_header,
    draw_panel,
    paste_bordered_spec_icon,
    random_background_canvas,
    rounded_alpha,
)


def createDungeonOverviewImg(tmpdir, out_path, dungeon_id, season, conn=None, cursor=None):
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    WIDTH, HEIGHT = config.WIDTH, config.HEIGHT

    dungeon_meta = find_dungeon_meta(dungeon_id)
    if not dungeon_meta:
        print(f"Could not find dungeon meta for {dungeon_id}")
        return None

    name_text = dungeon_meta["name"]["en_US"]

    close_conn = False
    if conn is None or cursor is None:
        conn = databaseConnector.get_connection()
        cursor = conn.cursor(dictionary=True)
        close_conn = True

    try:
        # Fetch stats
        # Total Runs
        tot = databaseConnector.fetch_dungeon_totals(conn, cursor, dungeon_id, season)
        play_count = 0
        if tot:
            val = list(tot[0].values())[0] if isinstance(tot[0], dict) else tot[0][0]
            play_count = int(val) if val else 0

        # Top comps
        top_comps_data = databaseConnector.fetch_dungeon_top_comps(conn, cursor, dungeon_id, season)

        # Per-level rows for this dungeon (timed %, most-run key, highest key)
        per_level = [
            r for r in databaseConnector.fetch_runs_per_dungeon_per_level(conn, cursor, season)
            if str(r.get("dungeon_id")) == str(dungeon_id)
        ]

        # Top routes
        top_routes_data = databaseConnector.fetch_dungeon_top_routes(conn, cursor, dungeon_id)
    finally:
        if close_conn:
            cursor.close()
            conn.close()

    # canvas: dungeon art scaled to cover, random background as fallback
    dungeon_icon_path = None
    if dungeon_meta and "icon" in dungeon_meta:
        dungeon_icon_path = os.path.join(config.ICON_DIR, dungeon_meta["icon"])

    if dungeon_icon_path and os.path.exists(dungeon_icon_path):
        bg_img = Image.open(dungeon_icon_path)
        canvas = dimmed_cover_bg(bg_img, WIDTH, HEIGHT)
    else:
        canvas = random_background_canvas(WIDTH, HEIGHT, alpha=config.BG_ALPHA, base=config.BG)

    draw = ImageDraw.Draw(canvas, "RGBA")
    font_sm = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)

    draw_header(
        draw,
        name_text,
        f"{humanize_number(play_count)} total runs tracked",
        WIDTH,
        margin=50,
    )

    # quick stats block, right-aligned in the header zone
    timed_pct_str = highest_key = ""
    if per_level:
        timed = sum(int(r.get("upgrade_1", 0)) + int(r.get("upgrade_2", 0)) + int(r.get("upgrade_3", 0)) for r in per_level)
        total = sum(int(r.get("total_runs", 0)) for r in per_level)
        if total:
            timed_pct_str = f"{timed / total * 100:.0f}%"
        levels_run = [r for r in per_level if int(r.get("total_runs", 0)) > 0]
        if levels_run:
            highest_key = f"+{max(int(r['keystone_level']) for r in levels_run)}"
        stat_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE + 4)
        stats = [(label, value) for label, value in (("Timed", timed_pct_str), ("Highest", highest_key)) if value]
        # bottom row sits on the header's subtitle line so the block reads as
        # part of the header instead of floating at the canvas edge
        for i, (label, value) in enumerate(stats):
            y = 124 - (len(stats) - 1 - i) * 42
            draw.text((WIDTH - 50, y), value, font=stat_font, fill=config.TEXT, anchor="ra")
            draw.text(
                (WIDTH - 50 - draw.textlength(value, font=stat_font) - 12, y),
                label.upper(), font=font_sm, fill=config.MUTED, anchor="ra",
            )

    # the route map's fixed geometry; the comps panel mirrors its vertical
    # extent so both blocks share top and bottom edges
    map_w, map_h = 600, 400
    map_x = WIDTH - map_w - 50
    map_y = 220

    pad = 20
    icon_sz = 48
    icon_gap = 5
    text_offset = 14

    comp_rows = []
    for r in top_comps_data[:5]:
        comp_str = r['comp'] if isinstance(r, dict) else r[0]
        comp_cnt = r['comp_count'] if isinstance(r, dict) else r[1]
        if not comp_str:
            continue
        spec_ids = sort_spec_ids_by_role(comp_str.split(','), spec_lookup)
        comp_rows.append((spec_ids, f"Runs: {humanize_number(int(comp_cnt))}"))

    # panel/content start at the header margin so they align with the divider
    panel_x1 = 50

    if comp_rows:
        # Measure Box Widths to wrap tightly like Spec Page panels
        top_comps_w = 0
        for spec_ids, runs_txt in comp_rows:
            r_box = draw.textbbox((0, 0), runs_txt, font=font_sm)
            icons_w = len(spec_ids) * icon_sz + (len(spec_ids) - 1) * icon_gap
            top_comps_w = max(top_comps_w, icons_w + text_offset + (r_box[2] - r_box[0]))
        box_pw = top_comps_w + (pad * 2)

        # heading above the panel, on the same line as the route map's label
        draw.text((panel_x1, map_y - 40), "Top Comps:", font=font_sm, fill=config.MUTED)
        draw_panel(draw, [(panel_x1, map_y), (panel_x1 + box_pw, map_y + map_h)], radius=15)

        # rows spread evenly over the panel's inner height
        avail_h = map_h - pad * 2
        for i, (spec_ids, runs_txt) in enumerate(comp_rows):
            row_cy = map_y + pad + (i + 0.5) * avail_h / len(comp_rows)
            x_offset = panel_x1 + pad
            for sid in spec_ids:
                paste_bordered_spec_icon(canvas, draw, sid, x_offset, int(row_cy - icon_sz / 2),
                                         icon_sz, spec_lookup, class_lookup)
                x_offset += icon_sz + icon_gap
            draw.text((x_offset - icon_gap + text_offset, row_cy), runs_txt,
                      font=font_sm, fill=config.TEXT, anchor="lm")

    # Top Route Image Integration
    if top_routes_data:
        top_route_key = top_routes_data[0]['route_key'] if isinstance(top_routes_data[0], dict) else top_routes_data[0][0]
        if not top_route_key:
            print("Top route key is missing or empty, cannot fetch thumbnail.")
        if top_route_key:
            print(f"Fetching thumbnail for top route: {top_route_key}")

            auth = requests.auth.HTTPBasicAuth(os.environ.get("KEYSTONE_GURU_USER", ""), os.environ.get("KEYSTONE_GURU_PW", ""))

            # Step 1: Check if this dungeon has combined view enabled
            combined_view_enabled = False
            try:
                dungeon_r = requests.get('https://keystone.guru/api/v1/dungeon', timeout=60, auth=auth)
                if dungeon_r.status_code == 200:
                    dungeons_data = dungeon_r.json().get('data', [])
                    for d in dungeons_data:
                        d_name = d.get("name", "")
                        d_key = d.get("key", d.get("slug", ""))
                        if str(d.get("gameVersionId")) == str(dungeon_id) or str(d.get("id")) == str(dungeon_id) or d_name == name_text or d_key == dungeon_meta.get("slug"):
                            combined_view_enabled = d.get("combinedViewEnabled", False)
                            break
            except Exception as e:
                print(f"Error fetching dungeons from keystone.guru: {e}")

            url = f'https://keystone.guru/api/v1/route/{top_route_key}/thumbnail'
            payload = {
              "viewportWidth": 900,
              "viewportHeight": 600,
              "imageWidth": 900,
              "imageHeight": 600,
              "zoomLevel": 2.2,
              "quality": 90
            }
            try:
                r = requests.post(url, json=payload, timeout=60, auth=auth)
                if r.status_code == 200:
                    resp_data = r.json()
                    jobs = resp_data.get("data", [])
                    if jobs:
                        # Important: If combined view exists use the thumbnail of the last floor otherwise use the first floor
                        if combined_view_enabled:
                            job = max(jobs, key=lambda x: x.get("floorIndex", 0))
                        else:
                            job = min(jobs, key=lambda x: x.get("floorIndex", 0))

                        status = job.get("status")

                        if status in ["queued", "processing", "error"]:
                            status_url = job["links"]["status"]
                            for _ in range(15): # wait up to 2 minutes
                                time.sleep(10)
                                poll_r = requests.get(status_url, auth=auth, timeout=20)
                                if poll_r.status_code == 200:
                                    poll_data = poll_r.json()
                                    poll_job = poll_data.get("data", {})
                                    status = poll_job.get("status")
                                    if status == "completed":
                                        job = poll_job
                                        break

                        if status == "completed" and job.get("links", {}).get("result"):
                            img_url = job["links"]["result"]
                            print(f"Thumbnail ready, fetching image from {img_url}...")
                            img_r = requests.get(img_url, timeout=60)
                            if img_r.status_code == 200:
                                print("Thumbnail image fetched successfully, processing image...")
                                route_img = Image.open(io.BytesIO(img_r.content)).convert("RGBA")
                                # Resize map to fix right side smoothly
                                target_w = map_w
                                target_h = int(target_w * (route_img.height / route_img.width))
                                route_img = route_img.resize((target_w, target_h), LANCZOS)

                                # Position on the right side
                                img_x = map_x
                                img_y = map_y

                                # Add simple rounded mask using Pillow
                                route_img = rounded_alpha(route_img, 15)

                                # Paste map
                                canvas.paste(route_img, (img_x, img_y), route_img)

                                # Add label and route key
                                draw.text(
                                    (img_x, img_y - 40),
                                    f"Top Route (keystone.guru/{top_route_key})",
                                    font=font_sm,
                                    fill=config.MUTED,
                                )

                                # Add team comp for this route
                                if isinstance(top_routes_data[0], dict) and top_routes_data[0].get('specs'):
                                    route_specs = top_routes_data[0]['specs']
                                    if route_specs:
                                        r_spec_ids = sort_spec_ids_by_role(
                                            [str(s) for s in route_specs], spec_lookup
                                        )
                                        icon_w = 40
                                        comp_x = img_x + target_w - (len(r_spec_ids) * (icon_w + 5))
                                        comp_y = img_y - 45

                                        for sid in r_spec_ids:
                                            paste_bordered_spec_icon(canvas, draw, sid, int(comp_x), int(comp_y),
                                                                     icon_w, spec_lookup, class_lookup)
                                            comp_x += (icon_w + 5)
                            else:
                                print(f"Getting image for {top_route_key} failed. Status: {img_r.status_code}")
                        else:
                            print(f"Thumbnail job for {top_route_key} failed or missing result. Status: {status}")
                else:
                    print(f"Failed to fetch thumbnail for route {top_route_key}, status code: {r.status_code}")
            except Exception as e:
                print(f"Error fetching thumbnail for route {top_route_key}: {str(e)}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="bottom_right", padding_x=30, padding_y=20)

    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    top_comp_str = ""
    if top_comps_data:
        first_comp = top_comps_data[0]
        comp_str = first_comp["comp"] if isinstance(first_comp, dict) else first_comp[0]
        top_comp_str = format_comp_names(comp_str)

    post_data = {
        "dungeon": name_text,
        "amount_data_source_runs": humanize_number(play_count),
        "timed_pct": timed_pct_str,
        "highest_key": highest_key,
        "top_comp": top_comp_str,
        "top_route": f"keystone.guru/{top_routes_data[0]['route_key'] if isinstance(top_routes_data[0], dict) else top_routes_data[0][0]}" if top_routes_data else "Unknown"
    }

    return {"out_path": out_path, "post_data": post_data}
