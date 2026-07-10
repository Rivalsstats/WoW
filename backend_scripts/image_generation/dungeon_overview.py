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
    get_spec_lookup,
    humanize_number,
    sort_spec_ids_by_role,
)
from image_generation import config
from image_generation.pil_helpers import (
    LANCZOS,
    apply_watermark_to_canvas,
    cover_crop,
    draw_comp_rows,
    random_background_canvas,
    rounded_alpha,
    spec_icon_path,
)


def createDungeonOverviewImg(tmpdir, out_path, dungeon_id, season, conn=None, cursor=None):
    spec_lookup = get_spec_lookup()
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
        bg_img = Image.open(dungeon_icon_path).convert("RGB")
        canvas = cover_crop(bg_img, WIDTH, HEIGHT)
    else:
        canvas = random_background_canvas(WIDTH, HEIGHT)

    draw = ImageDraw.Draw(canvas)
    font_big = ImageFont.truetype(config.FONT_FILE, config.TITLE_SIZE)
    font_sm = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)

    # Draw Title
    draw.text(
        (50, 30),
        name_text,
        font=font_big,
        fill=(255, 255, 255),
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    # Draw Total Runs
    draw.text(
        (50, 130),
        f"{humanize_number(play_count)} total runs tracked",
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
                                target_w = 600
                                target_h = int(target_w * (route_img.height / route_img.width))
                                route_img = route_img.resize((target_w, target_h), LANCZOS)

                                # Position on the right side
                                img_x = WIDTH - target_w - 50
                                img_y = 220

                                # Add simple rounded mask using Pillow
                                route_img = rounded_alpha(route_img, 15)

                                # Paste map
                                canvas.paste(route_img, (img_x, img_y), route_img)

                                # Add label and route key
                                draw.text(
                                    (img_x, img_y - 40),
                                    f"Top Route (keystone.guru/{top_route_key})",
                                    font=font_sm,
                                    fill=(255, 255, 255),
                                    stroke_width=2,
                                    stroke_fill=(0, 0, 0),
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
                                            if sid in spec_lookup:
                                                r_icon_file = spec_icon_path(spec_lookup[sid])
                                                if os.path.exists(r_icon_file):
                                                    r_img = Image.open(r_icon_file).convert("RGBA").resize((icon_w, icon_w), LANCZOS)
                                                    canvas.paste(r_img, (int(comp_x), int(comp_y)), r_img)
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
    canvas = apply_watermark_to_canvas(canvas, position="top_right", padding_x=30, padding_y=30)

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
        "top_comp": top_comp_str,
        "top_route": f"keystone.guru/{top_routes_data[0]['route_key'] if isinstance(top_routes_data[0], dict) else top_routes_data[0][0]}" if top_routes_data else "Unknown"
    }

    return {"out_path": out_path, "post_data": post_data}
