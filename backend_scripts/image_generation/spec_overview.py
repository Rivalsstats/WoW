"""Renderer for the spec overview image (talents, stats, hero trees, gems,
best run) used for spec social posts and the spec pages' OG previews."""

import os
from contextlib import closing
from datetime import datetime, timezone

import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw, ImageFont

import aggregateData
import databaseConnector
from chartData import RARITY_COLORS
from commonUtils import (
    LOOKUP_DIR,
    fetch_stat_info,
    get_class_lookup,
    get_spec_lookup,
    humanize_number,
    load_json,
)
from image_generation import config
from image_generation.mpl_setup import init_matplotlib
from image_generation.mplus_run import create_MplusImage, get_run_data
from image_generation.pil_helpers import (
    LANCZOS,
    apply_watermark_to_canvas,
    compute_panel_width,
    draw_panel,
    format_timestamp,
    random_background_canvas,
    rounded_alpha,
)


def createSpecOverviewImg(tmpdir, out_path, spec_id, season):
    """
    Creates and saves a spec overview image.
    """
    init_matplotlib()
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()

    WIDTH, HEIGHT, DPI = config.WIDTH, config.HEIGHT, config.DPI

    talent_lookup = load_json(os.path.join(LOOKUP_DIR, "talents", f"{spec_id}.json"))

    # gather data
    spec_meta = spec_lookup.get(spec_id, {})
    class_meta = class_lookup.get(str(spec_meta.get("classID", "")), {})
    name_text = f"{spec_meta.get('name', '')} {class_meta.get('name', '')}"

    # upgrade distribution
    tiers = ["depleted", "1", "2", "3"]
    upgrade_counts = {tier: 0 for tier in tiers}
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrade_counts = databaseConnector.fetch_spec_upgrade(
            conn, cursor, spec_id
        )
        play_count = 0
        for u in spec_upgrade_counts:
            upgrade_counts[u["upgrade_tier"]] += int(u["run_count"])
            play_count += int(u["run_count"])

        counts_list = [upgrade_counts[t] for t in tiers]
        total_up = sum(counts_list) or 1

        # hero tree picks
        hero_trees_raw = databaseConnector.fetch_hero_tree_overview(
            conn, cursor, spec_id
        )
        valid_subtrees = set(talent_lookup.get("subTrees", {}).keys())
        hero_trees = []
        for row in hero_trees_raw:
            tree_id = row[0]
            tree_count = row[1]
            # Drop hero trees that don't belong to this spec (e.g. cross-spec
            # contaminated loadouts). Without this the subTrees lookup below
            # KeyErrors and the whole overview image fails to render.
            if str(tree_id) not in valid_subtrees:
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] "
                    f"WARNING: spec {spec_id} returned hero tree {tree_id} "
                    f"(count={tree_count}) which is not in its subTrees "
                    f"{sorted(valid_subtrees)}; skipping contaminated loadout."
                )
                continue
            hero_trees.append({"tree_id": tree_id, "count": tree_count})

        hero_total = sum(tree["count"] for tree in hero_trees)

        # runs
        highest = get_run_data(False, spec_id, season)
        highest_run = create_MplusImage(
            highest, "highest_run", {}, False, False, False, False
        )
        spec_talent_overview = databaseConnector.fetch_spec_talent_overview(
            conn, cursor, spec_id, season
        )
        class_talent_overview = databaseConnector.fetch_class_talent_overview(
            conn, cursor, spec_id, season
        )
        missives = databaseConnector.fetch_missive_count(conn, cursor, spec_id, season)
        total_missive_count = sum(e[1] for e in missives)

        embellishments = databaseConnector.fetch_embellishment_count(
            conn, cursor, spec_id, season
        )

        total_embellishment_count = sum(e[1] for e in embellishments)

        sockets = aggregateData.get_sockets(conn, cursor, spec_id, season)

        total_socket_count = sum(s.get("count", 0) for s in sockets)

        stat_priority, tertiary_priority, health_priority = fetch_stat_info(
            conn, cursor, spec_id, season, spec_lookup
        )

    # canvas: random background image (flat dark fallback)
    canvas = random_background_canvas(WIDTH, HEIGHT)

    draw = ImageDraw.Draw(canvas)
    font_big = ImageFont.truetype(config.FONT_FILE, config.TITLE_SIZE)
    font_med = ImageFont.truetype(config.FONT_FILE, config.SUBTITLE_SIZE)
    font_sm = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
    font_vsm = ImageFont.truetype(config.FONT_FILE, config.VERY_SMALL_SIZE)

    # header
    class_color = (
        int(class_meta["color"]["r"]),
        int(class_meta["color"]["g"]),
        int(class_meta["color"]["b"]),
    )
    draw.text(
        (50, 30),
        name_text,
        font=font_big,
        fill=class_color,
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )
    icon_file = os.path.join(config.ICON_DIR, f"{spec_meta.get('SpellIconFileId')}.jpg")
    if os.path.exists(icon_file):
        icon = Image.open(icon_file).resize((80, 80))
        canvas.paste(icon, (WIDTH - 130, 20))

    # Panel: upgrades via matplotlib
    key_upgrades_panel_h = 25
    fig, ax = plt.subplots(figsize=(WIDTH / DPI, key_upgrades_panel_h / DPI), dpi=DPI)
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")
    left = 0
    upgrade_map = {"depleted": "Depleted", "1": "+1", "2": "+2", "3": "+3"}
    colors = [
        RARITY_COLORS["Depleted"],
        RARITY_COLORS["Uncommon"],
        RARITY_COLORS["Epic"],
        RARITY_COLORS["Legendary"],
    ]
    for tier, col in zip(upgrade_counts, colors):
        frac = upgrade_counts[tier] / total_up * 100
        ax.barh(0, frac, left=left, color=col)
        if frac > 5:
            label_text = f"{upgrade_map[tier]} ({round(frac, 2)} %)"
        else:
            label_text = f"{upgrade_map[tier]}"
        txt = ax.text(
            left + frac / 2,
            0,  # x, y
            label_text,  # label text
            va="center",
            ha="center",
            color="white",
            fontsize=config.VERY_SMALL_SIZE,
            fontweight="bold",
        )
        txt.set_path_effects(
            [path_effects.withStroke(linewidth=1.5, foreground="black")]
        )
        left += frac
    ax.axis("off")
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    buf = os.path.join(tmpdir, "tmp_upgrade.png")
    os.makedirs(os.path.dirname(buf), exist_ok=True)
    plt.savefig(buf, facecolor=fig.get_facecolor())
    plt.close(fig)
    panel1 = Image.open(buf)
    canvas.paste(panel1, (0, HEIGHT - key_upgrades_panel_h))
    draw.text(
        (60, 105),
        f"{humanize_number(play_count)} total runs",
        font=font_sm,
        fill=class_color,
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    # Panel: hero trees
    x0, y0 = round(2 * (WIDTH / 3)), 20
    icon_size = 64
    for i, ht in enumerate(hero_trees):
        tree_icon = os.path.join(
            config.ICON_DIR, f"{talent_lookup['subTrees'][str(ht['tree_id'])]['icon']}.png"
        )
        pct = ht["count"] / hero_total * 100 if hero_total else 0
        if os.path.exists(tree_icon):
            img = Image.open(tree_icon).convert("RGBA")
            img = img.resize((icon_size, icon_size), LANCZOS)

            x = x0 + i * 150
            y = y0
            canvas.paste(img, (x, y), img)
            text = f"{pct:.0f}%"
            cx = x + icon_size // 2
            cy = y + icon_size // 2

            draw.text(
                (cx, cy),
                text,
                font=font_sm,
                anchor="mm",
                fill=class_color,
                stroke_width=2,
                stroke_fill=(0, 0, 0),
            )
            name_x = cx
            name_y = y + icon_size + 5
            draw.text(
                (name_x, name_y),
                talent_lookup["subTrees"][str(ht["tree_id"])]["name"],
                font=font_vsm,
                anchor="mt",
                fill=class_color,
                stroke_width=2,
                stroke_fill=(0, 0, 0),
            )

    # Panel: runs
    # ------------------- PANEL: Highest Key (1/3) + Primary & Tertiary stat panels (filled backgrounds) -------------------
    # prepare primary (skip first element) and tertiary lists
    if stat_priority and len(stat_priority) > 1:
        prim_list = stat_priority[1:5]  # skip first element, up to 4
    else:
        prim_list = stat_priority[:4] if stat_priority else []
    tert_list = tertiary_priority[:4] if tertiary_priority else []

    outer_margin = 30
    inner_margin = 18

    image_panel_w = round(WIDTH * 0.33)
    remaining = WIDTH - 2 * outer_margin - image_panel_w - 2 * inner_margin
    stat_panel_w = max(180, round(remaining / 2))
    panel_height = round(HEIGHT / 3)
    panel_y_offset = HEIGHT - panel_height - 30
    panel_y_text_off = panel_y_offset - 20
    corner_radius = 12
    inset = 10

    # fonts
    stat_label_font = ImageFont.truetype(config.FONT_FILE, max(10, config.SMALL_SIZE))
    stat_value_font = ImageFont.truetype(config.FONT_FILE, max(12, config.SMALL_SIZE + 2))
    panel_title_font = font_med

    x_image = outer_margin
    x_stat_primary = x_image + image_panel_w + inner_margin
    x_stat_tertiary = x_stat_primary + stat_panel_w + inner_margin

    # ---------- draw image panel (left) with filled background ----------
    draw_panel(
        draw,
        [
            (x_image, panel_y_offset),
            (x_image + image_panel_w, panel_y_offset + panel_height),
        ],
        radius=corner_radius,
        fill=(0, 0, 0, 200),
    )

    draw.text(
        (x_image + image_panel_w // 2, panel_y_text_off),
        "Highest Key",
        anchor="mm",
        font=panel_title_font,
        fill=class_color,
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    content_x = x_image + inset
    content_y = panel_y_offset + inset
    content_w = image_panel_w - 2 * inset
    content_h = panel_height - 2 * inset

    if highest_run and isinstance(highest_run, dict) and highest_run.get("out_path"):
        try:
            img = Image.open(highest_run["out_path"]).convert("RGBA")
            img_ratio = img.width / img.height
            content_ratio = content_w / content_h
            if img_ratio > content_ratio:
                scaled_h = content_h
                scaled_w = int(img_ratio * scaled_h)
            else:
                scaled_w = content_w
                scaled_h = int(scaled_w / img_ratio)
            img = img.resize((scaled_w, scaled_h), LANCZOS)
            left = (scaled_w - content_w) // 2
            top = (scaled_h - content_h) // 2
            img = img.crop((left, top, left + content_w, top + content_h))
            img = rounded_alpha(img, corner_radius)
            canvas.paste(img, (content_x, content_y), img)
        except Exception:
            draw.rectangle(
                [
                    (content_x, content_y),
                    (content_x + content_w, content_y + content_h),
                ],
                fill=(40, 40, 40),
            )
            draw.text(
                (content_x + content_w / 2, content_y + content_h / 2),
                "Image\nmissing",
                anchor="mm",
                font=stat_label_font,
                fill="white",
            )
    else:
        draw.rectangle(
            [(content_x, content_y), (content_x + content_w, content_y + content_h)],
            fill=(40, 40, 40),
        )
        draw.text(
            (content_x + content_w / 2, content_y + content_h / 2),
            "No run",
            anchor="mm",
            font=stat_label_font,
            fill="white",
        )

    # ---------- draw PRIMARY stat panel (middle) filled ----------
    draw_panel(
        draw,
        [
            (x_stat_primary, panel_y_offset),
            (x_stat_primary + stat_panel_w, panel_y_offset + panel_height),
        ],
        radius=corner_radius,
        fill=(0, 0, 0, 200),
    )

    draw.text(
        (x_stat_primary + stat_panel_w // 2, panel_y_text_off),
        "Stat Priority",
        anchor="mm",
        font=panel_title_font,
        fill=class_color,
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    # content region for primary panel
    content_x = x_stat_primary + inset
    content_y = panel_y_offset + inset
    content_w = stat_panel_w - 2 * inset
    content_h = panel_height - 2 * inset

    # fixed horizontal center for chevrons (same for every row in this panel)
    chevron_center_x_primary = content_x + content_w // 2

    # evenly spaced blocks, center-first assignment
    # ---------- PRIMARY stats: vertical stacked rows, centered vertically ----------
    n = len(prim_list)
    if n == 0:
        draw.text(
            (x_stat_primary + stat_panel_w // 2, content_y + content_h // 2),
            "No data",
            font=stat_label_font,
            anchor="mm",
            fill=(160, 160, 160),
        )
    else:
        padding = 8
        icon_sz = min(36, int(content_h * 0.18))
        row_h = max(icon_sz, stat_label_font.size + stat_value_font.size) + 8
        total_h = n * row_h - 8  # remove extra gap after last
        start_y = content_y + max(0, (content_h - total_h) // 2)

        for i, s in enumerate(prim_list):
            row_top = int(start_y + i * row_h)
            # icon
            ix = content_x + padding
            iy = row_top + (row_h - icon_sz) // 2
            stat_name_raw = (s.get("name") or "").lower().replace(" ", "_")
            icon_file = os.path.join(config.ICON_DIR, "stats", f"{stat_name_raw}.png")
            if not os.path.exists(icon_file):
                icon_file = os.path.join(config.ICON_DIR, f"{stat_name_raw}.png")
            if os.path.exists(icon_file):
                try:
                    ic = (
                        Image.open(icon_file)
                        .convert("RGBA")
                        .resize((icon_sz, icon_sz), LANCZOS)
                    )
                    canvas.paste(ic, (ix, iy), ic)
                except Exception:
                    draw.rectangle(
                        (ix, iy, ix + icon_sz, iy + icon_sz), fill=(100, 100, 100)
                    )
                    draw.text(
                        (ix + icon_sz // 2, iy + icon_sz // 2),
                        (s.get("name", "")[:1] or "?"),
                        font=stat_label_font,
                        anchor="mm",
                        fill="white",
                    )
            else:
                draw.rectangle(
                    (ix, iy, ix + icon_sz, iy + icon_sz), fill=(100, 100, 100)
                )
                draw.text(
                    (ix + icon_sz // 2, iy + icon_sz // 2),
                    (s.get("name", "")[:1] or "?"),
                    font=stat_label_font,
                    anchor="mm",
                    fill="white",
                )

            # name (left of center)
            name_x = ix + icon_sz + 8

            # value (right aligned)
            if s.get("avg_percent") is not None:
                try:
                    val_txt = f"{float(s['avg_percent']):.2f}%"
                except Exception:
                    val_txt = "-"
            else:
                try:
                    val_txt = f"{float(s.get('avg_raw', 0)):.0f}"
                except Exception:
                    val_txt = "-"

            # measure value bbox precisely (we will use draw.textbbox to compute top/bottom)
            val_bbox = draw.textbbox((0, 0), val_txt, font=stat_value_font)
            val_w = val_bbox[2] - val_bbox[0]
            val_x = content_x + content_w - padding - val_w

            # truncate name to avoid collision with value
            max_name_w = val_x - 6 - name_x
            name_text_row = (s.get("name") or "").capitalize()
            if max_name_w <= 0:
                name_draw = ""
            else:
                name_draw = name_text_row
                nbbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
                # shorten until it fits
                while nbbox[2] - nbbox[0] > max_name_w and len(name_draw) > 1:
                    name_draw = name_draw[:-1]
                    nbbox = draw.textbbox((0, 0), name_draw + "…", font=stat_label_font)
                if nbbox[2] - nbbox[0] > max_name_w:
                    name_draw = ""
                elif name_draw != name_text_row:
                    name_draw = name_draw + "…"

            # compute precise bboxes for vertical centering
            if name_draw:
                name_bbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
            else:
                name_bbox = (0, 0, 0, 0)
            val_bbox = draw.textbbox((0, 0), val_txt, font=stat_value_font)

            # icon center
            icon_center = iy + icon_sz / 2.0

            # text vertical center when drawn at y is y + (bbox_top + bbox_bottom)/2
            # so solve for y = icon_center - (bbox_top + bbox_bottom)/2
            name_y = int(icon_center - (name_bbox[1] + name_bbox[3]) / 2.0)
            val_y = int(icon_center - (val_bbox[1] + val_bbox[3]) / 2.0)

            # draw name and value
            if name_draw:
                draw.text(
                    (name_x, name_y), name_draw, font=stat_label_font, fill="white"
                )
            draw.text(
                (val_x, val_y), val_txt, font=stat_value_font, fill=(200, 200, 200)
            )

            # draw a small downward chevron between this row and the next (if not last row)
            if i < n - 1:
                center_x = chevron_center_x_primary
                mid_y = int(row_top + row_h - max(6, int(row_h * 0.18)))
                csize = max(4, int(row_h * 0.12))
                tri = [
                    (center_x - csize, mid_y - csize),
                    (center_x + csize, mid_y - csize),
                    (center_x, mid_y + csize),
                ]
                draw.polygon(tri, fill=(200, 200, 200))

    # ---------- draw TERTIARY stat panel (right) filled ----------
    draw_panel(
        draw,
        [
            (x_stat_tertiary, panel_y_offset),
            (x_stat_tertiary + stat_panel_w, panel_y_offset + panel_height),
        ],
        radius=corner_radius,
        fill=(0, 0, 0, 200),
    )

    draw.text(
        (x_stat_tertiary + stat_panel_w // 2, panel_y_text_off),
        "Tertiary Priority",
        anchor="mm",
        font=panel_title_font,
        fill=class_color,
        stroke_width=2,
        stroke_fill=(0, 0, 0),
    )

    content_x = x_stat_tertiary + inset
    content_y = panel_y_offset + inset
    content_w = stat_panel_w - 2 * inset
    content_h = panel_height - 2 * inset

    # fixed horizontal center for chevrons (tertiary)
    chevron_center_x_tertiary = content_x + content_w // 2

    # ---------- TERTIARY stats: vertical stacked rows, centered vertically ----------
    m = len(tert_list)
    if m == 0:
        draw.text(
            (x_stat_tertiary + stat_panel_w // 2, content_y + content_h // 2),
            "No data",
            font=stat_label_font,
            anchor="mm",
            fill=(160, 160, 160),
        )
    else:
        padding = 8
        icon_sz2 = min(34, int(content_h * 0.16))
        row_h2 = max(icon_sz2, stat_label_font.size + stat_value_font.size) + 8
        total_h2 = m * row_h2 - 8
        start_y2 = content_y + max(0, (content_h - total_h2) // 2)

        for i, s in enumerate(tert_list):
            row_top = int(start_y2 + i * row_h2)
            # icon
            ix = content_x + padding
            iy = row_top + (row_h2 - icon_sz2) // 2
            stat_name_raw = (s.get("name") or "").lower().replace(" ", "_")
            icon_file = os.path.join(config.ICON_DIR, "stats", f"{stat_name_raw}.png")
            if not os.path.exists(icon_file):
                icon_file = os.path.join(config.ICON_DIR, f"{stat_name_raw}.png")
            if os.path.exists(icon_file):
                try:
                    ic = (
                        Image.open(icon_file)
                        .convert("RGBA")
                        .resize((icon_sz2, icon_sz2), LANCZOS)
                    )
                    canvas.paste(ic, (ix, iy), ic)
                except Exception:
                    draw.rectangle(
                        (ix, iy, ix + icon_sz2, iy + icon_sz2), fill=(100, 100, 100)
                    )
                    draw.text(
                        (ix + icon_sz2 // 2, iy + icon_sz2 // 2),
                        (s.get("name", "")[:1] or "?"),
                        font=stat_label_font,
                        anchor="mm",
                        fill="white",
                    )
            else:
                draw.rectangle(
                    (ix, iy, ix + icon_sz2, iy + icon_sz2), fill=(100, 100, 100)
                )
                draw.text(
                    (ix + icon_sz2 // 2, iy + icon_sz2 // 2),
                    (s.get("name", "")[:1] or "?"),
                    font=stat_label_font,
                    anchor="mm",
                    fill="white",
                )

            # name and value
            name_x = ix + icon_sz2 + 8

            if s.get("avg_percent") is not None:
                try:
                    val_txt = f"{float(s['avg_percent']):.2f}%"
                except Exception:
                    val_txt = "-"
            else:
                try:
                    val_txt = f"{float(s.get('avg_raw', 0)):.0f}"
                except Exception:
                    val_txt = "-"

            val_bbox = draw.textbbox((0, 0), val_txt, font=stat_value_font)
            val_w = val_bbox[2] - val_bbox[0]
            val_x = content_x + content_w - padding - val_w

            # truncate name to avoid collision with value
            max_name_w = val_x - 6 - name_x
            name_text_row = (s.get("name") or "").capitalize()
            if max_name_w <= 0:
                name_draw = ""
            else:
                name_draw = name_text_row
                nbbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
                while nbbox[2] - nbbox[0] > max_name_w and len(name_draw) > 1:
                    name_draw = name_draw[:-1]
                    nbbox = draw.textbbox((0, 0), name_draw + "…", font=stat_label_font)
                if nbbox[2] - nbbox[0] > max_name_w:
                    name_draw = ""
                elif name_draw != name_text_row:
                    name_draw = name_draw + "…"

            # compute precise bboxes for vertical centering
            if name_draw:
                name_bbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
            else:
                name_bbox = (0, 0, 0, 0)
            val_bbox = draw.textbbox((0, 0), val_txt, font=stat_value_font)

            # icon center
            icon_center = iy + icon_sz2 / 2.0

            name_y = int(icon_center - (name_bbox[1] + name_bbox[3]) / 2.0)
            val_y = int(icon_center - (val_bbox[1] + val_bbox[3]) / 2.0)

            if name_draw:
                draw.text(
                    (name_x, name_y), name_draw, font=stat_label_font, fill="white"
                )
            draw.text(
                (val_x, val_y), val_txt, font=stat_value_font, fill=(200, 200, 200)
            )

            # draw downward chevron between rows (fixed horizontal position)
            if i < m - 1:
                center_x = chevron_center_x_tertiary
                mid_y = int(row_top + row_h2 - max(6, int(row_h2 * 0.18)))
                csize = max(4, int(row_h2 * 0.12))
                tri = [
                    (center_x - csize, mid_y - csize),
                    (center_x + csize, mid_y - csize),
                    (center_x, mid_y + csize),
                ]
                draw.polygon(tri, fill=(200, 200, 200))

    # --- Panel: top 2 and worst 2 talents by pick-rate ---
    # load talents and compute pick-rates
    talents = {}
    # combine class_talents and spec_talents
    for section, data in (
        ("class_talents", class_talent_overview),
        ("spec_talents", spec_talent_overview),
    ):
        talents[section] = []
        max_count = max((t["count"] for t in data), default=0)
        baseline = max_count if max_count > 0 else 1
        for t in data:
            # each t has 'talent_id' and 'count'
            pick_rate = t["count"] / baseline * 100
            # look up icon & name via talent_lookup
            tl = talent_lookup.get("talents", {}).get(str(t["talent_id"]), {})
            if not tl:
                continue
            talents[section].append(
                {
                    "id": t["talent_id"],
                    "count": t["count"],
                    "pick_rate": pick_rate,
                    "icon": tl.get("icon"),
                    "name": tl.get("name", f"Talent {t['talent_id']}"),
                }
            )
    # sort by pick_rate
    class_talents_sorted = sorted(
        talents["class_talents"], key=lambda x: x["pick_rate"]
    )
    class_worst2 = class_talents_sorted[:2]
    class_best2 = class_talents_sorted[-2:]

    spec_talents_sorted = sorted(talents["spec_talents"], key=lambda x: x["pick_rate"])
    spec_worst2 = spec_talents_sorted[:2]
    spec_best2 = spec_talents_sorted[-2:]

    # layout parameters
    panel_y = 150  # top of both panels
    icon_sz = 24
    v_spacing = 10  # pixels between rows
    text_offset = 5  # pixels between icon & text
    extra_gap = 20  # extra space between best & worst blocks
    pad = 10  # padding inside rounded rect
    corner_radius = 8

    # compute number of icon rows and panel heights
    n_rows = len(class_best2) + len(class_worst2)

    # create a draw handle
    draw = ImageDraw.Draw(canvas, "RGBA")

    enchant_lookup_all = load_json(os.path.join(LOOKUP_DIR, "enchantments.json"))
    crafting_all = load_json(os.path.join(LOOKUP_DIR, "crafting.json"))
    reagent_lookup = {r["id"]: r for r in crafting_all.get("reagents", [])}
    socket_lookup = {
        e["itemId"]: e for e in enchant_lookup_all if e.get("slot") == "socket"
    }

    embellishment_counts = {e[0]: e[1] for e in embellishments}
    missive_counts = {e[0]: e[1] for e in missives}
    socket_counts = {s["id"]: s["count"] for s in sockets}

    missive_best2_raw = sorted(
        missive_counts.items(), key=lambda x: x[1], reverse=True
    )[:2]
    missive_best2 = []
    for m2, count in missive_best2_raw[:2]:
        missive_best2.append(
            {
                "name": reagent_lookup[m2]["name"].rsplit(" ", 1)[-1],
                "icon": reagent_lookup[m2]["icon"],
                "count": count,
                "pick_rate": count / total_missive_count * 100,
            }
        )
    missive_worst2_raw = sorted(missive_counts.items(), key=lambda x: x[1])[:2]
    missive_worst2 = []
    for m2, count in missive_worst2_raw[:2]:
        missive_worst2.append(
            {
                "name": reagent_lookup[m2]["name"].rsplit(" ", 1)[-1],
                "icon": reagent_lookup[m2]["icon"],
                "count": count,
                "pick_rate": count / total_missive_count * 100,
            }
        )

    embell_best2_raw = sorted(
        embellishment_counts.items(), key=lambda x: x[1], reverse=True
    )[:2]
    embell_best2 = []
    for m2, count in embell_best2_raw[:2]:
        embell_best2.append(
            {
                "name": reagent_lookup[m2]["name"],
                "icon": reagent_lookup[m2]["icon"],
                "count": count,
                "pick_rate": count / total_embellishment_count * 100,
            }
        )
    embell_worst2_raw = sorted(embellishment_counts.items(), key=lambda x: x[1])[:2]
    embell_worst2 = []
    for m2, count in embell_worst2_raw[:2]:
        embell_worst2.append(
            {
                "name": reagent_lookup[m2]["name"],
                "icon": reagent_lookup[m2]["icon"],
                "count": count,
                "pick_rate": count / total_embellishment_count * 100,
            }
        )

    socket_best2_raw = sorted(socket_counts.items(), key=lambda x: x[1], reverse=True)[
        :2
    ]
    socket_best2 = []
    for m2, count in socket_best2_raw[:2]:
        socket_best2.append(
            {
                "name": socket_lookup[int(m2)]["itemName"],
                "icon": socket_lookup[int(m2)]["itemIcon"],
                "count": count,
                "pick_rate": count / total_socket_count * 100,
            }
        )
    socket_worst2_raw = sorted(socket_counts.items(), key=lambda x: x[1])[:2]
    socket_worst2 = []
    for m2, count in socket_worst2_raw[:2]:
        socket_worst2.append(
            {
                "name": socket_lookup[int(m2)]["itemName"],
                "icon": socket_lookup[int(m2)]["itemIcon"],
                "count": count,
                "pick_rate": count / total_socket_count * 100,
            }
        )

    panels = [
        ("Class Talents", class_best2, class_worst2, "lime", "orange"),
        ("Spec Talents", spec_best2, spec_worst2, "lime", "orange"),
        ("Missives", missive_best2, missive_worst2, "lime", "orange"),
        ("Embellishment", embell_best2, embell_worst2, "lime", "orange"),
        ("Gems", socket_best2, socket_worst2, "lime", "orange"),
    ]

    panel_sizes = []
    for label, best, worst, bc, wc in panels:
        w = compute_panel_width(
            draw, [best, worst], font_vsm, icon_sz, text_offset, pad
        )
        n_rows = len(best) + len(worst) + 1  # +1 for the heading
        h = n_rows * (icon_sz + v_spacing) - v_spacing + extra_gap + 2 * pad
        panel_sizes.append((w, h))

    num_panels = len(panel_sizes)
    total_panels_w = sum(w for w, h in panel_sizes)
    # compute equal margins on left, right, and between panels
    margin = (WIDTH - total_panels_w) / (num_panels + 1)
    # start x at the left margin
    x = round(margin)

    for (label, best, worst, bc, wc), (pw, ph) in zip(panels, panel_sizes):
        # background
        draw.rounded_rectangle(
            [(x, panel_y), (x + pw, panel_y + ph)],
            radius=corner_radius,
            fill=(0, 0, 0, 200),
        )
        # draw the icon+text blocks
        y = panel_y + pad
        # optional heading
        draw.text((x + pw / 2, y), label, font=font_sm, fill="white", anchor="ma")
        y += icon_sz + v_spacing

        # best block
        for t in best:
            img = (
                Image.open(os.path.join(config.ICON_DIR, f"{t['icon']}.png"))
                .convert("RGBA")
                .resize((icon_sz, icon_sz), LANCZOS)
            )
            canvas.paste(img, (x + pad, y), img)
            name = t["name"] if len(t["name"]) < 40 else t["name"][:17] + "..."
            block_txt = (
                f"{name} [{t['pick_rate']:.1f}%]"
                if t["pick_rate"] < 100
                else f"{name} [100%]"
            )
            draw.text(
                (x + pad + icon_sz + text_offset, y + icon_sz // 2),
                block_txt,
                font=font_vsm,
                fill=bc,
                anchor="lm",
            )
            y += icon_sz + v_spacing

        # gap
        y += extra_gap

        # worst block
        for t in worst:
            img = (
                Image.open(os.path.join(config.ICON_DIR, f"{t['icon']}.png"))
                .convert("RGBA")
                .resize((icon_sz, icon_sz), LANCZOS)
            )
            canvas.paste(img, (x + pad, y), img)
            name = t["name"] if len(t["name"]) < 40 else t["name"][:17] + "..."
            block_txt = (
                f"{name} [{t['pick_rate']:.1f}%]"
                if t["pick_rate"] < 100
                else f"{name} [100%]"
            )
            draw.text(
                (x + pad + icon_sz + text_offset, y + icon_sz // 2),
                block_txt,
                font=font_vsm,
                fill=wc,
                anchor="lm",
            )
            y += icon_sz + v_spacing

        x += round(pw + margin)

    # footer
    upd = datetime.now().timestamp()
    draw.text(
        (0, 0), f"Updated: {format_timestamp(upd * 1000)}", font=font_sm, fill="gray"
    )

    os.makedirs(tmpdir, exist_ok=True)
    canvas = apply_watermark_to_canvas(canvas, position="top_center", padding_x=30, padding_y=2)

    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
    canvas.save(out_path)

    # --- hero tree spread: leader, its share, and the runner-up ---
    sorted_trees = sorted(hero_trees, key=lambda ht: ht["count"], reverse=True)
    top_hero_tree = ""
    top_hero_name = ""
    top_hero_pct = ""
    runner_up_hero = ""
    if sorted_trees and hero_total:
        top = sorted_trees[0]
        top_hero_name = talent_lookup["subTrees"][str(top["tree_id"])]["name"]
        top_hero_pct = f"{round(top['count'] / hero_total * 100)}%"
        top_hero_tree = (
            f"{top_hero_name} ({round(top['count'] / hero_total * 100, 2)}%)"
        )
        if len(sorted_trees) > 1:
            ru = sorted_trees[1]
            ru_name = talent_lookup["subTrees"][str(ru["tree_id"])]["name"]
            runner_up_hero = f"{ru_name} ({round(ru['count'] / hero_total * 100)}%)"

    # --- how the keys land: timed vs three-chested share ---
    timed_runs = upgrade_counts["1"] + upgrade_counts["2"] + upgrade_counts["3"]
    timed_pct = f"{round(timed_runs / total_up * 100)}%"
    three_chest_pct = f"{round(upgrade_counts['3'] / total_up * 100)}%"

    # --- secondary stat priority (index 0 is the primary stat, so skip it) ---
    _stat_src = (
        stat_priority[1:5]
        if stat_priority and len(stat_priority) > 1
        else (stat_priority[:4] if stat_priority else [])
    )
    stat_priority_str = " > ".join(
        (s.get("name") or "").replace("_", " ").strip().title()
        for s in _stat_src
        if (s.get("name") or "").strip()
    )

    post_data = {
        "spec": f"{spec_meta.get('name', '')} {class_meta.get('name', '')}",
        "amount_data_source_runs": humanize_number(play_count),
        "highest_run": f"+{highest_run['level']} {highest_run['dungeon_name']} in {highest_run['duration_str']}",
        "top_hero_tree": top_hero_tree,
        "top_hero_tree_name": top_hero_name,
        "top_hero_tree_pct": top_hero_pct,
        "runner_up_hero_tree": runner_up_hero,
        "hero_tree_count": len(hero_trees),
        "timed_pct": timed_pct,
        "three_chest_pct": three_chest_pct,
        "stat_priority": stat_priority_str,
    }
    return {"out_path": out_path, "post_data": post_data}
