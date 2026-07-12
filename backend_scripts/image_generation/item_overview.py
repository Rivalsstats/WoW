"""Renderer for the per-item social/OG preview card (1200x675) used as the
og:image on every item page.

Unlike the spec/dungeon renderers this one takes **no DB handle**: it is fed a
single fully-assembled ``payload`` dict (the same structure
``generateItemPages`` embeds into each item page) and draws entirely from that
plus the DB-free static JSON lookups. That makes it safe to run inside a
``multiprocessing.Pool`` where workers must never touch the database.

Entrypoints:
    render_item_card(payload, slug, out_path)   # standalone (lazy-inits lookups)
    init_worker()                               # Pool initializer
    _render_task((payload, slug, out_path))     # picklable Pool task
"""

import os
import random

from PIL import Image, ImageDraw, ImageFont

from commonUtils import get_class_lookup, get_spec_lookup, humanize_number
from image_generation import config
from image_generation.pil_helpers import (
    LANCZOS,
    apply_watermark_to_canvas,
    cover_crop,
    draw_panel,
    fit_font_to_width,
)

# WoW item-quality -> name colour (poor/common/uncommon/rare/epic/legendary/artifact).
QUALITY_COLORS = {
    0: (157, 157, 157),
    1: (255, 255, 255),
    2: (30, 255, 0),
    3: (0, 112, 221),
    4: (163, 53, 238),
    5: (255, 128, 0),
    6: (229, 204, 128),
    7: (0, 204, 255),
}

# --- worker-local caches (populated lazily / by init_worker) ----------------
_SPEC_LOOKUP = None
_CLASS_LOOKUP = None
_BG_FILES = None
_BG_CACHE = {}


def init_worker():
    """Pool initializer: warm the DB-free lookups once per worker process."""
    global _SPEC_LOOKUP, _CLASS_LOOKUP
    _SPEC_LOOKUP = get_spec_lookup()
    _CLASS_LOOKUP = get_class_lookup()


def _ensure_lookups():
    if _SPEC_LOOKUP is None:
        init_worker()


def _get_background():
    """A dimmed random background canvas. Backgrounds are decoded + cover-cropped
    once per worker and cached, so per-image cost is a cheap copy() rather than a
    ~4.5 MB JPEG decode."""
    global _BG_FILES
    if _BG_FILES is None:
        bg_dir = os.path.join("data", "bg_imgs")
        _BG_FILES = (
            [os.path.join(bg_dir, f) for f in os.listdir(bg_dir)
             if f.lower().endswith((".jpg", ".jpeg", ".png"))]
            if os.path.isdir(bg_dir) else []
        )
    if not _BG_FILES:
        return Image.new("RGB", (config.WIDTH, config.HEIGHT), config.BG)
    path = random.choice(_BG_FILES)
    canvas = _BG_CACHE.get(path)
    if canvas is None:
        img = cover_crop(Image.open(path).convert("RGB"), config.WIDTH, config.HEIGHT)
        canvas = Image.blend(
            Image.new("RGB", (config.WIDTH, config.HEIGHT), config.BG), img, config.BG_ALPHA
        )
        _BG_CACHE[path] = canvas
    return canvas.copy()


def _spec_visual(spec_id):
    """(display name, class-colour rgb, icon path) for a spec id."""
    sm = _SPEC_LOOKUP.get(str(spec_id), {})
    cm = _CLASS_LOOKUP.get(str(sm.get("classID", "")), {})
    col = cm.get("color", {})
    try:
        cc = (int(col["r"]), int(col["g"]), int(col["b"]))
    except (KeyError, TypeError, ValueError):
        cc = (200, 200, 200)
    icon = os.path.join(config.ICON_DIR, f"{sm.get('SpellIconFileId')}.jpg")
    name = f"{sm.get('name', '')} {cm.get('name', '')}".strip() or f"Spec {spec_id}"
    return name, cc, icon


def _variant_label(v):
    """Short human label for an item-level / bonus variant row. Track tags
    ("Mythic"/"Hero"/…) are intentionally dropped: they add no signal and push
    the useful item level off the end of the row."""
    parts = []
    if v.get("ilvl"):
        parts.append(f"{v['ilvl']} ilvl")
    if v.get("sockets"):
        parts.append(f"+{v['sockets']} gem")
    if not parts and v.get("crafted_stats"):
        parts.append("/".join(v["crafted_stats"][:2]))
    return " · ".join(parts) if parts else "Base"


def _paste_icon(canvas, path, box_xy, size):
    """Paste a square icon (RGBA-aware) if the file exists; silent no-op otherwise."""
    if not path or not os.path.exists(path):
        return False
    try:
        icon = Image.open(path).convert("RGBA").resize((size, size), LANCZOS)
        canvas.paste(icon, box_xy, icon)
        return True
    except Exception:
        return False


def _draw_list_panel(canvas, draw, box, title, rows, fonts):
    """Draw a titled card with up to N icon+label+value rows.

    rows: list of {icon, label, value, color, icon_border}. icon may be None.
    """
    font_title, font_row = fonts
    x0, y0, x1, y1 = box
    draw_panel(draw, [(x0, y0), (x1, y1)], radius=12)
    pad = 14
    icon_sz = 30
    draw.text((x0 + pad, y0 + pad), title, font=font_title, fill=config.TEXT)
    ry = y0 + pad + font_title.size + 12
    row_h = icon_sz + 12
    avail = y1 - pad - ry
    max_rows = max(0, int(avail // row_h))
    for row in rows[:max_rows]:
        icon_drawn = False
        text_x = x0 + pad
        if row.get("icon") is not None:
            icon_drawn = _paste_icon(canvas, row["icon"], (x0 + pad, ry), icon_sz)
            if icon_drawn:
                if row.get("icon_border"):
                    draw.rectangle(
                        (x0 + pad, ry, x0 + pad + icon_sz, ry + icon_sz),
                        outline=row["icon_border"], width=2,
                    )
                text_x = x0 + pad + icon_sz + 10
        cy = ry + icon_sz // 2
        # value (right-aligned)
        value = row.get("value")
        val_right = x1 - pad
        if value:
            vb = draw.textbbox((0, 0), value, font=font_row)
            vw = vb[2] - vb[0]
            draw.text((val_right - vw, cy), value, font=font_row,
                      fill=config.MUTED, anchor="lm")
            val_right = val_right - vw - 10
        # label (truncated to fit before the value)
        label = row.get("label", "")
        max_w = val_right - text_x
        while label and draw.textlength(label, font=font_row) > max_w and len(label) > 1:
            label = label[:-1]
        if label != row.get("label", "") and label:
            label = label[:-1] + "…"
        draw.text((text_x, cy), label, font=font_row,
                  fill=row.get("color", config.TEXT), anchor="lm")
        ry += row_h


def render_item_card(payload, slug, out_path):
    """Render one item's preview card to ``out_path`` (``.jpg`` -> JPEG)."""
    _ensure_lookups()
    W, H = config.WIDTH, config.HEIGHT
    g = payload.get("global", {}) or {}

    canvas = _get_background().convert("RGB")
    draw = ImageDraw.Draw(canvas)

    font_title = ImageFont.truetype(config.FONT_FILE, config.TITLE_SIZE)
    font_sub = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
    font_panel_title = ImageFont.truetype(config.FONT_FILE, config.SUBTITLE_SIZE)
    font_row = ImageFont.truetype(config.FONT_FILE, max(18, config.SMALL_SIZE))

    margin = 40
    # ---- header: item icon + name, then subtitle + runs strip stacked
    # vertically beneath the name so nothing overlaps regardless of title size ----
    icon_sz = 96
    icon_top = 30
    icon_path = os.path.join(config.ICON_DIR, f"{payload.get('icon', '')}.png")
    name_x = margin
    if _paste_icon(canvas, icon_path, (margin, icon_top), icon_sz):
        name_x = margin + icon_sz + 20
    q_color = QUALITY_COLORS.get(int(payload.get("quality", 0) or 0), config.TEXT)
    name = payload.get("name", f"Item {payload.get('id', '')}")
    max_title_w = W - name_x - margin
    title_font = fit_font_to_width(draw, name, max_title_w,
                                   start_size=config.TITLE_SIZE, min_size=20, step=2)
    name_y = 32
    draw.text((name_x, name_y), name, font=title_font, fill=q_color)

    sub_bits = [payload.get("slot") or ""]
    if payload.get("ilvl"):
        sub_bits.append(f"ilvl {payload['ilvl']}")
    sub_bits.append(payload.get("weaponType") or "")
    subtitle = "  •  ".join(b for b in sub_bits if b)
    sub_y = name_y + title_font.size + 4
    draw.text((name_x, sub_y), subtitle.upper(), font=font_sub, fill=config.MUTED)

    total_runs = g.get("total_runs", 0)
    strip = f"{humanize_number(total_runs)} RUNS".strip()
    if g.get("adoption") is not None:
        strip += f"   •   {g['adoption']}% OF RUNS"
    strip_y = sub_y + font_sub.size + 10
    draw.text((name_x, strip_y), strip, font=font_sub, fill=config.TEXT)

    # divider clears both the stacked header text and the item icon
    divider_y = max(strip_y + font_sub.size + 14, icon_top + icon_sz + 14)
    draw.line([(margin, divider_y), (W - margin, divider_y)], fill=config.DIVIDER, width=2)

    # ---- build the candidate panels (only those with data) ----
    panels = []  # (title, rows)

    specs = payload.get("spec_overview") or g.get("specs") or []
    spec_rows = []
    for s in specs[:6]:
        nm, cc, icon = _spec_visual(s["spec_id"])
        if s.get("adoption") is not None:
            val = f"{s['adoption']}%"
        elif s.get("share_pct"):
            val = f"{s['share_pct']}%"
        else:
            val = ""
        spec_rows.append({"icon": icon, "icon_border": cc, "label": nm,
                          "value": val, "color": cc})
    if spec_rows:
        panels.append(("Top Specs", spec_rows))

    variants = g.get("variants") or []
    var_rows = []
    for v in variants[:6]:
        var_rows.append({"icon": None, "label": _variant_label(v),
                         "value": f"{v['pct']}%" if v.get("pct") is not None else ""})
    # Only worth a panel when there's real item-level/track signal (not a lone "Base").
    if var_rows and any(v.get("ilvl") or v.get("sockets") or v.get("crafted_stats")
                        for v in variants[:6]):
        panels.append(("Item Level", var_rows))

    gems = g.get("gems") or []
    gem_rows = []
    for gm in gems[:6]:
        icon = os.path.join(config.ICON_DIR, f"{gm['icon']}.png") if gm.get("icon") else None
        gem_rows.append({
            "icon": icon,
            "label": gm.get("name", "Gem"),
            "value": f"{gm['pct']}%" if gm.get("pct") is not None else "",
            "color": QUALITY_COLORS.get(int(gm.get("quality", 0) or 0), config.TEXT),
        })
    if gem_rows:
        panels.append(("Popular Gems", gem_rows))

    enchants = g.get("enchants") or []
    ench_rows = []
    for e in enchants[:6]:
        icon = os.path.join(config.ICON_DIR, f"{e['icon']}.png") if e.get("icon") else None
        ench_rows.append({"icon": icon, "label": e.get("name", "Enchant"),
                          "value": f"{e['pct']}%" if e.get("pct") is not None else ""})
    if ench_rows:
        panels.append(("Enchants", ench_rows))

    panels = panels[:4]

    # ---- lay the panels out in a single row across the lower area ----
    if panels:
        top = divider_y + 18
        bottom = H - 56  # leave room so the cards clear the bottom-right watermark
        gap = 20
        n = len(panels)
        panel_w = (W - 2 * margin - (n - 1) * gap) / n
        x = margin
        for title, rows in panels:
            _draw_list_panel(
                canvas, draw,
                (int(x), top, int(x + panel_w), bottom),
                title, rows, (font_panel_title, font_row),
            )
            x += panel_w + gap

    canvas = apply_watermark_to_canvas(canvas, position="bottom_right",
                                       padding_x=30, padding_y=10)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        canvas = canvas.convert("RGB")
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        canvas.save(out_path, quality=82, optimize=True)
    else:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        canvas.save(out_path)
    return out_path


def _render_task(args):
    """Picklable Pool task: returns (slug, error_or_None)."""
    payload, slug, out_path = args
    try:
        render_item_card(payload, slug, out_path)
        return (slug, None)
    except Exception as e:  # keep one bad item from killing the whole batch
        return (slug, repr(e))
