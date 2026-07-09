"""Preview (og:image) generator for the Sim DPS tierlist page.

Renders a static 1200x630 link-unfurl thumbnail from a tierlist tab's DPS rows.

Kept separate from generateSocialsPost.py on purpose: that module initialises a
DB connection pool and imports openai/matplotlib/pandas at import time, whereas
the tierlist page is built in a decoupled, DB-free CI job that only has jinja2 +
Pillow available. So this module depends on Pillow alone (imported lazily inside
the render function) and never touches the social-media posting pipeline — the
image is purely for SEO / link unfurls.
"""

import os
import sys

# og:image preview paths.
PREVIEW_REL_PATH = os.path.join("assets", "img", "previews", "sim_dps_tierlist.png")
PREVIEW_URL = "https://mythistone.com/assets/img/previews/sim_dps_tierlist.png"
PREVIEW_TARGETS = 5  # target-count tab to snapshot for the preview (falls back)

FONT_FILE = os.path.join("assets", "fonts", "BebasNeue-Regular.ttf")
LOGO_FILE = os.path.join("assets", "img", "favicon", "web-app-manifest-192x192.png")
ICON_DIR = os.path.join("data", "icons")

# Tier badge colours (outline, text) mirroring simc_tierlist.html's CSS.
TIER_COLORS = {
    "S": ("#ff7c0a", "#ff9d47"),
    "A": ("#a335ee", "#c77dff"),
    "B": ("#0070dd", "#4da3ff"),
    "C": ("#1eff00", "#52d769"),
    "D": ("#9d9d9d", "#b8b8b8"),
    "F": ("#ff4141", "#ff7b7b"),
}


def _hex_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _blend(fg, bg, a):
    return tuple(int(fg[i] * a + bg[i] * (1 - a)) for i in range(3))


def generate_preview_image(rows, spec_lookup, class_lookup, season_name, targets):
    """Render the 1200x630 og:image preview from one tab's DPS rows.

    Self-contained (Pillow only, no DB) and best-effort: any failure — missing
    Pillow, font or icons — is logged and the build continues without a preview,
    so a thumbnail problem never blocks the page. Returns True on success.

    This produces a static SEO/link-unfurl image only; it deliberately does not
    touch the social-media posting pipeline in generateSocialsPost.py.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        print(f"WARN: Pillow unavailable, skipping tierlist preview: {e}", file=sys.stderr)
        return False

    rows = [r for r in rows if r.get("primary")][:8]
    if not rows:
        print("WARN: no DPS rows for tierlist preview; skipping", file=sys.stderr)
        return False

    try:
        W, H = 1200, 630
        BG = (17, 21, 30)
        MUTED = (150, 158, 172)
        WHITE = (233, 236, 242)
        TRACK = (40, 46, 60)
        margin = 56

        font_cache = {}

        def font(size):
            if size not in font_cache:
                try:
                    font_cache[size] = ImageFont.truetype(FONT_FILE, size)
                except Exception:
                    font_cache[size] = ImageFont.load_default()
            return font_cache[size]

        def fit_font(text, max_w, size, floor=16):
            while size > floor and draw.textlength(text, font=font(size)) > max_w:
                size -= 1
            return font(size)

        img = Image.new("RGB", (W, H), BG)
        draw = ImageDraw.Draw(img)

        # header
        draw.text((margin, 38), "Mythic+ Sim DPS Tierlist", font=font(66), fill=WHITE)
        subtitle = f"{season_name}  •  {targets} target" + ("s" if targets != 1 else "") + "  •  SimulationCraft"
        draw.text((margin, 116), subtitle.upper(), font=font(29), fill=MUTED)
        draw.line([(margin, 168), (W - margin, 168)], fill=(48, 55, 70), width=2)

        leader = rows[0]["primary"] or 1
        top, floor_y = 186, H - 74
        rh = (floor_y - top) / len(rows)
        icon_sz = int(min(38, rh - 12))
        bar_x0, bar_x1, bar_h = 560, W - margin - 130, 15
        name_max_w = bar_x0 - (margin + 118 + icon_sz + 16) - 12

        for i, row in enumerate(rows):
            cy = top + rh * i + rh / 2

            sdata = spec_lookup.get(str(row["spec_id"]), {})
            cdata = class_lookup.get(str(sdata.get("classID", "")), {})
            col = cdata.get("color", {})
            try:
                cc = (int(col["r"]), int(col["g"]), int(col["b"]))
            except Exception:
                cc = (200, 200, 200)

            # rank
            draw.text((margin + 12, cy), str(row["rank"]), font=font(28), fill=MUTED, anchor="mm")

            # tier badge
            outline, txt = TIER_COLORS.get(row["tier"], ("#9d9d9d", "#b8b8b8"))
            bx0, bx1 = margin + 40, margin + 96
            draw.rounded_rectangle(
                [bx0, cy - 17, bx1, cy + 17], radius=7,
                fill=_blend(_hex_rgb(outline), BG, 0.22), outline=_hex_rgb(outline), width=2,
            )
            draw.text(((bx0 + bx1) / 2, cy), row["tier"], font=font(26), fill=_hex_rgb(txt), anchor="mm")

            # spec icon
            ix = margin + 118
            if row.get("icon"):
                icon_path = os.path.join(ICON_DIR, f"{row['icon']}.jpg")
                try:
                    ic = Image.open(icon_path).convert("RGB").resize((icon_sz, icon_sz))
                    img.paste(ic, (int(ix), int(cy - icon_sz / 2)))
                except Exception:
                    pass

            # spec + class name in class colour
            name = f"{row['name']} {row['class_name']}"
            nx = ix + icon_sz + 16
            draw.text((nx, cy), name, font=fit_font(name, name_max_w, 28), fill=cc, anchor="lm")

            # dps bar relative to the leader
            draw.rounded_rectangle([bar_x0, cy - bar_h / 2, bar_x1, cy + bar_h / 2], radius=bar_h / 2, fill=TRACK)
            frac = max(0.0, min(1.0, row["primary"] / leader))
            fill_w = (bar_x1 - bar_x0) * frac
            if fill_w >= bar_h:
                draw.rounded_rectangle([bar_x0, cy - bar_h / 2, bar_x0 + fill_w, cy + bar_h / 2], radius=bar_h / 2, fill=cc)

            # dps value
            draw.text((W - margin, cy), f"{row['primary']:,.0f}", font=font(23), fill=WHITE, anchor="rm")

        # footer: brand + logo, bottom-right
        fy = H - 42
        brand = "mythistone.com"
        draw.text((W - margin - 40, fy), brand, font=font(26), fill=MUTED, anchor="rm")
        try:
            logo = Image.open(LOGO_FILE).convert("RGBA").resize((32, 32))
            img.paste(logo, (W - margin - 32, int(fy - 16)), logo)
        except Exception:
            pass

        os.makedirs(os.path.dirname(PREVIEW_REL_PATH), exist_ok=True)
        img.save(PREVIEW_REL_PATH, "PNG")
        print(f"Generated {PREVIEW_REL_PATH}")
        return True
    except Exception as e:
        print(f"WARN: failed to render tierlist preview image: {e}", file=sys.stderr)
        return False
