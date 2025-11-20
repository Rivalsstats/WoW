from contextlib import closing
import os
import argparse
import re
import random
from openai import OpenAI
import openai
import json
import time
from datetime import datetime, timedelta, timezone
from PIL import Image, ImageDraw, ImageFont
import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.patheffects as path_effects
from matplotlib import font_manager, rcParams
import pandas as pd
import matplotlib.image as mpimg
import numpy as np
import databaseConnector
import aggregateData
from collections import defaultdict

from generateSpecPages import (
    LOOKUP_DIR,
    upgrade_info,
    format_duration,
    load_json,
    humanize_number,
    fetch_stat_info,
    MULTI_SLOT_GROUPS,
)

from generateDashboardPage import (
    compute_shades,
    create_dungeon_ease,
    create_spec_scatter,
    RARITY_COLORS,
)

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    2,
)

ICON_DIR = os.path.join("data", "icons")
OUTPUT_DIR = os.path.join("data", "social")
os.makedirs(OUTPUT_DIR, exist_ok=True)
SOCIALS_FILE = os.path.join("data", "socials.json")
POST_FILE = os.path.join(OUTPUT_DIR, "post.json")
FONT_DIR = os.path.join("assets", "fonts")
FONT_FILE = os.path.join(FONT_DIR, "BebasNeue-Regular.ttf")
font_manager.fontManager.addfont(FONT_FILE)
# get the font’s internal name and set as default
FONT_NAME = font_manager.FontProperties(fname=FONT_FILE).get_name()
CUSTOM_FONT = font_manager.FontProperties(fname=FONT_FILE).get_name()
rcParams["font.family"] = CUSTOM_FONT
rcParams["font.sans-serif"] = [CUSTOM_FONT]

WIDTH, HEIGHT = 1200, 675
DPI = 100
TITLE_PCT = 0.12  # title = 12% of canvas height
SUBTITLE_PCT = 0.055  # subtitle = 5.5%
SMALL_PCT = 0.035  # small = 3.5%
VERY_SMALL_PCT = 0.02  # very small = 2%
TITLE_SIZE = int(HEIGHT * TITLE_PCT)
SUBTITLE_SIZE = int(HEIGHT * SUBTITLE_PCT)
SMALL_SIZE = int(HEIGHT * SMALL_PCT)
VERY_SMALL_SIZE = int(HEIGHT * VERY_SMALL_PCT)

rcParams.update(
    {
        "axes.titlesize": SUBTITLE_SIZE,
        "axes.labelsize": VERY_SMALL_SIZE,
        "xtick.labelsize": VERY_SMALL_SIZE,
        "ytick.labelsize": VERY_SMALL_SIZE,
        "legend.fontsize": VERY_SMALL_SIZE,
    }
)
spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))


def format_timestamp(ms_string):
    ms = int(ms_string)
    dt = datetime.fromtimestamp(ms / 1000.0, timezone.utc)
    return dt.strftime("%b %d, %Y")


def get_openai_client(api_key: str):
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )


def time_ago(ms_timestamp: int) -> str:
    # Convert milliseconds timestamp to a datetime in UTC
    dt = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    delta = now - dt

    seconds = int(delta.total_seconds())
    periods = [
        ("year", 60 * 60 * 24 * 365),
        ("month", 60 * 60 * 24 * 30),
        ("day", 60 * 60 * 24),
        ("hour", 60 * 60),
        ("minute", 60),
        ("second", 1),
    ]

    for name, count in periods:
        value = seconds // count
        if value:
            return f"{value} {name}{'s' if value > 1 else ''} ago"
    return "just now"


PROMPT_TEMPLATE = """
You are a witty social-media manager with a dad-joke sense of humor for a World of Warcraft M+ stats site.
Given these inputs:
{data}

Produce one single social-media post (max 210 characters) that:
- uses no emojis or hidden unicode characters
- uses no em-dashes (—); use simple hyphens (-) if needed
- is funny and engaging, includes a dad joke or pun
- invites people to click or reply
- includes the hashtags #WoW, #MythicPlus, #WorldofWarcraft
- stays under 210 characters total which includes the text and any hashtags
- the character limit is strict so double check it and skip part of the information if needed to fit

Output only the post text (no explanation, Comments or Quotation marks).
"""

MODELS = [
    "deepseek/deepseek-r1:free",
    "deepseek/deepseek-chat-v3.1:free",
    "meta-llama/llama-3.3-8b-instruct:free",
    "openai/gpt-oss-20b:free",
    "mistralai/mistral-small-3.2-24b-instruct:free",
]


def generate_post_text(client, data, url, max_retries=5):
    prompt = PROMPT_TEMPLATE.format(data=data).strip()

    for attempt in range(1, max_retries + 1):
        any_model_succeeded = False
        any_model_rate_limited = False

        for model in MODELS:
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                )

                any_model_succeeded = True
                text = resp.choices[0].message.content.strip()
                cleanText = re.sub(r"^['\"]|['\"]$", "", text)

                total_length = len(cleanText) + len(url)
                if total_length < 250:
                    return f"{cleanText} {url}"
                elif len(cleanText) <= 250:
                    return cleanText

                # if too long -> log and break to retry
                print(
                    f"[Attempt {attempt}] Model {model} returned too-long post (len {len(cleanText)}). Retrying..."
                )
                break

            except openai.RateLimitError as e:
                any_model_rate_limited = True
                print(
                    f"[Attempt {attempt}] Model {model} rate-limited: {e}. Trying next model..."
                )
                continue

            except Exception as e:
                # other errors: log and try next model
                print(
                    f"[Attempt {attempt}] Model {model} failed: {e}. Trying next model..."
                )
                continue

        # If we never got to try any model successfully and all were rate-limited, bail early
        if not any_model_succeeded and any_model_rate_limited:
            raise RuntimeError("All models are rate-limited upstream")

        time.sleep(0.5)  # small backoff and retry

    raise RuntimeError(
        f"Failed to generate a post in {max_retries} attempts (too long / errors / rate limits)."
    )


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
    or the TTF can’t be loaded at all, falls back to the default font.
    """
    # If the TTF is missing entirely, this will immediately return default.
    for size in range(start_size, min_size - 1, -step):
        try:
            # force-load our one-and-only TTF
            font = ImageFont.truetype(FONT_FILE, size)
        except (OSError, IOError):
            # if we can’t load Bebas at this size, skip it
            continue

        # measure and return as soon as it fits
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        if text_w <= max_width:
            return font

    # if we never found a fitting Bebas font, fall back once here
    return ImageFont.load_default()


def create_MplusImage(
    active_run, run, donesocials, check_socials=True, add_region=True, add_season=True
):
    dungeon_id = str(active_run["dungeon_id"])
    dungeon_meta = dungeon_lookup[dungeon_id]
    dungeon_name = dungeon_meta["name"]["en_US"]
    dungeon_icon = os.path.join(ICON_DIR, dungeon_meta["icon"])

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
        OUTPUT_DIR, f"{run}_mplus_{dungeon_id}_{level}_{duration_ms}_{timestamp}.png"
    )
    if check_socials and out_path in donesocials:
        return None

    # load dungeon background (no resizing)
    img = Image.open(dungeon_icon).convert("RGBA")
    # if bg is larger, crop=center; if smaller, tile or fill
    bg_w, bg_h = img.size
    scale = max(WIDTH / bg_w, HEIGHT / bg_h)

    new_w = int(bg_w * scale)
    new_h = int(bg_h * scale)

    # resize with high quality filter
    bg_resized = img.resize((new_w, new_h), Image.LANCZOS)

    # center‑crop back down to exactly width×height
    left = (new_w - WIDTH) // 2
    top = (new_h - HEIGHT) // 2
    bg_crop = bg_resized.crop((left, top, left + WIDTH, top + HEIGHT))

    # dark overlay for contrast
    overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 120))
    canvas = Image.alpha_composite(bg_crop, overlay).convert("RGB")
    draw = ImageDraw.Draw(canvas)

    # --- header: dungeon icon + name ---
    header_text = f"{dungeon_name} {upgrade_info(duration=duration_ms, upgrade_map=dungeon_meta['keystone_upgrades'], keystone_level=level)['text']}"
    max_header_w = WIDTH * 0.8
    title_font = fit_font_to_width(
        draw, header_text, max_header_w, start_size=TITLE_SIZE, min_size=12, step=2
    )
    subtitle_font = ImageFont.truetype(FONT_FILE, SUBTITLE_SIZE)
    draw.text((50, 50), header_text, font=title_font, fill=(255, 255, 255))
    draw.text((50, 130), f"{duration_str}", font=subtitle_font, fill=(200, 200, 200))

    # --- footer: region / season / period ---
    footer_text = ""
    if add_region:
        footer_text = f"{footer_text}Region: {region} "
    if add_season:
        footer_text = f"{footer_text}Season: {season} "
    footer_text = f"{footer_text}Date: {date_str}"
    footer_font = ImageFont.truetype(FONT_FILE, SMALL_SIZE)
    bbox = draw.textbbox((0, 0), footer_text, font=footer_font)
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    draw.text(
        ((WIDTH - w) // 2, HEIGHT - h - 20),
        footer_text,
        font=footer_font,
        fill=(180, 180, 180),
    )

    # --- member row ---
    # Each slot: spec icon, class border, spec name
    slot_w, y = 200, 260
    count = len(members)
    total_span = slot_w * (count - 1) if count > 1 else 0
    first_cx = (WIDTH // 2) - (total_span // 2)
    for idx, member in enumerate(members):
        spec_id = str(member["spec_id"])
        spec = spec_lookup[spec_id]
        class_id = str(spec["classID"])
        class_info = class_lookup[class_id]

        # load spell icon
        spell_icon_file = os.path.join(ICON_DIR, f"{spec['SpellIconFileId']}.jpg")
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
        small_font = ImageFont.truetype(FONT_FILE, SMALL_SIZE)
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
    os.makedirs(OUTPUT_DIR, exist_ok=True)
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
        if run_type == "longest":
            return databaseConnector.fetch_longest_run(conn, cursor, season)
        if run_type == "highest":
            return databaseConnector.fetch_max_key_run(conn, cursor, season)
        if run_type == "shortest":
            return databaseConnector.fetch_shortest_run(conn, cursor, season)
    return {}


def create_MplusRun(run, season, donesocials, api_key, url):
    active_run = get_run_data(run, False, season)
    if not active_run:
        raise ValueError(f"No {run} found")

    # --- pull core fields ---
    mplus_image = create_MplusImage(active_run, run, donesocials, True)

    if not mplus_image:
        print(f"Skipping {run} as it already exists in donesocials.")
        return None

    client = get_openai_client(api_key)

    post_data = {
        "dungeon": mplus_image["dungeon_name"],
        "level": mplus_image["level"],
        "duration": mplus_image["duration_str"],
        "run_happened": time_ago(int(mplus_image["timestamp"])),
        "region": mplus_image["region"],
        "run_type": f"{run} this season",
    }
    print(post_data)
    post = generate_post_text(client, post_data, url)
    return {"out_path": mplus_image["out_path"], "post": post}


tier_colors = {
    "S": "#ff8000",  # Legendary
    "A": "#a335ee",  # Epic
    "B": "#0070dd",  # Rare
    "C": "#1eff00",  # Uncommon
    "F": "#9d9d9d",  # Poor
}


def create_overall_spec_popularity(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    """
    Creates and saves a tierlist of total key counts per spec.
    Uses DB-returned rows from:
      - fetch_spec_upgrades(...) -> list of dicts with 'spec_id','keystone_level','total_runs',...
      - fetch_runs_per_dungeon_per_level(...) -> list of dicts with 'dungeon_id','keystone_level','total_runs',...
    """
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_popularity_tierlist_{week}.png")
    if out_path in donesocials:
        return None

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor, season)
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )

    # --- build spec-level totals from spec_upgrades rows ---
    if not spec_upgrades:
        # nothing to do
        return None

    # aggregate total runs per spec_id
    spec_totals = {}
    for r in spec_upgrades:
        sid = int(r["spec_id"])
        spec_totals[sid] = spec_totals.get(sid, 0) + int(r.get("total_runs", 0))

    df = pd.DataFrame(
        {
            "spec_id": list(spec_totals.keys()),
            "total_keys": list(spec_totals.values()),
        }
    )

    # assign tier bins using quantiles (same labels as before)
    df["tier"] = pd.cut(
        df["total_keys"],
        bins=[
            -1,
            df["total_keys"].quantile(0.2),
            df["total_keys"].quantile(0.4),
            df["total_keys"].quantile(0.6),
            df["total_keys"].quantile(0.8),
            df["total_keys"].max() + 1,
        ],
        labels=["F", "C", "B", "A", "S"],
    )
    tier_order = ["S", "A", "B", "C", "F"]
    df["tier"] = pd.Categorical(df["tier"], categories=tier_order, ordered=True)
    df = df.sort_values(["tier", "total_keys"], ascending=[True, False])

    # --- build dungeon DataFrame from dungeon_runs_per_level (for background icons) ---
    if not dungeon_runs_per_level:
        ddf = pd.DataFrame(columns=["id", "count"])
    else:
        d_groups = {}
        for r in dungeon_runs_per_level:
            did = int(r["dungeon_id"])
            d_groups[did] = d_groups.get(did, 0) + int(r.get("total_runs", 0))
        ddf = (
            pd.DataFrame([{"id": k, "count": v} for k, v in d_groups.items()])
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )

    # pick top dungeons → one per tier (same behavior as before: top N dungeons overall)
    d_by_tier = {}
    for i in range(min(len(tier_order), len(ddf))):
        d_by_tier[tier_order[i]] = str(ddf.loc[i, "id"])

    # prepare plot (rest of plotting code unchanged)
    DPI = 100
    fig, ax = plt.subplots(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    # dark background
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

    y_positions = {t: (len(tier_order) - 1 - i) for i, t in enumerate(tier_order)}
    x_offsets = {t: 0 for t in tier_order}

    # dynamic pixel sizing for icons & borders
    fig_w, fig_h = fig.get_size_inches()
    icon_width_in = icon_size * fig_w
    icon_px = int(icon_width_in * fig.dpi)
    border_width = max(1, int(icon_px * 0.075))
    dpi = fig.dpi
    width_px = int(fig_w * dpi)  # full figure width in px
    # each band spans 0.8 data-units; total data-units = len(tier_order)
    band_frac = 0.8 / len(tier_order)
    height_px = int(fig_h * dpi * band_frac)  # band height in px
    spacing = icon_size + 0.02
    count_by_tier = df["tier"].value_counts().to_dict()
    tier_widths = {t: count_by_tier.get(t, 0) * spacing for t in tier_order}
    max_x = max(tier_widths.values()) if tier_widths else 0

    # draw dungeon‐icon backdrops & tier labels
    for t in tier_order:
        y = y_positions[t]
        # full‐row background from dungeon icon
        if t in d_by_tier:
            did = d_by_tier[t]
            icon_file = dungeon_lookup[did]["icon"]
            bg_img = Image.open(os.path.join(ICON_DIR, icon_file))

            orig_w, orig_h = bg_img.size
            new_h = int(width_px * (orig_h / orig_w))
            # make sure height is at least the band height
            if new_h < height_px:
                new_h = height_px
            bg_resized = bg_img.resize((width_px, new_h), Image.LANCZOS)

            # center‐crop vertically to exactly the band height
            top = (new_h - height_px) // 2
            bg_cropped = bg_resized.crop((0, top, width_px, top + height_px))

            ax.imshow(
                np.asarray(bg_cropped),
                extent=(0, max_x, y - 0.4, y + 0.4),
                aspect="auto",
                alpha=0.5,
                zorder=0,
            )
        # tier label
        ax.text(
            -0.02,
            y,
            f"{t}-Tier",
            va="center",
            ha="right",
            fontsize=SMALL_SIZE,
            fontweight="bold",
            color=tier_colors[t],
            zorder=1,
        )

    # plot each spec icon with padded border
    for _, row in df.iterrows():
        t = row["tier"]
        sid = str(row["spec_id"])
        y = y_positions[t]
        x = x_offsets[t]

        # get class color
        spec = spec_lookup[sid]
        cls_info = class_lookup[str(spec["classID"])]
        color_rgb = (
            int(cls_info["color"]["r"]),
            int(cls_info["color"]["g"]),
            int(cls_info["color"]["b"]),
        )

        # load & resize icon
        icon_path = os.path.join(ICON_DIR, f"{spec['SpellIconFileId']}.jpg")
        icon = Image.open(icon_path).convert("RGBA").resize((icon_px, icon_px))

        # padded canvas for full border
        canvas_s = icon_px + 2 * border_width
        canvas = Image.new("RGBA", (canvas_s, canvas_s), (0, 0, 0, 0))
        canvas.paste(icon, (border_width, border_width))

        # draw border
        draw = ImageDraw.Draw(canvas)
        draw.rectangle(
            [0, 0, canvas_s - 1, canvas_s - 1], outline=color_rgb, width=border_width
        )

        # inset axes (icon_size fraction wide)
        ax_ins = ax.inset_axes(
            [x, y - icon_size / 2, icon_size, icon_size],
            transform=ax.transData,
            zorder=2,
        )
        ax_ins.imshow(canvas)
        ax_ins.axis("off")

        x_offsets[t] += icon_size + 0.02

    # finalize layout
    max_x = max(x_offsets.values()) if x_offsets else 0
    ax.set_xlim(-0.5, max_x + 0.05)
    ax.set_ylim(-0.5, len(tier_order) - 0.5)
    ax.axis("off")
    plt.title(
        "Mythic+ Spec Popularity Tier List",
        color="white",
        fontsize=SUBTITLE_SIZE,
        pad=20,
    )
    plt.tight_layout()

    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    # prepare social post data using aggregated spec totals
    max_row = df.loc[df["total_keys"].idxmax()]
    min_row = df.loc[df["total_keys"].idxmin()]
    post_data = {
        "tierlist_type": "Spec Popularity Overall",
        "most_popular_spec": {
            "name": f"{spec_lookup[str(int(max_row['spec_id']))]['name']} {class_lookup[str(spec_lookup[str(int(max_row['spec_id']))]['classID'])]['name']}",
            "runs": humanize_number(int(max_row["total_keys"])),
        },
        "least_popular_spec": {
            "name": f"{spec_lookup[str(int(min_row['spec_id']))]['name']} {class_lookup[str(spec_lookup[str(int(min_row['spec_id']))]['classID'])]['name']}",
            "runs": humanize_number(int(min_row["total_keys"])),
        },
        "total_runs": humanize_number(
            sum(int(r.get("total_runs", 0)) for r in dungeon_runs_per_level)
        ),
    }
    print(post_data)
    client = get_openai_client(api_key)
    post = generate_post_text(client, post_data, url)
    return {"out_path": out_path, "post": post}


def create_spec_popularity_by_level(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    """
    Creates and saves a stacked horizontal bar chart of total key counts per spec,
    split by upgrade tier (depleted, upgrade_1, upgrade_2, upgrade_3).

    Uses rows returned by fetch_spec_upgrades:
      [{"spec_id","keystone_level","upgrade_3","upgrade_2","upgrade_1","depleted","total_runs"}, ...]
    """
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_distribution_by_level_{week}.png")
    if out_path in donesocials:
        return None

    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor, season)

    # build records from DB rows (one row per spec-level)
    records = []
    for r in spec_upgrades:
        sid = int(r["spec_id"])
        records.append(
            {
                "level": int(r["keystone_level"]),
                "spec_id": sid,
                "spec_name": spec_lookup[str(sid)]["name"],
                "count": int(r.get("total_runs", 0)),
            }
        )

    df = pd.DataFrame.from_records(records)
    if df.empty:
        raise ValueError("No key data found in spec_upgrades")

    # group specs by class for color shades (use spec ids present in df for stability)
    specs_seen = sorted(set(df["spec_id"].tolist()))
    specs_by_class = {}
    for sid in specs_seen:
        cid = spec_lookup[str(sid)]["classID"]
        specs_by_class.setdefault(cid, []).append(sid)

    color_map = {}
    for cid, sids in specs_by_class.items():
        base = class_lookup[str(cid)]["color"]
        rgb_base = (int(base["r"]), int(base["g"]), int(base["b"]))
        shades = compute_shades(rgb_base, len(sids))
        for sid, shade in zip(sorted(sids), shades):
            color_map[sid] = (shade["r"] / 255, shade["g"] / 255, shade["b"] / 255)

    # pivot to level x spec table (counts), then convert to per-level pct
    pivot = df.pivot_table(
        index="level", columns="spec_id", values="count", aggfunc="sum", fill_value=0
    ).sort_index()
    pct = pivot.div(pivot.sum(axis=1).replace(0, 1), axis=0)  # avoid division by zero

    # order specs by classID then spec id (missing classID -> sort last)
    def _class_sort_key(sid):
        cid = spec_lookup.get(str(sid), {}).get("classID")
        return (cid if cid is not None else 10**9, sid)

    ordered = sorted(pivot.columns.tolist(), key=_class_sort_key)
    pct = pct[ordered]

    # plotting params (DPI local fallback)
    DPI = globals().get("DPI", 100)
    fig, ax = plt.subplots(
        figsize=(WIDTH / DPI, HEIGHT / DPI),
        dpi=DPI,
        facecolor=(30 / 255, 30 / 255, 30 / 255),
    )
    ax.set_facecolor((30 / 255, 30 / 255, 30 / 255))

    # build color list for ordered specs (fallback gray if missing)
    colors = [color_map.get(sid, (0.6, 0.6, 0.6)) for sid in ordered]

    pct.plot(
        kind="barh",
        stacked=True,
        width=0.8,
        color=colors,
        legend=False,
        linewidth=0,
        ax=ax,
    )

    ax.set_ylabel("Keystone Level", color="white")
    ax.set_title("Spec Distribution across Keylevels", color="white")
    ax.set_xlim(0, 1)
    ax.set_xticklabels([])

    yticks = ax.get_yticks()
    bar_height = 0.8
    y0 = yticks[0] - bar_height / 2 - 0.05

    # icons along the bottom: evenly spaced across the width
    N = len(ordered)
    x_fracs = [i / (N - 1) if N > 1 else 0.5 for i in range(N)]
    for x_frac, sid in zip(x_fracs, ordered):
        icon_file = os.path.join(
            ICON_DIR, f"{spec_lookup[str(sid)]['SpellIconFileId']}.jpg"
        )
        arr_img = plt.imread(icon_file)
        im = OffsetImage(arr_img, zoom=0.35)
        ab = AnnotationBbox(
            im,
            (x_frac, y0),
            xycoords=("axes fraction", "data"),
            box_alignment=(0.5, 1),
            frameon=False,
        )
        ax.add_artist(ab)

    ax.tick_params(axis="y", colors="white")
    plt.tight_layout()

    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close()

    # prepare social post data: find highest keylevel and top specs at that level
    if spec_upgrades:
        max_keylevel = max(int(r["keystone_level"]) for r in spec_upgrades)
    else:
        max_keylevel = None

    top_specs = []
    if max_keylevel is not None:
        # total runs per spec at the max level
        runs_at_max = {}
        for r in spec_upgrades:
            if int(r["keystone_level"]) == max_keylevel:
                sid = int(r["spec_id"])
                runs_at_max[sid] = runs_at_max.get(sid, 0) + int(r.get("total_runs", 0))
        for sid, cnt in runs_at_max.items():
            top_specs.append(
                {
                    "specName": spec_lookup[str(sid)]["name"],
                    "className": class_lookup[str(spec_lookup[str(sid)]["classID"])][
                        "name"
                    ],
                    "count": cnt,
                }
            )
        top_specs = sorted(top_specs, key=lambda s: s["count"], reverse=True)[:3]

    post_data = {
        "tierlist_type": "Spec Popularity by Keylevel",
        "highest_keylevel": max_keylevel,
        "highest_specs": [
            f"{spec['specName']} - {spec['className']}" for spec in top_specs
        ],
    }
    print(post_data)
    client = get_openai_client(api_key)
    post = generate_post_text(client, post_data, url)
    return {"out_path": out_path, "post": post}


def create_dungeon_popularity_vs_ease(output_dir, donesocials, api_key, url, season):
    """
    Creates and saves a stacked horizontal bar chart showing, for each Mythic+ level,
    the share of total runs completed in each dungeon (i.e. “ease”).
    """
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(
        output_dir, f"dungeon_popularity_across_keylevels_{week}.png"
    )
    if out_path in donesocials:
        return None
    # --- prepare data ---
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_runs_per_level = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )

    ease_data = create_dungeon_ease(dungeon_runs_per_level, dungeon_lookup, None)
    key_levels = ease_data["keyLevels"]
    datasets = ease_data["datasets"]

    # build a DataFrame so we can sort and assign colors
    df = pd.DataFrame(datasets)
    # ensure consistent order
    df = df.set_index("label").loc[[d["label"] for d in datasets]].reset_index()
    dungeon_names = df["label"].tolist()
    pct_matrix = df["data"].tolist()

    # --- plotting ---
    fig, ax = plt.subplots(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

    # stack each dungeon as a segment in each bar
    y_pos = np.arange(len(key_levels))
    left = np.zeros(len(key_levels))

    # pick a colormap
    cmap = plt.get_cmap("tab20")

    for idx, (dungeon, pct_vals) in enumerate(zip(dungeon_names, pct_matrix)):
        color = cmap(idx % cmap.N)
        ax.barh(
            y=y_pos, width=pct_vals, left=left, height=0.8, label=dungeon, color=color
        )
        left += np.array(pct_vals)

    # --- styling ---
    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"M + Level {lvl}" for lvl in key_levels], color="white")
    ax.invert_yaxis()  # highest level at top
    ax.set_xlim(0, 100)
    ax.set_xlabel("Share of Runs (%)", color="white")
    ax.set_title("Dungeon Popularity across Mythic+ Levels", color="white", pad=15)
    ax.set_xticklabels([])

    # legend outside
    ax.legend(
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
        labelcolor="white",
        fontsize=VERY_SMALL_SIZE,
    )

    plt.tight_layout()

    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    # assemble OpenAI post
    client = get_openai_client(api_key)
    post_data = {
        "chart_type": "Dungeon Popularity across Keylevels",
        "levels_covered": len(key_levels),
        "top_dungeon": dungeon_names[0],
        "bottom_dungeon": dungeon_names[-1],
    }
    post = generate_post_text(client, post_data, url)

    return {"out_path": out_path, "post": post}


def parse_color(s):
    vals = s[s.find("(") + 1 : s.find(")")].split(",")
    r, g, b, a = map(float, vals)
    return (r / 255, g / 255, b / 255, a)


def create_spec_popularity_vs_performance(
    output_dir, donesocials, api_key, url, season
):
    """
    Generate and save/show a scatter plot of spec performance vs popularity,
    using spec icons as markers, reusing create_spec_scatter.
    If output_path is provided, saves the figure to that path.
    """
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_popularity_vs_performance_{week}.png")
    if out_path in donesocials:
        return None
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        spec_upgrades = databaseConnector.fetch_spec_upgrades(conn, cursor, season)
        highest_run = databaseConnector.fetch_max_key_run(conn, cursor, season)
    # get point data dicts with x, y, iconUrl, borderColor, backgroundColor
    raw_points = create_spec_scatter(
        spec_upgrades, spec_lookup, class_lookup, highest_run
    )

    # transform raw_points to local representation
    points = []
    for p in raw_points:
        # parse borderColor and backgroundColor strings 'rgba(r,g,b,a)'

        border = parse_color(p["borderColor"])
        face = parse_color(p["backgroundColor"])
        # convert icon URL to local file path (strip leading slash)
        icon_path = p["iconUrl"].lstrip("/")

        points.append(
            {
                "x": p["x"],
                "y": p["y"],
                "icon_path": icon_path,
                "edge_color": border,
                "face_color": face,
            }
        )

    # plotting

    fig, ax = plt.subplots(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    dark = (30 / 255, 30 / 255, 30 / 255)
    ax.set_facecolor(dark)
    fig.patch.set_facecolor(dark)

    # draw each icon marker
    for p in points:
        try:
            img = Image.open(p["icon_path"]).convert("RGBA")
        except Exception:
            continue
        im = OffsetImage(np.array(img), zoom=0.2)
        ab = AnnotationBbox(
            im,
            (p["x"], p["y"]),
            frameon=True,
            bboxprops={
                "edgecolor": p["edge_color"],
                "facecolor": p["face_color"],
                "linewidth": 1.5,
                "boxstyle": "round,pad=0.2",
            },
        )
        ax.add_artist(ab)

    ax.set_xlabel("Performance", color="white")
    ax.set_ylabel("Popularity", color="white")
    ax.set_title("Spec Popularity vs Performance", color="white")
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.grid(True, linestyle="--", alpha=0.3)

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]
    dx = (max(xs) - min(xs)) * 0.05
    dy = (max(ys) - min(ys)) * 0.05
    ax.set_xlim(min(xs) - dx, max(xs) + dx)
    ax.set_ylim(min(ys) - dy, max(ys) + dy)

    ys = np.array([p["y"] for p in raw_points])
    xs = np.array([p["x"] for p in raw_points])

    # fit a straight line: x ≈ m*y + b
    m = (xs @ ys) / (ys @ ys)
    b = 0.0

    # annotate each point with its residual
    for p in raw_points:
        expected = m * p["y"] + b
        p["residual"] = p["x"] - expected

    plt.tight_layout()

    plt.savefig(out_path)

    plt.close(fig)
    client = get_openai_client(api_key)

    most_overperforming = max(raw_points, key=lambda p: p["residual"])
    most_underperforming = min(raw_points, key=lambda p: p["residual"])
    post_data = {
        "chart_type": "Dungeon Popularity across Keylevels",
        "most_overperforming_spec": most_overperforming["label"],
        "most_underperforming_spec": most_underperforming["label"],
    }
    print(post_data)
    post = generate_post_text(client, post_data, url)

    return {"out_path": out_path, "post": post}


def compute_dungeon_score_from_rows(dungeon_rows, w_depleted=-1, w_1=1, w_2=2, w_3=3):
    """
    dungeon_rows: iterable of rows returned by fetch_runs_per_dungeon_per_level
                  but only the rows for a single dungeon_id.
    Each row is a dict with keys:
      'keystone_level', 'upgrade_3', 'upgrade_2', 'upgrade_1', 'depleted', ...
    Returns: float score (same formula as original compute_dungeon_score).
    """
    total = 0.0
    for r in dungeon_rows:
        lvl = int(r["keystone_level"])
        depleted = int(r.get("depleted", 0))
        u1 = int(r.get("upgrade_1", 0))
        u2 = int(r.get("upgrade_2", 0))
        u3 = int(r.get("upgrade_3", 0))

        total += lvl * (w_depleted * depleted + w_1 * u1 + w_2 * u2 + w_3 * u3)
    return total


# === helper: build a DataFrame of dungeon scores and tier labels ===
def build_dungeon_scores_df(db_rows):
    """
    Build the same DataFrame as the previous build_dungeon_scores_df but using
    rows returned by fetch_runs_per_dungeon_per_level (flat rows per level).

    db_rows: list of dicts like returned by fetch_runs_per_dungeon_per_level
    Returns: pandas.DataFrame with columns ['id','count','score','tier']
    """
    if not db_rows:
        return pd.DataFrame(columns=["id", "count", "score", "tier"])

    # group rows by dungeon_id
    groups = defaultdict(list)
    for r in db_rows:
        groups[int(r["dungeon_id"])].append(r)

    rows = []
    for dungeon_id, rows_for_d in groups.items():
        score = compute_dungeon_score_from_rows(rows_for_d)
        # derive 'count' as total_runs summed across levels (similar to previous 'count')
        total_runs = sum(int(r.get("total_runs", 0)) for r in rows_for_d)
        rows.append({"id": dungeon_id, "count": total_runs, "score": score})

    df = pd.DataFrame(rows)

    # assign quantile‐based tiers (fallback to equal-width bins if qcut fails)
    try:
        df["tier"] = pd.qcut(df["score"], q=5, labels=["F", "C", "B", "A", "S"])
    except ValueError:
        # e.g. not enough unique values to form 5 quantiles -> use pd.cut fallback
        df["tier"] = pd.cut(df["score"], bins=5, labels=["F", "C", "B", "A", "S"])

    # order the categories (S highest)
    df["tier"] = pd.Categorical(
        df["tier"], categories=["S", "A", "B", "C", "F"], ordered=True
    )
    df = df.sort_values(["tier", "score"], ascending=[True, False])
    return df


# === main: create the dungeon tierlist image and social post ===
def create_dungeon_tierlist(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    """
    Generates a horizontal tier-list of dungeons, one row per tier,
    placing each dungeon's spell-icon in the tier. Background is the
    dungeon art (faded). Saves to PNG and returns {"out_path","post"}.
    """
    # 1) prepare data
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, season
        )
        total_runs = databaseConnector.fetch_total_season_runs(conn, cursor, season)

    df = build_dungeon_scores_df(dungeon_data)
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"dungeon_tierlist_{week}.png")
    if out_path in donesocials:
        return None

    fig, ax = plt.subplots(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    fig.patch.set_facecolor("#222222")
    ax.set_facecolor("#222222")

    tiers = ["S", "A", "B", "C", "F"]
    y_positions = {t: len(tiers) - 1 - i for i, t in enumerate(tiers)}
    x_offsets = {t: 0 for t in tiers}
    max_x = len(df) * (icon_size + 0.02)  # or simply: max(x_offsets.values())

    # 3) draw backgrounds and tier labels
    for t in tiers:
        y = y_positions[t]
        sub = df[df["tier"] == t]
        if not sub.empty:
            did = str(sub.iloc[0]["id"])
            icon_file = dungeon_lookup[did]["icon"]
            bg = Image.open(os.path.join(ICON_DIR, icon_file))
            # fill full width
            w0, h0 = bg.size
            scale = WIDTH / w0
            bg = bg.resize((WIDTH, int(h0 * scale)), Image.LANCZOS)
            band_h = HEIGHT / len(tiers)
            top = (bg.height - band_h) // 2
            bg = bg.crop((0, top, WIDTH, top + int(band_h)))
            ax.imshow(
                np.asarray(bg),
                extent=(0, max_x, y - 0.4, y + 0.4),
                aspect="auto",
                alpha=0.3,
                zorder=0,
            )
        ax.text(
            -0.05 * max_x,
            y,
            f"{t}-Tier",
            va="center",
            ha="right",
            fontsize=SMALL_SIZE,
            fontweight="bold",
            color=tier_colors[t],
            zorder=1,
        )
    # 4) plot each dungeon icon
    # determine pixel size for icons
    fig_w, fig_h = fig.get_size_inches()
    icon_w_in = icon_size * fig_w
    icon_px = int(icon_w_in * DPI)
    border_w = max(1, icon_px // 20)

    for _, row in df.iterrows():
        t = row["tier"]
        y = y_positions[t]
        x = x_offsets[t]

        icon = Image.open(
            os.path.join(ICON_DIR, dungeon_lookup[str(row["id"])]["icon"])
        )
        icon = icon.resize((icon_px, icon_px))
        canv = Image.new(
            "RGBA", (icon_px + 2 * border_w, icon_px + 2 * border_w), (0, 0, 0, 0)
        )
        canv.paste(icon, (border_w, border_w))
        # inset_axes in FRACTIONAL units: [left, bottom, width, height]
        # left = x / max_x, bottom = (y - icon_size/2) / len(tiers)
        left_frac = x / max_x
        center_frac = (y + 0.5) / len(tiers)
        bottom_frac = center_frac - (icon_size / 2) / len(tiers)
        ax_ins = ax.inset_axes(
            [left_frac, bottom_frac, icon_size / max_x * max_x, icon_size / len(tiers)],
            transform=ax.transAxes,
            zorder=2,
        )
        ax_ins.imshow(canv)
        ax_ins.axis("off")

        dungeon_name = dungeon_lookup[str(row["id"])]["name"]["en_US"]
        # height for label: about 5% of the axes height
        text_h = 0.05
        label_gap = 0.02
        # inset for text: same left_frac, shifted down by text_h
        text_ins = ax.inset_axes(
            [
                left_frac,
                bottom_frac - text_h - label_gap,  # move below the icon
                icon_size,  # same width
                text_h,
            ],  # small height
            transform=ax.transAxes,
            zorder=2,
        )
        text_ins.text(
            0.5,
            1.0,  # x=center, y=top of this box
            dungeon_name,
            va="top",
            ha="center",
            color="white",
            fontsize=VERY_SMALL_SIZE,
            wrap=True,  # auto‑wrap if it’s long
        )
        text_ins.axis("off")

        x_offsets[t] += icon_size + 0.2

    # finalize
    ax.set_xlim(-0.05 * max_x, max_x + 0.02)
    ax.set_ylim(-0.5, len(tiers) - 0.5)
    ax.axis("off")
    plt.title("Dungeon Tier List", color="white", fontsize=SUBTITLE_SIZE, pad=20)
    plt.tight_layout()
    plt.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    # generate the social‐media post text
    client = get_openai_client(api_key)
    best = df.iloc[0]
    worst = df.iloc[-1]
    post_data = {
        "tierlist_type": "Dungeon Tierlist",
        "best_dungeon": dungeon_lookup[str(best["id"])]["name"]["en_US"],
        "worst_dungeon": dungeon_lookup[str(worst["id"])]["name"]["en_US"],
        "total_runs": humanize_number(total_runs),
    }
    print(post_data)
    post = generate_post_text(client, post_data, url)
    return {"out_path": out_path, "post": post}


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


def createSpecOverview(output_dir, donesocials, api_key, url, spec_id, season):
    """
    Creates and saves a spec overview image.
    """
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_overview_{spec_id}_{week}.png")
    if out_path in donesocials:
        return None
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
            conn, cursor, spec_id, season
        )
        play_count = 0
        for u in spec_upgrade_counts:
            upgrade_counts[u["upgrade_tier"]] += int(u["run_count"])
            play_count += int(u["run_count"])

        counts_list = [upgrade_counts[t] for t in tiers]
        total_up = sum(counts_list) or 1

        # hero tree picks
        hero_trees_raw = databaseConnector.fetch_hero_tree_overview(
            conn, cursor, spec_id, season
        )
        hero_trees = []
        for row in hero_trees_raw:
            tree_id = row[0]
            tree_count = row[1]
            hero_trees.append({"tree_id": tree_id, "count": tree_count})

        hero_total = sum(tree["count"] for tree in hero_trees)

        # runs
        highest = get_run_data(False, spec_id, season)
        highest_run = create_MplusImage(
            highest, "highest_run", donesocials, False, False, False
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

    # canvas
    bg_dir = os.path.join("data", "bg_imgs")
    # list all .jpg/.png in that dir
    bg_files = [
        os.path.join(bg_dir, f)
        for f in os.listdir(bg_dir)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    ]
    if not bg_files:
        # fallback to flat color if no images found
        canvas = Image.new("RGB", (WIDTH, HEIGHT), "#222222")
    else:
        bg_path = random.choice(bg_files)
        canvas = Image.open(bg_path).convert("RGB")
        # resize if needed
        if canvas.size != (WIDTH, HEIGHT):
            canvas = canvas.resize((WIDTH, HEIGHT), Image.LANCZOS)

    draw = ImageDraw.Draw(canvas)
    font_big = ImageFont.truetype(FONT_FILE, TITLE_SIZE)
    font_med = ImageFont.truetype(FONT_FILE, SUBTITLE_SIZE)
    font_sm = ImageFont.truetype(FONT_FILE, SMALL_SIZE)
    font_vsm = ImageFont.truetype(FONT_FILE, VERY_SMALL_SIZE)

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
    icon_file = os.path.join(ICON_DIR, f"{spec_meta.get('SpellIconFileId')}.jpg")
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
            fontsize=VERY_SMALL_SIZE,
            fontweight="bold",
        )
        txt.set_path_effects(
            [path_effects.withStroke(linewidth=1.5, foreground="black")]
        )
        left += frac
    ax.axis("off")
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    buf = os.path.join(output_dir, "tmp_upgrade.png")
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
            ICON_DIR, f"{talent_lookup['subTrees'][str(ht['tree_id'])]['icon']}.png"
        )
        pct = ht["count"] / hero_total * 100 if hero_total else 0
        if os.path.exists(tree_icon):
            img = Image.open(tree_icon).convert("RGBA")
            img = img.resize((icon_size, icon_size), Image.LANCZOS)

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
    stat_label_font = ImageFont.truetype(FONT_FILE, max(10, SMALL_SIZE))
    stat_value_font = ImageFont.truetype(FONT_FILE, max(12, SMALL_SIZE + 2))
    panel_title_font = (
        font_med
        if "font_med" in globals()
        else ImageFont.truetype(FONT_FILE, SUBTITLE_SIZE)
    )

    x_image = outer_margin
    x_stat_primary = x_image + image_panel_w + inner_margin
    x_stat_tertiary = x_stat_primary + stat_panel_w + inner_margin

    # ---------- draw image panel (left) with filled background ----------
    try:
        draw.rounded_rectangle(
            [
                (x_image, panel_y_offset),
                (x_image + image_panel_w, panel_y_offset + panel_height),
            ],
            radius=corner_radius,
            fill=(0, 0, 0, 200),
        )
    except Exception:
        draw.rectangle(
            [
                (x_image, panel_y_offset),
                (x_image + image_panel_w, panel_y_offset + panel_height),
            ],
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
            img = img.resize((scaled_w, scaled_h), Image.LANCZOS)
            left = (scaled_w - content_w) // 2
            top = (scaled_h - content_h) // 2
            img = img.crop((left, top, left + content_w, top + content_h))
            mask = Image.new("L", img.size, 0)
            md = ImageDraw.Draw(mask)
            md.rounded_rectangle([(0, 0), img.size], radius=corner_radius, fill=255)
            img.putalpha(mask)
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
    try:
        draw.rounded_rectangle(
            [
                (x_stat_primary, panel_y_offset),
                (x_stat_primary + stat_panel_w, panel_y_offset + panel_height),
            ],
            radius=corner_radius,
            fill=(0, 0, 0, 200),
        )
    except Exception:
        draw.rectangle(
            [
                (x_stat_primary, panel_y_offset),
                (x_stat_primary + stat_panel_w, panel_y_offset + panel_height),
            ],
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
            icon_file = os.path.join(ICON_DIR, "stats", f"{stat_name_raw}.png")
            if not os.path.exists(icon_file):
                icon_file = os.path.join(ICON_DIR, f"{stat_name_raw}.png")
            if os.path.exists(icon_file):
                try:
                    ic = (
                        Image.open(icon_file)
                        .convert("RGBA")
                        .resize((icon_sz, icon_sz), Image.LANCZOS)
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
            name_text = (s.get("name") or "").capitalize()
            if max_name_w <= 0:
                name_draw = ""
            else:
                name_draw = name_text
                nbbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
                # shorten until it fits
                while nbbox[2] - nbbox[0] > max_name_w and len(name_draw) > 1:
                    name_draw = name_draw[:-1]
                    nbbox = draw.textbbox((0, 0), name_draw + "…", font=stat_label_font)
                if nbbox[2] - nbbox[0] > max_name_w:
                    name_draw = ""
                elif name_draw != name_text:
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
    try:
        draw.rounded_rectangle(
            [
                (x_stat_tertiary, panel_y_offset),
                (x_stat_tertiary + stat_panel_w, panel_y_offset + panel_height),
            ],
            radius=corner_radius,
            fill=(0, 0, 0, 200),
        )
    except Exception:
        draw.rectangle(
            [
                (x_stat_tertiary, panel_y_offset),
                (x_stat_tertiary + stat_panel_w, panel_y_offset + panel_height),
            ],
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
            icon_file = os.path.join(ICON_DIR, "stats", f"{stat_name_raw}.png")
            if not os.path.exists(icon_file):
                icon_file = os.path.join(ICON_DIR, f"{stat_name_raw}.png")
            if os.path.exists(icon_file):
                try:
                    ic = (
                        Image.open(icon_file)
                        .convert("RGBA")
                        .resize((icon_sz2, icon_sz2), Image.LANCZOS)
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
            name_text = (s.get("name") or "").capitalize()
            if max_name_w <= 0:
                name_draw = ""
            else:
                name_draw = name_text
                nbbox = draw.textbbox((0, 0), name_draw, font=stat_label_font)
                while nbbox[2] - nbbox[0] > max_name_w and len(name_draw) > 1:
                    name_draw = name_draw[:-1]
                    nbbox = draw.textbbox((0, 0), name_draw + "…", font=stat_label_font)
                if nbbox[2] - nbbox[0] > max_name_w:
                    name_draw = ""
                elif name_draw != name_text:
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
        for t in data:
            # each t has 'talent_id' and 'count'
            pick_rate = t["count"] / play_count * 100
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
    for m, count in missive_best2_raw[:2]:
        missive_best2.append(
            {
                "name": reagent_lookup[m]["name"].rsplit(" ", 1)[-1],
                "icon": reagent_lookup[m]["icon"],
                "count": count,
                "pick_rate": count / total_missive_count * 100,
            }
        )
    missive_worst2_raw = sorted(missive_counts.items(), key=lambda x: x[1])[:2]
    missive_worst2 = []
    for m, count in missive_worst2_raw[:2]:
        missive_worst2.append(
            {
                "name": reagent_lookup[m]["name"].rsplit(" ", 1)[-1],
                "icon": reagent_lookup[m]["icon"],
                "count": count,
                "pick_rate": count / total_missive_count * 100,
            }
        )

    embell_best2_raw = sorted(
        embellishment_counts.items(), key=lambda x: x[1], reverse=True
    )[:2]
    embell_best2 = []
    for m, count in embell_best2_raw[:2]:
        embell_best2.append(
            {
                "name": reagent_lookup[m]["name"],
                "icon": reagent_lookup[m]["icon"],
                "count": count,
                "pick_rate": count / total_embellishment_count * 100,
            }
        )
    embell_worst2_raw = sorted(embellishment_counts.items(), key=lambda x: x[1])[:2]
    embell_worst2 = []
    for m, count in embell_worst2_raw[:2]:
        embell_worst2.append(
            {
                "name": reagent_lookup[m]["name"],
                "icon": reagent_lookup[m]["icon"],
                "count": count,
                "pick_rate": count / total_embellishment_count * 100,
            }
        )

    socket_best2_raw = sorted(socket_counts.items(), key=lambda x: x[1], reverse=True)[
        :2
    ]
    socket_best2 = []
    for m, count in socket_best2_raw[:2]:
        socket_best2.append(
            {
                "name": socket_lookup[int(m)]["itemName"],
                "icon": socket_lookup[int(m)]["itemIcon"],
                "count": count,
                "pick_rate": count / total_socket_count * 100,
            }
        )
    socket_worst2_raw = sorted(socket_counts.items(), key=lambda x: x[1])[:2]
    socket_worst2 = []
    for m, count in socket_worst2_raw[:2]:
        socket_worst2.append(
            {
                "name": socket_lookup[int(m)]["itemName"],
                "icon": socket_lookup[int(m)]["itemIcon"],
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
                Image.open(os.path.join(ICON_DIR, f"{t['icon']}.png"))
                .convert("RGBA")
                .resize((icon_sz, icon_sz), Image.LANCZOS)
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
                Image.open(os.path.join(ICON_DIR, f"{t['icon']}.png"))
                .convert("RGBA")
                .resize((icon_sz, icon_sz), Image.LANCZOS)
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

    os.makedirs(output_dir, exist_ok=True)
    canvas.save(out_path)
    client = get_openai_client(api_key)

    if hero_trees:
        # find the single subtree with the highest count
        top = max(hero_trees, key=lambda ht: ht["count"])
        top_hero_tree = f"{talent_lookup['subTrees'][str(top['tree_id'])]['name']} ({round((top['count'] / sum(ht['count'] for ht in hero_trees)) * 100, 2)}%)"
    else:
        top_hero_tree = ""
    post_data = {
        "spec": f"{spec_meta.get('name', '')} {class_meta.get('name', '')}",
        "amount_data_source_runs": humanize_number(play_count),
        "highest_run": f"+{highest_run['level']} {highest_run['dungeon_name']} Completed in ({highest_run['duration_str']})",
        "top_hero_tree": top_hero_tree,
    }
    print(post_data)
    post = generate_post_text(client, post_data, url)
    return {"out_path": out_path, "post": post}


def create_socials_post(donesocials, api_key, url):
    """
    Randomly selects one of several post-generating routines, skipping any already done.
    Gives each spec overview an equal chance, collectively outweighing other generators.
    """
    print("Generating social media post...")

    # Prepare spec IDs for spec overview
    specs = [f for f in spec_lookup.keys()]

    access_token = aggregateData.get_access_token(
        os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"]
    )
    current_season_id = aggregateData.get_current_season_id(access_token)

    # Create spec-specific generators
    spec_generators = []
    for spec_id in specs or ["62"]:

        def make_spec_gen(sid):
            return lambda: createSpecOverview(
                OUTPUT_DIR, donesocials, api_key, url, sid, current_season_id
            )

        spec_generators.append(make_spec_gen(spec_id))

    # Other generators
    def gen_dungeon_tier():
        return create_dungeon_tierlist(
            OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_spec_pop_vs_perf():
        return create_spec_popularity_vs_performance(
            OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_dungeon_pop_vs_ease():
        return create_dungeon_popularity_vs_ease(
            OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_overall_spec_popularity():
        return create_overall_spec_popularity(
            OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_spec_pop_by_level():
        return create_spec_popularity_by_level(
            OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    run_types = ["highest_run", "longest_run", "shortest_run"]

    def make_run_gen(run_type):
        return lambda: create_MplusRun(
            run_type, current_season_id, donesocials, api_key, url
        )

    other_generators = [
        gen_dungeon_tier,
        gen_spec_pop_vs_perf,
        gen_dungeon_pop_vs_ease,
        gen_overall_spec_popularity,
        gen_spec_pop_by_level,
    ] + [make_run_gen(rt) for rt in run_types]

    # Combine all generators
    generators = spec_generators + other_generators

    # Assign weight: each spec generator weight=1 (total spec weight = len(specs)), others weight=1
    weights = [1] * len(generators)

    # Create a list of available indices
    available = list(range(len(generators)))
    available_weights = weights.copy()

    # Select until a valid, new post is found or exhausted
    while available:
        # pick index weighted
        idx = random.choices(available, weights=available_weights, k=1)[0]
        post = generators[idx]()
        if post:
            out_path = post.get("out_path")
            if out_path not in donesocials:
                donesocials[out_path] = {
                    "post": post["post"],
                    "timestamp": int(time.time() * 1000),
                }
                return post
        # remove tried generator
        rem = available.index(idx)
        available.pop(rem)
        available_weights.pop(rem)

    # All options exhausted
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--api-key", required=True)
    p.add_argument("--url", required=True)
    args = p.parse_args()
    if os.path.exists(SOCIALS_FILE):
        donesocials = load_json(SOCIALS_FILE)
    else:
        donesocials = {}
    post = create_socials_post(donesocials, args.api_key, args.url)
    print(f"Generated post: {post}")
    with open(SOCIALS_FILE, "w") as f:
        json.dump(donesocials, f, indent=4)
    with open(POST_FILE, "w") as f:
        json.dump(post, f, indent=4)


if __name__ == "__main__":
    main()
