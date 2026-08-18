"""Post selection, persistence and CLI entry point for the daily socials job.

main() owns the side effects that used to run at import time in the old
generateSocialsPost.py: the DB connection pool (skipped in --debug, which is
documented as DB-free) and creating the output directory.
"""

import argparse
import json
import os
import random
import time
from contextlib import closing
from datetime import datetime

from PIL import Image, ImageDraw, ImageFont

import databaseConnector
import season_gate
from commonUtils import get_dungeon_lookup, get_spec_lookup, load_json, load_season_info
from image_generation import config
from image_generation.pil_helpers import apply_watermark_to_canvas
from social_posts.links import build_site_link
from social_posts.posts import (
    createCompOverview,
    createDungeonOverview,
    createSpecOverview,
    create_MplusRun,
    create_dungeon_popularity_vs_ease,
    create_dungeon_tierlist,
    create_overall_spec_popularity,
    create_season_countdown,
    create_spec_popularity_by_level,
    create_spec_popularity_vs_performance,
)

SOCIALS_FILE = os.path.join("data", "socials.json")
POST_FILE = os.path.join(config.OUTPUT_DIR, "post.json")


def create_socials_post(donesocials, api_key, url):
    """
    Randomly selects one of several post-generating routines, skipping any already done.
    Gives each spec overview an equal chance, collectively outweighing other generators.
    """
    print("Generating social media post...")

    spec_lookup = get_spec_lookup()
    dungeon_lookup = get_dungeon_lookup()

    # Prepare spec IDs for spec overview
    specs = [f for f in spec_lookup.keys()]

    # Prepare dungeon IDs for dungeon overview
    dungeons = []
    if isinstance(dungeon_lookup, dict):
        dungeons = [d.get("id", k) for k, d in dungeon_lookup.items()]
    elif isinstance(dungeon_lookup, list):
        dungeons = [d.get("id") for d in dungeon_lookup]

    current_season_id = int(load_season_info()["blizzard_season_id"])

    # Pre-season gate: during the gap between seasons (DB wiped, no runs logged
    # for the current season yet) every normal generator would render an empty
    # "0 total runs tracked" card. Detect that the same way the Discord bot's
    # season-not-started guard does (season_gate) and post a release countdown
    # instead. Once the first keys are logged this flips back automatically.
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        try:
            started = season_gate.season_has_started(conn, cursor, current_season_id)
        finally:
            cursor.close()
    if not started:
        print("Season has no runs yet: posting release countdown instead of data.")
        post = create_season_countdown(
            config.OUTPUT_DIR, donesocials, url, load_season_info()
        )
        if post and post.get("bundle"):
            out_path = post["out_path"]
            if out_path not in donesocials:
                record = bundle_to_record(post)
                donesocials[out_path] = record
                return {"out_path": out_path, **record}
        # Countdown for today already recorded (or unbuildable): nothing new to post.
        return None

    # Create spec-specific generators
    spec_generators = []
    for spec_id in specs or ["62"]:

        def make_spec_gen(sid):
            return lambda: createSpecOverview(
                config.OUTPUT_DIR, donesocials, api_key, url, sid, current_season_id
            )

        spec_generators.append(make_spec_gen(spec_id))

    # Create dungeon-specific generators
    dungeon_generators = []
    for dungeon_id in dungeons:
        def make_dungeon_gen(did):
            return lambda: createDungeonOverview(
                config.OUTPUT_DIR, donesocials, api_key, url, did, current_season_id
            )

        dungeon_generators.append(make_dungeon_gen(dungeon_id))

    # Other generators
    def gen_dungeon_tier():
        return create_dungeon_tierlist(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_spec_pop_vs_perf():
        return create_spec_popularity_vs_performance(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_dungeon_pop_vs_ease():
        return create_dungeon_popularity_vs_ease(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_overall_spec_popularity():
        return create_overall_spec_popularity(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    def gen_spec_pop_by_level():
        return create_spec_popularity_by_level(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    run_types = ["highest_run", "longest_run", "shortest_run"]

    def make_run_gen(run_type):
        return lambda: create_MplusRun(
            run_type, current_season_id, donesocials, api_key, url
        )

    def gen_comp_overview():
        return createCompOverview(
            config.OUTPUT_DIR, donesocials, api_key, url, current_season_id
        )

    other_generators = [
        gen_dungeon_tier,
        gen_spec_pop_vs_perf,
        gen_dungeon_pop_vs_ease,
        gen_overall_spec_popularity,
        gen_spec_pop_by_level,
        gen_comp_overview,
    ] + [make_run_gen(rt) for rt in run_types]

    # Combine all generators
    generators = spec_generators + other_generators + dungeon_generators

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
        if post and post.get("bundle"):
            out_path = post.get("out_path")
            if out_path not in donesocials:
                record = bundle_to_record(post)
                donesocials[out_path] = record
                return {"out_path": out_path, **record}
        # remove tried generator
        rem = available.index(idx)
        available.pop(rem)
        available_weights.pop(rem)

    # All options exhausted
    return None


def bundle_to_record(post):
    """Flatten a generator result into the record stored in socials.json."""
    bundle = post["bundle"]
    social = bundle["social"]
    return {
        "title": bundle["title"],
        "post_type": post.get("post_type", ""),
        "link": post.get("link", ""),
        # one humorous text used across every social platform
        "social": social,
        "blog": bundle["blog"],
        # legacy field kept so the workflow (and older consumers) keep working
        "post": social,
        "timestamp": int(time.time() * 1000),
    }


def create_debug_post(url):
    """Offline test post: no database, no Blizzard API, no OpenRouter.

    Renders a synthetic image and a canned text bundle so the whole
    socials.json -> blog page pipeline can be exercised locally.
    """
    now = datetime.now()
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    out_path = os.path.join(
        config.OUTPUT_DIR, f"debug_test_{now.strftime('%Y-%m-%d_%H-%M-%S')}.png"
    )

    canvas = Image.new("RGB", (config.WIDTH, config.HEIGHT), "#222222")
    draw = ImageDraw.Draw(canvas)
    title_font = ImageFont.truetype(config.FONT_FILE, config.TITLE_SIZE)
    small_font = ImageFont.truetype(config.FONT_FILE, config.SMALL_SIZE)
    draw.text(
        (config.WIDTH // 2, config.HEIGHT // 2 - 60),
        "Blog Display Test",
        font=title_font,
        fill=(255, 255, 255),
        anchor="mm",
    )
    draw.text(
        (config.WIDTH // 2, config.HEIGHT // 2 + 40),
        f"generated {stamp}",
        font=small_font,
        fill=(200, 200, 200),
        anchor="mm",
    )
    canvas = apply_watermark_to_canvas(
        canvas, position="top_right", padding_x=30, padding_y=30
    )
    canvas.save(out_path, format="PNG")

    link = build_site_link(url, "pages/dashboard")
    bundle = {
        "title": f"Debug post from {stamp}",
        "social": f"[DEBUG] Blog display test generated {stamp}. #WoW #MythicPlus {link}",
        "blog": (
            f"This is a debug post generated locally at {stamp} to verify that "
            "images and text render correctly on the blog page.\n\n"
            "If you can read this on the blog with the image above, the "
            "socials.json record, the image pipeline and the card layout all "
            "work. Delete this entry from data/socials.json (and the PNG from "
            "data/social) when you are done."
        ),
    }
    return {
        "out_path": out_path,
        "bundle": bundle,
        "post_type": "debug_test",
        "link": link,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--api-key")
    p.add_argument("--url", default="https://mythistone.com/")
    p.add_argument(
        "--debug",
        action="store_true",
        help="generate an offline test post (no DB, no Blizzard API, no OpenRouter) to preview on the blog page",
    )
    args = p.parse_args()
    if not args.debug and not args.api_key:
        p.error("--api-key is required unless --debug is set")

    # Side effects that used to run at import time: the pool is only needed by
    # the real pipeline (--debug is documented as database-free).
    if not args.debug:
        databaseConnector.init_connection_pool(
            os.environ.get("DATABASE_HOST"),
            os.environ.get("DATABASE_USER"),
            os.environ.get("DATABASE_PASSWORD"),
            os.environ.get("DATABASE_NAME"),
            os.environ.get("DATABASE_PORT"),
            2,
        )
    config.ensure_output_dir()

    if os.path.exists(SOCIALS_FILE):
        donesocials = load_json(SOCIALS_FILE)
    else:
        donesocials = {}
    if args.debug:
        result = create_debug_post(args.url)
        record = bundle_to_record(result)
        donesocials[result["out_path"]] = record
        post = {"out_path": result["out_path"], **record}
        print("DEBUG: offline test post created; do NOT commit this socials.json entry")
    else:
        post = create_socials_post(donesocials, args.api_key, args.url)
    print(f"Generated post: {post}")
    with open(SOCIALS_FILE, "w") as f:
        json.dump(donesocials, f, indent=4)
    with open(POST_FILE, "w") as f:
        json.dump(post, f, indent=4)


if __name__ == "__main__":
    main()
