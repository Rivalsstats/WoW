"""Per-post-type wrappers: each builds the output filename, skips posts that
were already published (donesocials), calls the matching image_generation
renderer and assembles the text bundle. Output filename prefixes are load-
bearing — generateBlogPage.py's FILENAME_TYPE_PATTERNS maps them back to post
types for legacy socials.json entries."""

import os
from contextlib import closing
from datetime import datetime

import databaseConnector
from commonUtils import format_comp_names, get_spec_lookup
from image_generation.comp_overview import createCompOverviewImg
from image_generation.dungeon_overview import createDungeonOverviewImg
from image_generation.dungeon_popularity_ease import create_dungeon_popularity_vs_ease_img
from image_generation.dungeon_tierlist import create_dungeon_tierlist_img
from image_generation.mplus_run import create_MplusImage, get_run_data
from image_generation.spec_distribution_by_level import create_spec_popularity_by_level_img
from image_generation.spec_overview import createSpecOverviewImg
from image_generation.spec_popularity_performance import create_spec_popularity_vs_performance_img
from image_generation.spec_popularity_tierlist import create_spec_tierlist_img
from social_posts.links import build_site_link, dungeon_page_link, spec_page_link, time_ago
from social_posts.llm import build_bundle, get_openai_client


def create_MplusRun(run, season, donesocials, api_key, url):
    active_run = get_run_data(run, False, season)
    if not active_run:
        raise ValueError(f"No {run} found for season {season}")

    # --- pull core fields ---
    mplus_image = create_MplusImage(active_run, run, donesocials, True)

    if not mplus_image:
        print(f"Skipping {run} as it already exists in donesocials.")
        return None

    client = get_openai_client(api_key)

    comp = format_comp_names(
        ",".join(str(m["spec_id"]) for m in active_run.get("members", []))
    )
    post_data = {
        "dungeon": mplus_image["dungeon_name"],
        "level": mplus_image["level"],
        "duration": mplus_image["duration_str"],
        "run_happened": time_ago(int(mplus_image["timestamp"])),
        "region": mplus_image["region"],
        "run_type": f"{run} this season",
        "comp": comp,
    }
    print(post_data)
    link = build_site_link(url, "pages/dashboard")
    bundle = build_bundle(client, post_data, link, run, run.replace("_", " "))
    return {
        "out_path": mplus_image["out_path"],
        "bundle": bundle,
        "post_type": run,
        "link": link,
    }


def create_overall_spec_popularity(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_popularity_tierlist_{week}.png")
    if out_path in donesocials:
        return None

    post_data = create_spec_tierlist_img(out_path, season)
    if post_data is None:
        # nothing to do
        return None

    print(post_data)
    client = get_openai_client(api_key)
    link = build_site_link(url)
    bundle = build_bundle(
        client, post_data, link, "spec_popularity_tierlist", "spec performance tier list"
    )
    return {
        "out_path": out_path,
        "bundle": bundle,
        "post_type": "spec_popularity_tierlist",
        "link": link,
    }


def create_spec_popularity_by_level(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_distribution_by_level_{week}.png")
    if out_path in donesocials:
        return None

    post_data = create_spec_popularity_by_level_img(out_path, season)

    print(post_data)
    client = get_openai_client(api_key)
    link = build_site_link(url, "pages/dashboard")
    bundle = build_bundle(
        client, post_data, link, "spec_distribution_by_level", "spec distribution across key levels"
    )
    return {
        "out_path": out_path,
        "bundle": bundle,
        "post_type": "spec_distribution_by_level",
        "link": link,
    }


def create_dungeon_popularity_vs_ease(output_dir, donesocials, api_key, url, season):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"dungeon_popularity_across_keylevels_{week}.png")
    if out_path in donesocials:
        return None

    post_data = create_dungeon_popularity_vs_ease_img(
        out_path, season)
    link = build_site_link(url, "pages/dashboard")
    if api_key is not None:
        print(post_data)
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, post_data, link, "dungeon_popularity_by_level", "dungeon popularity across key levels"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "dungeon_popularity_by_level",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "dungeon_popularity_by_level", "link": link}


def create_spec_popularity_vs_performance(output_dir, donesocials, api_key, url, season):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_popularity_vs_performance_{week}.png")
    if out_path in donesocials:
        return None

    post_data = create_spec_popularity_vs_performance_img(
        out_path, season)
    link = build_site_link(url, "pages/dashboard")
    if api_key is not None:
        print(post_data)
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, post_data, link, "spec_popularity_vs_performance", "spec popularity vs performance"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "spec_popularity_vs_performance",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "spec_popularity_vs_performance", "link": link}


def create_dungeon_tierlist(
    output_dir, donesocials, api_key, url, season, icon_size=0.4
):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"dungeon_tierlist_{week}.png")
    if out_path in donesocials:
        return None
    post_data = create_dungeon_tierlist_img(
        out_path, season, icon_size)
    link = build_site_link(url, "pages/dashboard")
    if api_key is not None:
        print(post_data)
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, post_data, link, "dungeon_tierlist", "dungeon tier list"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "dungeon_tierlist",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "dungeon_tierlist", "link": link}


def createSpecOverview(output_dir, donesocials, api_key, url, spec_id, season):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"spec_overview_{spec_id}_{week}.png")

    if out_path in donesocials:
        return None
    result = createSpecOverviewImg(
        'tmp', out_path, spec_id, season)
    link = spec_page_link(url, spec_id)
    if api_key is not None and result and result.get("post_data"):
        print(result["post_data"])
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, result["post_data"], link, "spec_overview", "spec overview"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "spec_overview",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "spec_overview", "link": link}


def createDungeonOverview(output_dir, donesocials, api_key, url, dungeon_id, season):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"dungeon_overview_{dungeon_id}_{week}.png")

    if out_path in donesocials:
        return None

    result = createDungeonOverviewImg('tmp', out_path, dungeon_id, season)

    link = dungeon_page_link(url, dungeon_id)
    if api_key is not None and result and result.get("post_data"):
        print(result["post_data"])
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, result["post_data"], link, "dungeon_overview", "dungeon overview"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "dungeon_overview",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "dungeon_overview", "link": link}


def createCompOverview(output_dir, donesocials, api_key, url, season):
    week = datetime.now().strftime("%Y-%m")
    out_path = os.path.join(output_dir, f"comp_overview_{season}_{week}.png")

    if out_path in donesocials:
        return None

    try:
        # lazy import: keeps jinja2 (pulled in by generateCompPage) out of the
        # socials pipeline's import path unless a comp post is actually built
        from generateCompPage import calculate_comp_stats, compute_meta_comp, compute_top_comps
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor(dictionary=False)
            frontend_json = calculate_comp_stats(conn, cursor, season, get_spec_lookup())[0]
        meta_comp = compute_meta_comp(frontend_json)
        top_comps = compute_top_comps(frontend_json)
    except Exception as e:
        print(f"Stats check failed: {e}")
        meta_comp = None
        top_comps = None

    result = createCompOverviewImg('tmp', out_path, season, meta_comp=meta_comp, top_comps=top_comps)

    link = build_site_link(url, "pages/comps")
    if api_key is not None and result and result.get("post_data"):
        print(result["post_data"])
        client = get_openai_client(api_key)
        bundle = build_bundle(
            client, result["post_data"], link, "comp_overview", "global comp overview"
        )
        return {
            "out_path": out_path,
            "bundle": bundle,
            "post_type": "comp_overview",
            "link": link,
        }
    return {"out_path": out_path, "bundle": None, "post_type": "comp_overview", "link": link}
