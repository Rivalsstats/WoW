"""Temporary dev helper: render one of each social image against the live DB
without running any page generation or the posting pipeline.

Run from the repo root with the DATABASE_* env vars set:

    python backend_scripts/renderAllSocialImages.py
    python backend_scripts/renderAllSocialImages.py --spec 62 --dungeon 1209
    python backend_scripts/renderAllSocialImages.py --only spec_overview,mplus_run
    python backend_scripts/renderAllSocialImages.py --no-route   # skip the slow keystone.guru thumbnail

Images land in --out (default: social_preview/). Safe to delete this script
once the restyle work is settled.
"""

import argparse
import os
import sys
import traceback

import databaseConnector
from commonUtils import get_dungeon_lookup, get_spec_lookup, load_json


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="social_preview", help="output directory (default: social_preview)")
    p.add_argument("--spec", help="spec id for spec_overview/run card (default: first DPS spec)")
    p.add_argument("--dungeon", help="dungeon id for dungeon_overview (default: first dungeon)")
    p.add_argument("--season", type=int, help="season id (default: blizzard_season_id from seasonInfo.json)")
    p.add_argument("--no-route", action="store_true",
                   help="skip the keystone.guru route thumbnail for dungeon_overview (faster, no KEYSTONE_GURU_* creds needed)")
    p.add_argument("--only", help="comma-separated subset of image names to render (see keys printed in the summary)")
    p.add_argument("--item", help="item id or slug for the item_overview card (default: most-used item)")
    args = p.parse_args()

    missing = [v for v in ("DATABASE_HOST", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME", "DATABASE_PORT")
               if not os.environ.get(v)]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)}")

    databaseConnector.init_connection_pool(
        os.environ.get("DATABASE_HOST"),
        os.environ.get("DATABASE_USER"),
        os.environ.get("DATABASE_PASSWORD"),
        os.environ.get("DATABASE_NAME"),
        os.environ.get("DATABASE_PORT"),
        2,
    )

    season = args.season or load_json(os.path.join("data", "static", "seasonInfo.json")).get("blizzard_season_id")
    spec_lookup = get_spec_lookup()
    dungeon_lookup = get_dungeon_lookup()
    spec_id = args.spec or next(s for s in sorted(spec_lookup, key=int) if int(spec_lookup[s].get("role", 2)) == 2)
    dungeon_id = args.dungeon or next(iter(sorted(dungeon_lookup, key=str)))

    out = args.out
    os.makedirs(out, exist_ok=True)

    from image_generation import config
    config.OUTPUT_DIR = out  # mplus_run writes into config.OUTPUT_DIR

    if args.no_route:
        # skip the keystone.guru thumbnail round-trip (can take minutes)
        databaseConnector.fetch_dungeon_top_routes = lambda *a, **k: []

    from image_generation.comp_overview import createCompOverviewImg
    from image_generation.dungeon_overview import createDungeonOverviewImg
    from image_generation.dungeon_popularity_ease import create_dungeon_popularity_vs_ease_img
    from image_generation.dungeon_tierlist import create_dungeon_tierlist_img
    from image_generation.mplus_run import create_MplusImage, get_run_data
    from image_generation.spec_distribution_by_level import create_spec_popularity_by_level_img
    from image_generation.spec_overview import createSpecOverviewImg
    from image_generation.spec_popularity_performance import create_spec_popularity_vs_performance_img
    from image_generation.spec_popularity_tierlist import create_spec_tierlist_img

    def render_mplus_run():
        active_run = get_run_data("highest_run", False, season)
        if not active_run:
            raise ValueError(f"no highest_run found for season {season}")
        create_MplusImage(active_run, "highest_run", {}, check_socials=False)

    def render_comp_overview():
        from contextlib import closing
        from generateCompPage import calculate_comp_stats, compute_meta_comp, compute_top_comps
        with closing(databaseConnector.get_connection()) as conn:
            frontend_json = calculate_comp_stats(conn, conn.cursor(), season, spec_lookup)[0]
        createCompOverviewImg(out, os.path.join(out, "comp_overview.png"), season,
                              meta_comp=compute_meta_comp(frontend_json),
                              top_comps=compute_top_comps(frontend_json))

    def render_item_overview():
        # Reuse the item page's single DB sweep + payload assembly, then render
        # one card from the in-memory payload (no re-fetching, no DB in the card
        # renderer). With --item we only assemble that one item's payload.
        import generateItemPages as gip
        from image_generation.item_overview import render_item_card

        ctx = gip.load_static_lookups()
        only = None
        if args.item:
            if str(args.item).isdigit():
                only = str(args.item)
            else:
                slug_to_id = {slug: iid for iid, slug in ctx["slug_map"].items()}
                if args.item not in slug_to_id:
                    raise ValueError(f"item slug '{args.item}' not found")
                only = str(slug_to_id[args.item])
        payloads, manifest = gip.build_payloads(season, ctx, only_item=only)
        if not manifest:
            raise ValueError(f"no usage data found for item {args.item or '(most-used)'}")
        target = only or str(manifest[0]["id"])
        payload = payloads.get(target)
        if not payload:
            raise ValueError(f"no payload assembled for item {target}")
        slug = ctx["slug_map"][int(target)]
        render_item_card(payload, slug, os.path.join(out, "item_overview.png"))

    def render_items_overview():
        # Reuse the item page's static lookups + single DB sweep to get the
        # manifest, then render the browse-page overview card from it (no DB in
        # the card renderer).
        import generateItemPages as gip
        from image_generation.item_overview import render_items_overview as _render

        ctx = gip.load_static_lookups()
        _payloads, manifest = gip.build_payloads(season, ctx)
        if not manifest:
            raise ValueError("no item usage data found")
        _render(manifest, ctx["season_info"].get("name", ""),
                os.path.join(out, "items_overview.png"))

    renderers = {
        "comp_overview": render_comp_overview,
        "dungeon_overview": lambda: createDungeonOverviewImg(out, os.path.join(out, "dungeon_overview.png"), dungeon_id, season),
        "mplus_run": render_mplus_run,
        "spec_overview": lambda: createSpecOverviewImg(out, os.path.join(out, "spec_overview.png"), spec_id, season),
        "dungeon_tierlist": lambda: create_dungeon_tierlist_img(os.path.join(out, "dungeon_tierlist.png"), season),
        "spec_popularity_tierlist": lambda: create_spec_tierlist_img(os.path.join(out, "spec_popularity_tierlist.png"), season),
        "spec_distribution_by_level": lambda: create_spec_popularity_by_level_img(os.path.join(out, "spec_distribution_by_level.png"), season),
        "spec_popularity_performance": lambda: create_spec_popularity_vs_performance_img(os.path.join(out, "spec_popularity_performance.png"), season),
        "dungeon_popularity_ease": lambda: create_dungeon_popularity_vs_ease_img(os.path.join(out, "dungeon_popularity_ease.png"), season),
        "item_overview": render_item_overview,
        "items_overview": render_items_overview,
    }

    if args.only:
        wanted = [n.strip() for n in args.only.split(",") if n.strip()]
        unknown = [n for n in wanted if n not in renderers]
        if unknown:
            sys.exit(f"Unknown image name(s): {', '.join(unknown)}. Valid: {', '.join(renderers)}")
        renderers = {n: renderers[n] for n in wanted}

    print(f"Rendering {len(renderers)} image(s) into {out}/ (season {season}, spec {spec_id}, dungeon {dungeon_id})")
    results = {}
    for name, fn in renderers.items():
        try:
            fn()
            results[name] = "OK"
        except Exception as e:
            results[name] = f"FAIL: {e}"
            traceback.print_exc()

    print("\n=== results ===")
    for name, status in results.items():
        print(f"{name}: {status}")
    if any(v != "OK" for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
