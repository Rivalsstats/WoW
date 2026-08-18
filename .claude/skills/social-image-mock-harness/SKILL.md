---
name: social-image-mock-harness
description: How to verify the social image renderers in backend_scripts/image_generation/ locally against the seeded local test DB. Use when changing anything under image_generation/ (comp_overview, dungeon_overview, item_overview, dungeon_tierlist, mplus_run, config, mpl_setup).
---

# Social image verification

The renderers in `backend_scripts/image_generation/*` all take their DB rows **injected by the
caller** (`spec_upgrades=`, `dungeon_data=`, `total_runs=`, `highest_run=`, `rows=`); only the
lookups/icons are file-based. So the way to verify them is to feed them **real rows from the seeded
local test DB**, not fabricated dicts.

The bot's `discord_bot/db_smoke_test.py` already does exactly this: it fetches
`fetch_spec_upgrades` / `fetch_runs_per_dungeon_per_level` / `fetch_total_season_runs` /
`fetch_max_key_run` from the seeded DB and renders `create_spec_tierlist_img`,
`create_dungeon_tierlist_img`, `create_spec_popularity_vs_performance_img` and (from the live
`simdps_tierlist` artifact) `generate_preview_image`, asserting each PNG is non-empty. Run it after
seeding:

```
python backend_scripts/localDev/seed_test_db.py     # prints DATABASE_* exports
# export those, then:
python -m discord_bot.db_smoke_test
```

Icons are optional: `data/icons` isn't required, tiles/markers just skip and a valid PNG still
lands. For a one-off render of a specific renderer, seed the DB, import the renderer, fetch the rows
it needs via `databaseConnector.fetch_*`, and call it with `out_path` pointed at a scratchpad file.
The SimC tierlist preview needs no DB at all, feed `generate_preview_image` rows directly.

The modern shared palette lives in `backend_scripts/image_generation/config.py` (`BG` / `TEXT` /
`MUTED` / `TIER_COLORS` etc.). The dark matplotlib theme is in
`backend_scripts/image_generation/mpl_setup.py` (`init_matplotlib`).

Why: seed the DB once and every renderer gets realistic, schema-accurate rows, so a query/shape
drift is caught instead of hidden behind hand-maintained fixtures. Full seed workflow:
[[local-test-render]]. The bot that reuses these renderers: [[discord-bot]].
