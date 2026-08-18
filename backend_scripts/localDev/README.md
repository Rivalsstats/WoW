# localDev — seed a throwaway MySQL for local page renders

This tool stands up a disposable MySQL 8 in Docker,
loads the current `backend_scripts/database.sql`, fills it with randomized-but-plausible data
sampled from `data/static/**`, then runs the **real** aggregation stored procedures so the
generators can be run locally against realistic aggregates.

## Requirements

- Docker Desktop running (the tool uses a `mysql:8` container).
- Python deps: `pip install mysql-connector-python` (the connector). The generators you run
  afterwards need their usual deps (`jinja2 pyyaml requests bs4 Pillow matplotlib pandas`).

## Quick start

```bash
python backend_scripts/localDev/seed_test_db.py
```

This provisions the container, loads the schema, seeds it, builds every aggregate, and prints
the `DATABASE_*` environment variables to point the generators at. Then, e.g.:

```bash
# (paste the exports the tool printed first)
python backend_scripts/generateDungeonPages.py --template templates/dungeon_page.html --output_dir dungeons
python backend_scripts/generateItemPages.py --template templates/items.html --output_dir pages --items_dir items
python backend_scripts/generateSpecPages.py --template templates/spec_page.html --output_dir classes
```

## Also backs the Discord bot smoke test

The same seeded DB backs `discord_bot/db_smoke_test.py`, which runs the bot's real
`databaseConnector.fetch_*` queries against it (and fetches the live site JSON artifacts over HTTP).
After seeding, export the printed `DATABASE_*` and run `python -m discord_bot.db_smoke_test`. The
`db-smoke-test` CI job in `buildBotImage.yml` does exactly this to gate the bot image build. See the
`discord-bot` skill.

## Options

| flag | default | meaning |
|------|---------|---------|
| `--runs-per-dungeon N` | 150 | runs generated per dungeon (5 members each) |
| `--routes-per-dungeon N` | 20 | route rows per dungeon |
| `--top-player-ranks N` | 12 | top-player loadout ranks per spec |
| `--simc-bis-ranks N` | 3 | SimC BiS ranks per slot per spec |
| `--seed N` | 1337 | RNG seed (reproducible data) |
| `--host-port N` | 3399 | host port mapped to the container's 3306 |
| `--container-name` | `mythistone-testdb` | container name |
| `--reuse` | | container is already seeded; just reprint the env exports |
| `--teardown` | | remove the container and exit |
| `--skip-trends` | | don't seed the Top-Trends bar's previous week |

## How it stays correct as the schema grows

`table_registry.py` classifies **every** base table in `database.sql` into a seeding strategy.
`seed_test_db.py` introspects the live schema and calls `classify_all`, which **raises on any
table it can't place**. When someone adds a new collector table to `database.sql`, this tool
fails loudly until the table is registered — it never silently renders pages on a
half-populated schema.

### Adding a seeder when a new table appears

1. Decide the category and add the table name to the matching set in `table_registry.py`:
   - `REFERENCE_TABLES` — a lookup / FK-target table seeded from `data/static`.
   - `RAW_TABLES` — a collector detail table the aggregation procs read.
   - `STANDALONE_TABLES` — a read table the pipeline does *not* build (like `top_player_*`).
   - `CONTROL_TABLES` / `IGNORE_TABLES` — control/watermark or diagnostics tables.
   - Anything matching `aggregated_*` / `global_aggregated_*` is auto-classified `PIPELINE`
     (built by `sp_run_agg_pipeline`) and needs no seeder.
2. If it's REFERENCE/RAW/STANDALONE, add rows for it in `seeders.py` (follow the existing
   `seed_*` functions) and call it from `seed_test_db.py`.

## What it does and doesn't cover

- **Aggregated tables are built by the real procs.** New `aggregated_*` tables are covered
  automatically — no seeder change needed.
- **Bounded pools.** Equipment/talents/enchants are drawn from small per-slot / per-spec pools
  so "popularity %" distributions concentrate like live data instead of being flat noise.
- **Current expansion only.** Items, gems, and enchants are sampled from the current expansion
  (the highest `expansion` in `equippable-items.json`, Midnight = 11), so pages show current
  content instead of random relics pulled from all of WoW history.
- **Top-50 loadouts are full, per-dungeon builds.** Each ranked player gets one loadout per
  dungeon carrying class + spec + hero nodes, with per-dungeon "flex" picks, so the spec page's
  Talent Differences modal and the analyzer's talent display have real per-dungeon signal.
- **Embellishments & missives** are seeded onto gear (real bonus ids from
  `embellishments.json` / `missives.json`) so their aggregates populate.
- **Concentrated comp distribution.** A designed set of comps (one dominant meta with >1000
  runs, plus niche "gem" comps and glue/filler comps) is seeded with lightweight runs so the
  comps page's Hidden Gems and Glue Specs (Flexibility Index) sections have data.
- **Hero trees never split 50/50.** Both raw members and the top-50 loadouts skew the hero-tree
  distribution (the top-50 keeps both trees above the per-dungeon-diff threshold).
- **Tertiary stats** (avoidance / lifesteal / speed) are seeded alongside primary/secondary.
- **14-day window.** All seeded runs are timestamped inside the last 14 days because the
  aggregation procs ignore anything older.
- **Only ids the render lookups know.** Every seeded id is drawn from the exact lookup the
  templates subscript (which the spec page does *without* a guard, so an unknown id crashes
  the build): items from `equippable-items.json`, enchants by `enchantments.json` `id`, gems
  by the `itemId` of its socket-slot entries, talent nodes / hero trees from the processed
  `data/static/talents/<specId>.json` `talents` / `subTrees` (a subset of the raw
  `talents.json` — processTalents drops some nodes). `seeders.py` filters against these, so
  add new id types the same way if a template grows a new strict lookup.
- **Known unrealistic bits (harmless for a build):**
  - `loadout` talent strings are synthetic placeholders. The generators don't decode them
    server-side; the spec-page talent trees come from the seeded talent aggregates. The
    client-side analyzer's *meta build* comparison won't decode a seeded string (but the
    talent-tree display itself renders).
  - **Run dungeon renders WITHOUT keystone.guru creds.** `generateDungeonPages.py` fetches a
    route-map thumbnail from keystone.guru for each dungeon's top route. Synthetic seed routes
    don't exist there, so with `KEYSTONE_GURU_USER`/`KEYSTONE_GURU_PW` set the request can return
    an "error" job the generator polls for up to 25 minutes per dungeon (looks like a hang). With
    those env vars unset the request fails fast and the page renders immediately (just without the
    route-map image, which can't be generated for a fake route). So unset them for test renders:
    `set KEYSTONE_GURU_USER=` and `set KEYSTONE_GURU_PW=` (cmd).
  - The analyzer's talent display needs the processed `data/static/talents/<specId>.json`
    files to carry `fullNodeOrder` + `nodes` (the decode data `processTalents.py` writes). If
    those are stale, regenerate them: `python backend_scripts/processTalents.py` (on Windows
    prefix `PYTHONUTF8=1`), then re-run `generateSpecPages.py` so each spec_meta JSON is
    rebuilt with the talent block.
  - Every generator (spec, index, dashboard, dungeon, item, comp, routes, analyzer, tierlist,
    search, sitemap, llms) is credential-free — it needs only the `DATABASE_*` exports above.
    The season id these pages render against comes from `data/static/seasonInfo.json`
    (`commonUtils.current_season_id()`), not the Blizzard API.

## Teardown

```bash
python backend_scripts/localDev/seed_test_db.py --teardown
```
