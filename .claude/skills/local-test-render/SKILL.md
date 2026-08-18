---
name: local-test-render
description: How to render MythiStone pages locally against a throwaway seeded MySQL. Use whenever you change a templates/*.html, a backend_scripts/generate*.py, pageGeneration.py/aggregateData.py/commonUtils.py, or page CSS/JS, and must verify the rendered page before handing it back.
---

# Local test render workflow

Never hand back page work that was only reasoned about. Render it against the local seeder in `backend_scripts/localDev/` (full docs: `backend_scripts/localDev/README.md`).

1. Seed a throwaway MySQL (needs Docker Desktop running). It starts one reusable `mysql:8` container `mythistone-testdb`, loads `backend_scripts/database.sql`, seeds plausible data from `data/static/**`, and runs the real `sp_run_agg_pipeline()`. Run `python backend_scripts/localDev/seed_test_db.py`. It prints `DATABASE_*` exports (user Test/test, host port 3399) in cmd / PowerShell / bash form. Point the generators at those.

2. Render the page you changed with its generator. Use the arguments that the buildPages workflow uses. Output dirs (`dungeons/`, `classes/`, `items/`, `pages/`) are gitignored. Examples:
   - `python backend_scripts/generateDungeonPages.py --template templates/dungeon_page.html --output_dir dungeons`
   - `python backend_scripts/generateItemPages.py --template templates/items.html --output_dir pages --items_dir items`
   - `python backend_scripts/generateCompPage.py --template templates/comps.html --output_dir pages`

3. Serve the repo root (pages use absolute `/assets/` paths) and inspect in the Browser pane: `python -m http.server 8899 --bind 127.0.0.1 &`, then `preview_start` `http://127.0.0.1:8899/<path>`. Confirm the cards you touched render with data and the console has no errors.

4. Optional teardown: `python backend_scripts/localDev/seed_test_db.py --teardown`.

Every generator (spec, index, dashboard, dungeon, item, comp, routes, analyzer, tierlist, search, sitemap, llms) is credential-free: it needs only the `DATABASE_*` exports.

Dungeon renders: For synthetic seed routes the keystone.guru thumbnail fetch can poll an error job for up to 25 minutes per dungeon (looks like a hang). Without the credentials, it fails fast and the page renders minus the route-map image, which cannot exist for a fake route. See [[add-database-table]] for keeping the seeder complete when the schema grows.
