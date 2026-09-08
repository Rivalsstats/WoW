# AGENTS.md

Cross-tool guidance for AI coding assistants (Claude Code, Codex, Cline, etc.) working in
**MythiStone**. This is the single, always-on source of repo truth. There is no separate skills
tree and no per-tool memory: everything durable lives here.

## How knowledge is handled (read this first)

Durable repo knowledge lives in exactly two files, both checked into the repo so it transfers across
machines and tools:

- **`AGENTS.md`** (this file): all repo truth. Architecture, constraints, preferences, and every
  hard-won subsystem gotcha.
- **`.claude/CLAUDE.md`**: Claude Code-specific working rules only (route work through the expert
  agent, always test-render). It points here and adds nothing else.

Never store repo facts in a tool's private auto-memory. It does not transfer between machines or
tools and silently rots; in-repo notes do not.

### When to record

After finishing a task that produced newly gained durable knowledge (non-obvious mechanics,
cross-file coupling, gotchas, corrected assumptions), add it to the relevant section of this file
**whenever it is relevant**, without asking permission first, then note in your final message what
you recorded so the user can review or revert. Skip recording only when nothing is new: planning
work that changed nothing, or behavior this file already captures. Gauge "new" against this file,
not the code. Much of the codebase is undocumented, so a fact being derivable from the code is not a
reason to skip it if this file does not yet capture it.

### How to write it

- **State only current truth, never history.** Describe how the project works right now and what
  caveats exist right now. No "this used to work like X", no changelog entries, no migration notes,
  no dated "verified on <date>" lines. When something changes, edit the affected prose so it reads
  as if the new behavior was always the behavior, and delete anything that only contrasted the old
  way.
- **No line-number references.** Lines shift and rot into noise. Identify code by stable names:
  file, function, proc, SQL constant, class, or CSS selector.
- **Keep it dense.** One tight paragraph or a few bullets per gotcha. Preserve the non-obvious "why"
  and the "do not do X" warnings; drop verbose retellings.

## What this is

A World of Warcraft Mythic+ **static-site pipeline**. Nothing is served dynamically. The flow:

```
collectors (fetch*/collect*/download*.py)
  -> MySQL (schema in backend_scripts/database.sql)
  -> aggregation stored procs (sp_run_agg_pipeline, sp_agg_*)
  -> generators (generate*.py) render Jinja2 -> static HTML
  -> deployed to GitHub Pages
```

Collect offline, render static, host static. The published site is plain HTML/CSS/JS. There is
**no web framework** and no request-time code.

## Hard constraints

- **No live DB is available to an agent.** To verify page work, seed a throwaway local MySQL (see
  Verification below).
- **No throwaway env-var scripts.** DB creds and API keys come from `os.environ` in CI/prod; an
  agent does not have them. Do not write scripts that require secrets. If you need live data, give
  the user the exact command and ask them to paste the output back.
- **All SQL goes through `databaseConnector.py`.** New queries belong as a named `*_SQL` constant +
  wrapper function there, parameterized with `%s`, reusing its retry helpers (`fetch_with_retry`,
  `execute_with_retry`, `executemany_with_retry`). No ad-hoc SQL or new DB paths elsewhere, ever
  from the frontend.
- **Output stays static.** No PHP/Node/Django/Flask serving pages. Valid MySQL only, consistent
  with `database.sql`.

## Working preferences (always apply)

- **Fail loudly.** Missing data files or failed fetches must raise, never warn-and-continue (unless
  a section below documents an intentional fail-soft, like raids.json or icon self-heal).
- **No git interaction.** Never run git write commands (`commit`, `push`, `add`, `reset`,
  `checkout`, `merge`, `rebase`). You cannot run them even with permission, so never ask the user to
  run them either. The user commits and pushes and decides when. Read-only git is fine.
- **User-facing copy.** No em dashes. Never end a sentence with a semicolon. Do not imply "most
  players pick X" for a minority or deviation pick.
- **Vendor third-party assets manually.** The user downloads vendor JS/CSS themselves. Do not curl
  or fetch them.
- **KISS and DRY.** Smallest change that fits the existing pipeline; reuse/modify existing helpers,
  macros, and lookups before adding code.

## Tooling reality and verification

- No `requirements.txt`/lockfile. Dependencies are installed ad-hoc per CI workflow. No linter,
  formatter, or test framework. Python 3.11 in CI, 3.13 locally.
- Every generator is credential-free: it needs only the `DATABASE_*` exports the seeder prints.

**Local test render** (the de-facto verification; full docs in
`backend_scripts/localDev/README.md`). Any change to a `templates/*.html`, a
`backend_scripts/generate*.py`, `pageGeneration.py`/`aggregateData.py`/`commonUtils.py`, or page
CSS/JS must be test-rendered before it is done. Never hand back page work that was only reasoned
about.

1. Needs Docker Desktop. `python backend_scripts/localDev/seed_test_db.py` starts one reusable
   `mysql:8` container `mythistone-testdb`, loads `database.sql`, seeds plausible data from
   `data/static/**`, runs the real `sp_run_agg_pipeline()`, and prints `DATABASE_*` exports (user
   Test/test, host port 3399). Seeded runs are timestamped inside the last 14 days (aggregations
   ignore older gear/talent data).
2. Render with the generator, using the args `buildPages.yml` uses. Output dirs (`dungeons/`,
   `classes/`, `items/`, `pages/`) are gitignored. Examples: `generateDungeonPages.py --template
   templates/dungeon_page.html --output_dir dungeons`; `generateSpecPages.py --template
   templates/spec_page.html --output_dir classes --spec=252` (`--spec` takes a numeric spec id, not
   a token; a token silently renders nothing).
3. Serve the repo root (pages use absolute `/assets/` paths) and inspect in the Browser pane:
   `python -m http.server 8099 --bind 127.0.0.1` (port matches `.claude/launch.json`), then open
   `http://127.0.0.1:8099/<path>`. Confirm the cards you touched render with data and the console is
   clean.
4. Teardown (optional): `seed_test_db.py --teardown`.
   For synthetic seed routes the keystone.guru thumbnail fetch can poll an error job for ~25 min per
   dungeon (looks like a hang); without creds it fails fast and renders minus the route-map image.

**Verify database.sql procs** without a live DB, against ephemeral MySQL 8 Docker: start
`mysql:8`, wait for `mysqladmin ping`. A `CREATE`-time syntax check catches most bugs (MySQL
validates routine bodies at create time but not table existence or `CREATE TABLE ... LIKE`, which
are runtime). `database.sql` has no `DELIMITER` statements, so extract changed routines and wrap
them `DELIMITER $$ ... END$$ DELIMITER ;`. Runtime `CALL` needs the definer user
(`CREATE USER 'Test'@'%' ...; GRANT ALL ... WITH GRANT OPTION;`) or it fails 1449. Harness traps:
`docker exec` needs `-i` for heredoc stdin; a proc created via `mysql <<SQL` without DELIMITER
truncates at the first inner `;`; write the temp .sql to a real Windows path (not `/c/...` MSYS);
hold an idle in-transaction MDL for contention tests with `START TRANSACTION; SELECT ...; \! sleep
40`. The seeder's `schema_loader.py` does this DELIMITER-wrapping/tablespace-stripping end-to-end;
use the manual recipe only for isolated proc changes.

## Database and aggregation

### Aggregation pipeline (shadow swap)

Per-table aggregation events were replaced by stored procs (`sp_agg_*`) run sequentially by
`ev_nightly_agg_pipeline` -> `sp_run_agg_pipeline`, all in `database.sql`. Per-step timing/errors go
to `agg_pipeline_log`. Rebuilds use **shadow tables** (`<t>_new`) plus atomic `RENAME`, never
`TRUNCATE`: a `TRUNCATE`'s exclusive metadata-lock request behind a long reader wedged the whole
server (MDL waits use `lock_wait_timeout`, default one year), which caused morning lock-ups. FKs on
`aggregated_*` tables were dropped so `CREATE TABLE ... LIKE` shadows work.

To add an aggregate: write a new `sp_agg_<step>` proc plus one `CALL sp_run_agg_step('<step>')` line
in the pipeline. Never add a standalone `TRUNCATE` event. Route swaps through
`sp_swap_public_table(p_base)`, which does `RENAME`+`DROP` with escalating `lock_wait_timeout`
(60->300s), logs blockers into `agg_lock_diag`, and after 3 failed attempts calls
`sp_kill_lock_holders`, which KILLs only idle (`Sleep`) MDL holders so it never hits the always-active
collector. Use short `lock_wait_timeout` plus retry (`sp_run_agg_step` retries on 1205, 30s backoff,
5 attempts), NOT a long timeout (a long timeout parks the exclusive RENAME in the fair MDL queue and
stacks readers behind it). Auditing lesson: the server can hold events/tables not in `database.sql`
(a lost heatmap aggregation was found this way); compare `information_schema.events` / `SHOW TABLES`
against the repo file.

### Gear and talent 2-week retention

Equipment rows (and their `bonus_sets`, via `equipment.bonus_set_id`) and talent info (`talent_sets`,
via `members.talent_set_id`) older than ~2 weeks are purged. So any aggregation joining through
`equipment` or the talent dictionary MUST be a **full rebuild** (shadow+RENAME or 14-day batched),
never watermark-incremental (`summary_meta`), or purged runs stay in the aggregates forever. The
procs also ignore anything older than 14 days on the read side (hence the seeder's 14-day
timestamps). A remaining watermark aggregation over per-character gear/talent state is a bug, not a
model. Spec-page popularity thresholds must use the 14-day `fetch_spec_sample_size` denominator, not
the season-wide `fetch_runs_per_spec`.

### Adding a table to database.sql

Never a one-file change. The seeder introspects the schema and `table_registry.classify_all` raises
`UnknownTableError` on any base table it cannot place (deliberate: it never ships pages on a
half-populated schema). Register the new table in `backend_scripts/localDev/table_registry.py`:
`REFERENCE_TABLES` (lookup/FK-target seeded from `data/static`), `RAW_TABLES` (collector detail the
aggregation reads), `STANDALONE_TABLES` (read table the pipeline does not build, like `top_player_*`,
`simc_bis_*`, `trend_snapshot`), or `CONTROL_TABLES`/`IGNORE_TABLES`. `aggregated_*` /
`global_aggregated_*` auto-classify `PIPELINE`; `*_new`/`*_old` auto-`IGNORE`. For
REFERENCE/RAW/STANDALONE add a `seed_*` in `localDev/seeders.py` and call it from `seed_test_db.py`.
Because templates subscript static lookups without a guard (a page dies on any missing id), seed any
id-bearing column from the **same** static-file lookup the template uses (items from
`equippable-items.json`, enchants by `enchantments.json` id, gems by `slot=='socket'` itemIds,
talent nodes/hero trees from `data/static/talents/<specId>.json`).

## Collectors and season lifecycle

### Season rollover wipe

The per-season blanket DB clear is a three-actor handshake so it never fights the always-on
collector or nightly events. (1) **CI intent**: `seasonRolloverWipe.yml` automated triggers run only
read-only `detect`+`notify`; `requestSeasonWipe.py --commit` runs solely from a manual
`workflow_dispatch` whose `confirm` input must equal the detected season. (2) **Collector pause**:
`collectLeaderboardData.py` `WriteGate` + `wipe_watch()`; while `request_season > done_season` it
pauses writers and acks `collector_paused=1` once quiesced (`read_wipe_control` /
`set_collector_wipe_state` in `databaseConnector.py`). (3) **DB executor**: `ev_season_wipe` fires
only when pending + `collector_paused=1` + it can `GET_LOCK('agg_pipeline')`, then
`CALL sp_season_wipe()` (blanket TRUNCATE, FK checks off, preserves static/reference tables, resets
`summary_meta`) and advances `done_season`. Semantics: `request_season = current` = the season rolled
INTO; requesting 18 clears season-17 data and sets `done_season=18`. Keyed off `seasonInfo.json` (not
`MAX(runs.season)`) because the collector flips `runs.season` before `getStaticData` flips
seasonInfo, and buildPages archives under the seasonInfo id.

Four invariants (break one and collection halts silently or the wipe deadlocks with no error):
1. `wipe_watch` owns the only path that un-pauses `WRITE_GATE`; `asyncio.gather(...,
   return_exceptions=True)` swallows its traceback, so every tick is try/except'd with a `finally`
   that resumes the gate. Never add an un-guarded `await` to that loop.
2. TRUNCATE blocks on the metadata lock governed by `lock_wait_timeout` (default 1 year), NOT
   `innodb_lock_wait_timeout`. `sp_truncate_with_retry` sets `lock_wait_timeout`, escalates, logs
   blockers, kills idle holders, then SIGNALs.
3. The collector resolves season+period exactly once, before the poller loop; resuming after a wipe
   would re-insert old-season rows, so `wipe_watch` sets `restart_event` and `restart: always` brings
   the process back (also how a new weekly period is picked up).
4. Partial regional rollover: each region resolves its own `get_current_season_id`, so US resets
   first and triggers the wipe while EU/KR/TW still return the old season. Module global
   `SEASON_FLOOR` makes `main()` skip a `realm_poller` for any region below it and `process_batch`
   skip rows below it (`0` = no floor). Lagging regions self-heal on a later restart.

### Patch-aware build cadence

`buildPages.yml` builds more often when content is fresh, measured in days since the most recent
content update (season start OR any retail `.5`/`.7` patch): `< 7` days = daily, `7`-`41` =
Mon/Wed/Fri, `>= 42` = Wed only. Cron is every day except Wednesday; Wednesday is always the anchor
via `getStaticData.yml`'s ungated `workflow_run`. `computeBuildPhase.py` (stdlib-only, reads
`data/static/{patches,periods,seasonInfo}.json`) runs as `Decide cadence` and emits `should_build`;
`prepare-sims` gates on it, so a gated-off day skips the whole pipeline and ends green with no
deploy. Only `schedule` events are gated (`push`/`dispatch`/Wed `workflow_run` always build).
Content go-lives snap each patch's `first_seen_ts` forward to the first US reset period, mirroring
`generateDashboardPage.compute_patch_annotations` exactly (keep the two in sync). Pre-season emits
`should_build=false` without raising.

## SimulationCraft

### Chunked checkpoint / resume

The collector container restarts ~daily and simc is pinned to one core, so the heaviest specs need
more than one lifetime. Do NOT just raise the timeout: `pick_next_spec` orders by `updated_at`
(written only on completion/graceful timeout), so a run killed mid-flight is re-picked first and
head-of-line-blocks everyone. Instead `run_simc_bis` sims in chunks of `SIMC_CHUNK_SIZE` (64),
checkpointing each to `simc_bis_progress` (+`_meta` header). `_build_run` combos are deterministic
(`tier_slots` MUST be sorted, since `detect_tier` returns a set and hash randomisation would reorder
the signature each restart). `run_signature` = sha256 of the .simc text EXCLUDING the simc build (so
image pulls do not nuke a run). `prep_snapshot` stores header/candidates/baseline so resume rebuilds
from the snapshot, not from re-prepared data (whose nightly-rebuilt popularity drift would mismatch
the signature and discard banked chunks); a mismatch now means only the profile-gen code changed. A
`failed` chunk queues by `last_attempt_at` (back); an unfailed in-progress run queues by `started_at`
(front); a SIGTERM-killed chunk leaves the checkpoint untouched. `SIMC_RUN_TIMEOUT` (8h) bounds one
chunk and must stay below the restart interval. Only rank-1/best-combo is consumed by any page.

## Templating and page generation

Jinja2 **composition** via `{% include %}` and `{% macro %}`. Pages are **standalone full HTML
documents** that assemble the chrome (`sidenav.html`, `navbar.html`, `notifications.html`,
`trends_bar.html`, `footer.html`, `right_aside.html`, `fixed_plugin.html`, `header_imports.html`,
`javascript_imports.html`) via includes. `base_template.html` is a bare skeleton reference only and
is dead code (nothing includes it); pages do NOT `extends` it. There are zero `extends`/`block` and
no Flask. Reuse the filters registered on the `Environment` in the generators (`humanize`,
`duration`, `format_ts`, `upgrade_info`, etc.). **Templates crash on unknown ids** (lookups are
subscripted without a guard), so new id-bearing data must be seeded from the same static-file lookup
the template uses.

**Scaffolding a new page.** Model on `templates/comps.html` or `spec_page.html`. `<head>` order:
`<title>`, SEO/OG/Twitter meta, `{% include "header_imports.html" %}`, then per-page stylesheets
AFTER the include (so they override the theme); add `stat-colors.css`/`datatables.min.css` when
needed. `<body class="g-sidenav-show g-sidenav-show-right">`, then `sidenav.html`, then `<main
class="main-content position-relative max-height-vh-100 h-100 border-radius-lg">` containing
`navbar.html`, `notifications.html`, `trends_bar.html`, then content in `<div class="container-fluid
py-2 mx-3 w-auto">` with `footer.html` at its end; after `</main>` add `right_aside.html` +
`fixed_plugin.html`. Scripts: `javascript_imports.html` near the end of `<body>`, THEN per-page
plugin `<script>` tags and the inline / `<page>.js`. Cache-bust volatile per-page assets with
`?v={{ generated_at | int }}`. One `<page>.css` and optional `<page>.js` per page; a new page needs a
`generate<Page>Page.py` (own Jinja2 `Environment`, `os.makedirs` its output dir) wired into
`buildPages.yml`.

## Frontend stack

**Material Dashboard 3 v3.2.0** on **Bootstrap 5.3.3**. **No build step**: plain `<script>`
includes, vanilla JS, app singletons on the `window.Mythi*` IIFE namespace. jQuery is present ONLY
because bootstrap-select needs it, so write no new jQuery logic. Fonts: Inter + Material Symbols
Rounded. Off-main-thread comp/route computation runs in `assets/js/comp-routes-worker.js`.

Pick a library by concern: modals/dropdowns/collapse/tabs/tooltips -> Bootstrap 5 `data-bs-*`
(tooltips auto-init in `material-dashboard.js`, do not hand-roll); selects/icon multi-selects ->
bootstrap-select (`.selectpicker`, loaded globally); sortable/searchable tables -> DataTables
(per-page include); charts -> Chart.js v4; fuzzy search -> Fuse.js (`site-search.js`); consent-gated
embeds -> Klaro via `MythiConsent`.

**Design tokens (never hardcode domain hex).** Palette lives in CSS custom properties in
`material-dashboard.css`, `classes.css`, `stat-colors.css`. Brand `var(--bs-primary)` = `#e91e63`.
Class colors are dual tokens in `classes.css`: `--class-<Name>` (contrast-tuned for TEXT) and
`--class-<Name>-raw` (true Blizzard hex for FILLS), PascalCase, with `.class-<Name>-text` /
`.class-<Name>-bg` utilities. Item rarity `--quality-0..8` with `.item-quality-N` /
`.border-quality-N`. Stat tiers `--stat-*` (+`-raw`) with `.stat-<name>` utilities. Scrollbars
`--mythi-scrollbar-*`. UI glyphs use Material Symbols Rounded; game icons from `/data/icons/<id>.jpg`
(specs/buffs) or `.png` (items).

**Theming.** Dark is the **unconditional default** via `data-bs-theme` on `<html>`, set by an inline
pre-paint script in `header_imports.html` reading `localStorage.theme` (OS `prefers-color-scheme` is
deliberately ignored). Every token is defined twice (`:root,[data-bs-theme=light]` and
`[data-bs-theme=dark]`). On toggle, `material-dashboard.js` dispatches a `window`
`mythistone:themechange` event.

**Chart theming.** Use `window.MythiChart` (`assets/js/chart-theme.js`, loaded globally, defines
helpers only so it is safe before Chart.js). `MythiChart.colors` is a live object refreshed on theme
change; read from it for ticks/grid/legend/tooltip/patch lines. Helpers: `refreshColors()`, `rgba()`,
`loadIcons()`, `buildPatchAnnotations()`, `makeIconLabelsPlugin()`, `registerChart()`. It re-themes
and redraws every live chart on `mythistone:themechange` (auto-discovers via `Chart.getChart()`, so
`registerChart` is rarely needed). Series colors are deliberately hardcoded (they encode class/rarity
meaning and are chosen to read on both themes); pull them from the class/quality tokens, not the
chrome palette. Gate zoom/pan to `min-width: 992px` so it does not fight mobile scroll.

**Deep links / permalinks.** `assets/js/deep-link.js` (loaded every page after `bootstrap.min.js`)
makes modals/accordions/tabs addressable as `#<elementId>` + optional `&key=value`, keeps the hash in
sync with `replaceState`, and injects copy-link buttons (no template markup carries them). Extra
state goes through `MythiLink.registerState(key, {read, apply})` (`read()` returns null at page
default so an untouched page keeps a bare URL); non-Bootstrap containers use
`registerRevealer`+`notifyShown`. Registration must happen before boot (one macrotask after
`DOMContentLoaded`), so a script above the `javascript_imports.html` include must register inside a
`DOMContentLoaded` listener. Panels whose id embeds a `run_id` carry `data-share-id` without it (so
stale links no-op). Programmatic scroll MUST pass `behavior: "instant"`: `<html>` has
`scroll-behavior: smooth`, and pages ship hundreds of un-sized icons that reflow mid-scroll and make
the browser abandon a smooth/auto scroll (`settleInView` re-asserts position as late images arrive).

**Klaro + dynamic embeds.** Klaro only swaps `data-src`->`src` for elements present at init, so any
client-rendered keystone.guru iframe must gate consent itself via `MythiConsent.loadEmbed(iframe)`
(`assets/js/consent.js`) on `shown.bs.collapse`. Hard rules: never set `src` on a Klaro-managed embed
(it kills the swap and loads at `display:none`); never use `src=""` (the browser loads the page's own
URL); test loaded-ness with `getAttribute('src')`, never the `.src` property (it resolves `""` to the
document URL so it is never empty); keep the `.iframe-spinner` hidden until a load is in flight;
`followKlaro` repairs Klaro's accept-once path forgetting to restore `display`. `MythiConsent` owns
the whole flow (consent check, deferral, `src`, spinner, stand-in notice).