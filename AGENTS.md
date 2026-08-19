# AGENTS.md

Cross-tool guidance for AI coding assistants (Claude Code, Codex, Cline, etc.) working in
**MythiStone**. This is the always-on source of repo truth. Task-specific gotchas live as
progressive-disclosure skills under `.claude/skills/`; read those when a task matches one.

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

## Directory map

- `backend_scripts/`: all Python.
  - Collectors/ingestion: `fetch*.py`, `collect*.py`, `download*.py`.
  - Transforms: `process*.py`.
  - Page generators: `generate*.py` (one per page/artifact).
  - Shared: `databaseConnector.py` (the entire MySQL data-access layer, ~160 KB, every SQL
    statement lives here), `pageGeneration.py`, `aggregateData.py`, `commonUtils.py`.
  - `localDev/`: throwaway MySQL seeder for offline rendering (see the `local-test-render` skill).
  - `image_generation/`: Matplotlib/PIL social + preview image renderers.
  - `social_posts/`: automated social-media pipeline.
  - `simcBis.py` + `*Simc*.sh`: SimulationCraft integration.
  - `database.sql`: tables, stored procedures, scheduled EVENTs.
- `templates/`: Jinja2 templates + shared partials/macros.
- `assets/`: frontend `css/`, `scss/` (Material Dashboard source), `js/`, `fonts/`, `img/`.
  Runtime `assets/json/*` is generated and gitignored.
- `data/static/`: JSON lookups the generators and seeder read (`specs.json`, `classes.json`,
  `dungeons.json`, `seasonInfo.json`, etc.).
- `data/icons/`: WoW game icons served to the site.
- `discord_bot/`: standalone dockerized discord.py bot serving the same data.
- `.github/workflows/`: CI `buildPages.yml` (build+deploy), `getStaticData.yml`,
  `buildSimcImage.yml`, `buildCollectorImage.yml`, `buildBotImage.yml`,
  `automatedSocialMediaPosts.yml`, `seasonRolloverWipe.yml`.

## Hard constraints

- **No live DB is available to an agent.** To verify page
  work, seed a throwaway local MySQL (the `local-test-render` skill / `backend_scripts/localDev/`).
- **No throwaway env-var scripts.** DB creds and API keys come from `os.environ` in CI/prod; an
  agent does not have them. Do not write scripts that require secrets. If you need live data, give
  the user the exact command and ask them to paste the output back.
- **All SQL goes through `databaseConnector.py`.** New queries belong as a named `*_SQL` constant +
  wrapper function there, parameterized with `%s`, reusing its retry helpers (`fetch_with_retry`,
  `execute_with_retry`, `executemany_with_retry`). No ad-hoc SQL or new DB paths elsewhere, ever
  from the frontend.
- **Output stays static.** No PHP/Node/Django/Flask serving pages. Valid MySQL only, consistent
  with `database.sql`.

## Templating

Jinja2 **composition** via `{% include %}` and `{% macro %}`. Pages are **standalone full HTML
documents** that assemble the chrome (`sidenav.html`, `navbar.html`, `notifications.html`,
`trends_bar.html`, `footer.html`, `right_aside.html`, `fixed_plugin.html`, `header_imports.html`,
`javascript_imports.html`) via includes. `base_template.html` is only a bare skeleton reference;
pages do **not** `extends` it. Comp markup uses `_team_comp_macros.html`. There are **zero**
`extends`/`block` in `templates/` and no Flask. Reuse the filters registered on the `Environment`
in the generators (`humanize`, `duration`, `format_ts`, `upgrade_info`, etc.) rather than
reimplementing formatting.

**Templates crash on unknown ids.** Lookups are subscripted without a guard, so a page dies on any
id its lookup lacks. New id-bearing data must be seeded from the *same* static-file lookup the
template uses.

## Frontend stack (summary; detail in the design skills)

- **Material Dashboard 3 v3.2.0** on **Bootstrap 5.3.3**.
- **No build step.** Vanilla JS via plain `<script>` includes; app singletons use the
  `window.Mythi*` IIFE namespace. jQuery is present only because bootstrap-select needs it.
- **Dark theme is the unconditional default** via `data-bs-theme` on `<html>` (OS preference is
  deliberately ignored); a `mythistone:themechange` window event drives re-theming.
- Fonts: **Inter** + **Material Symbols Rounded**.
- Compose Bootstrap/MD utility classes first; **never hardcode** class/stat/rarity hex. Use the
  design tokens (see the `frontend-design-tokens`, `frontend-framework-choices`, `build-new-page`,
  and `chart-theming` skills).

## Tooling reality

- No `requirements.txt`/lockfile. Dependencies are installed ad-hoc per CI workflow.
- No linter, formatter, or test framework. The only test is `discord_bot/db_smoke_test.py`, which
  runs the bot's real fetches against the seeded local test DB (see the `discord-bot` skill).
- De-facto verification is the local seed-and-render (`local-test-render` skill). Python 3.11 in
  CI, 3.13 locally.

## Working preferences (always apply)

- **Fail loudly.** Missing data files or failed fetches must raise, never warn-and-continue.
- **No git interaction.** Never run git write commands (`commit`, `push`, `add`, `reset`,
  `checkout`, `merge`, `rebase`). The user commits and pushes. Read-only git is fine.
- **User-facing copy.** No em dashes. Never end a sentence with a semicolon. Do not imply "most
  players pick X" for a minority or deviation pick.
- **Vendor third-party assets manually.** The user downloads vendor JS/CSS themselves. Do not curl
  or fetch them.
- **KISS and DRY.** Smallest change that fits the existing pipeline; reuse/modify existing helpers,
  macros, and lookups before adding code.

## Recording new knowledge (always apply)

When you finish a task that produced newly gained durable knowledge or important findings
(non-obvious mechanics, cross-file coupling, gotchas, corrected assumptions), **ask the user for
permission to record it** — as the **last line of your final message** for that task. Do not record
first and ask later, and do not record silently: the user decides. Store agreed knowledge as a narrow
skill under `.claude/skills/` or a section of this file. Never use a tool's private auto-memory — it
does not transfer across machines or tools; only in-repo notes do.

See the `knowledgebase-authoring` skill for the full protocol: when to skip the ask, how to gauge
"new" against the knowledgebase, and the writing rules (present-tense only, no history/dates, no
line-number references).

## Verifying page work

Any change to a `templates/*.html`, a `backend_scripts/generate*.py`,
`pageGeneration.py`/`aggregateData.py`/`commonUtils.py`, or page CSS/JS must be **test-rendered**
against a locally seeded DB before it is considered done. See
[backend_scripts/localDev/README.md](backend_scripts/localDev/README.md) and the
`local-test-render` skill. Serve the repo root with `python -m http.server 8099` and inspect the
rendered page.
