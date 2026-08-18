---
name: season-launch-day-presentation
description: How the social auto-poster and the Discord bot present the pre-season gap vs the season's first 24h, and why launch day needs its own time-based gate. Use when touching season_gate, social_posts/pipeline.py, image_generation/season_countdown.py, or the bot's guards/errors/embeds around a season start.
---

# Season pre-season vs launch-day presentations

Two consumers switch to a "no real data yet" presentation around a season boundary, and they
must stay in lockstep: the social auto-poster (`backend_scripts/social_posts/`) and the Discord
bot (`discord_bot/`). There are **two distinct windows**, gated differently:

## 1. Pre-season gap — DB gate (`season_gate.season_has_started`)
`backend_scripts/season_gate.py` answers "does the current season have any recorded runs?" (thin
wrapper over `databaseConnector.season_has_runs`). Both consumers import it so they flip together:
- Poster: `create_socials_post` posts `create_season_countdown` (a countdown *card*) instead of
  empty data cards.
- Bot: `season_guard` raises `SeasonNotStarted` → `season_not_started_embed` (a countdown *embed*).

## 2. Launch day (first 24h) — time gate (`in_launch_window`)
The DB gate flips `True` on the **first single logged run**. But regional starts are staggered
(`data/static/seasonInfo.json` `starts`: us/eu/tw/kr/cn), so the instant US logs one key the normal
generators/commands run against near-empty data (US barely started, EU/KR not started) and **error
or render empty**. That is the bug this window fixes.

It is not only that raw `runs` are sparse: on launch day the **nightly aggregation pipeline
(`sp_run_agg_pipeline` / `sp_agg_*` → the `aggregated_*` tables and rollups) has not run for the
new season yet**, so a lot of the data the generators and bot commands actually read (aggregated
throughput, per-dungeon/level rollups, comp/spec aggregates) is simply *absent*, not just thin.
Waiting on the DB gate alone is therefore not enough — the time window is what lets both consumers
avoid every data path until the first aggregation cycle has populated it.

`image_generation/season_countdown.py` owns the shared, **DB-free** time logic (pure PIL-module but
the helpers touch no PIL/DB — just time + seasonInfo):
- `launch_fields(season_info, now)` → `(live, upcoming, earliest)`, splitting `REGION_ORDER =
  ("us","eu","kr")` (tw/cn share kr) into started vs not-yet by comparing each start to `now`.
- `in_launch_window(season_info, now)` → True when `0 <= (now - earliest_regional_start) < 24h`.
  **Measured from the earliest region**, so all users see the launch presentation for 24h after the
  *first* region opens (EU/KR players get a "starts soon" countdown for their region meanwhile).

Both consumers import `in_launch_window` (single source of truth, same as `season_gate`):
- Poster: `create_socials_post` checks it **before the DB gate**; in-window it posts
  `create_season_launch` (posts.py) and returns — never falling through to the data generators.
  Filename `season_launch_{slug}_{date}.png` posts once/day; card headline "IT'S LIVE" with LIVE
  NOW / STARTS SOON region groups; deterministic copy, no LLM (like the countdown).
- Bot: `season_guard` checks it **first, outside the fail-open DB try/except** (so it is never
  swallowed) and raises `SeasonJustStarted` → `season_started_embed` (Live now / Starts soon
  fields, upcoming regions as live `<t:…:R>` Discord timestamps). Routed in `errors.py` *before*
  `SeasonNotStarted` so launch day wins once a region is live.

## Ordering / registration gotchas
- Precedence everywhere: **launch window → pre-season (no runs) → normal data**. Future earliest
  start ⇒ not in launch window ⇒ pre-season countdown; started <24h ⇒ launch; started >24h ⇒ normal.
- A new social post type must be registered in `generateBlogPage.py` in **both** `POST_TYPE_META`
  and `FILENAME_TYPE_PATTERNS` (`season_launch` → "Season Launch" badge, `season_launch_` prefix).
- The launch card/embed and copy are pure time + `seasonInfo.json` (no DB/Blizzard/LLM), so verify
  them by feeding a synthetic `seasonInfo` whose `starts` are relative to now (e.g. us -3h, eu +10h,
  kr +29h) — no DB seed needed. See [[social-image-mock-harness]] for the DB-backed renderers.
