---
name: build-cadence-phases
description: buildPages schedules daily/every-3-days/weekly by phase, gated on days since the most recent content update (season start or a .5/.7 patch). Use when touching the buildPages cron, computeBuildPhase.py, the resolve-season cadence gate, or reasoning about how often the site rebuilds.
---

# Patch-aware build cadence

`buildPages.yml` does not rebuild on a fixed rhythm. How often it builds tracks how fresh the current
content is, measured in days since the **most recent content update** (season start OR any retail
`.5`/`.7` patch, since those ship gear/talent changes):

| Phase | Days since latest content update | Scheduled build days | Net (incl. Wed anchor) |
|---|---|---|---|
| `daily`     | `< 14`      | Sun,Mon,Tue,Thu,Fri,Sat | every day |
| `three_day` | `14`–`41`   | Mon, Fri | Mon/Wed/Fri |
| `weekly`    | `>= 42`     | none | Wed only |

## How it is wired

- **Cron**: `buildPages.yml` schedule is `'0 8 * * SUN,MON,TUE,THU,FRI,SAT'` — every day **except
  Wednesday**. Wednesday is *always* the weekly anchor: `getStaticData.yml` (Wed) fires the build via
  its `workflow_run` trigger, which is never gated. So the cron only needs to add the *extra* days.
- **Gate**: `backend_scripts/computeBuildPhase.py` runs as the `Decide cadence` step in the
  `resolve-season` job and emits `should_build=true|false` (plus `phase`, `days_since`).
  `prepare-sims` has `if: has_data == 'true' && should_build == 'true'`. Everything downstream
  cascades off `prepare-sims`, so a gated-off day skips the whole pipeline and ends **green** with no
  deploy — the same cascade [[season-snapshot-archive]]'s pre-season skip uses.
- **Only `schedule` events are gated.** `push`, `workflow_dispatch`, and the Wednesday `workflow_run`
  always build (`should_build` returns true immediately when `event_name != 'schedule'`).

## computeBuildPhase.py specifics

- Stdlib-only, reads `data/static/{patches,periods,seasonInfo}.json` directly (no DB — the
  resolve-season job installs nothing extra).
- Content-update go-lives = season start (`seasonInfo.starts.us`) + each patch's `first_seen_ts`
  **snapped forward** to the first US reset period at/after it. This snapping mirrors
  `generateDashboardPage.compute_patch_annotations` exactly (`first_seen_ts` is a build-push time that
  leads go-live by a few days, so it must be snapped to the reset week, not used raw). Keep the two in
  sync if either changes.
- `most_recent = max(go-lives <= now)`; `days_since = (now - most_recent) / 86_400_000`.
- Thresholds are the module constants `DAILY_PHASE_DAYS = 14` and `THREE_DAY_PHASE_DAYS = 42`; the
  `three_day` build days are `THREE_DAY_BUILD_WEEKDAYS = {0, 4}` (Mon, Fri — Wed omitted on purpose).
- **Pre-season**: if no content update is live yet (`now` < season start), it emits
  `phase=pre_season, should_build=false` and does **not** raise — the `has_data` gate
  (`seasonHasData.py`) already governs the clean pre-season skip, and crashing here would turn the job
  red instead of green.
- Test hooks (no DB, no CI): `--now` (epoch ms or ISO-8601), `--weekday` (0=Mon..6=Sun),
  `--event-name`, and `--patches/--periods/--season-info` path overrides.

## Why the lag is fine

The static JSON only refreshes on Wednesday (`getStaticData`), so a mid-week patch first appears in
`patches.json` the Wednesday after it ships — which is the same day its new gear/talent data lands, so
the intensive `daily` phase kicking in then is well-aligned. `days_since` is still measured from the
real go-live reset (calendar days), so no window is lost to the detection lag.

Related: [[artifact-only-deploy]] (how the built `_site` deploys), [[blizzard-preseason-period]] (the
phantom pre-season period filtered so week 1 == season start), [[chart-theming]] (dashboard patch
annotations that share the snapping logic).
