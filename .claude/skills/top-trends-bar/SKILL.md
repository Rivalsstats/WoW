---
name: top-trends-bar
description: Covers the site-wide "Top Trends" bar: weekly trend_snapshot baseline, the Python snapshotTrends writer, per-page contextual feeds, and the marquee JS. Use when editing snapshotTrends.py, pageGeneration.build_trends/build_global_trends, templates/trends_bar.html, assets/js/trends-bar.js, or the trend_snapshot table.
---

# Site-Wide "Top Trends" Bar

The bar shows entities that moved up/down since the previous reset week (Blizzard period), with a tier/rank arrow + popularity % delta. Contextual per page: index = spec+dungeon global; spec pages = that spec's talents/items-per-slot/embellishments/gems/crafted/combos; comps + dungeon pages = team-comp ARCHETYPE movement. Pages that pass no `trends` self-hide the partial.

## Data path
- `trend_snapshot` table (`backend_scripts/database.sql`): weekly per-entity freeze, bounded top-N. Cleared in `sp_season_wipe`.
- `backend_scripts/snapshotTrends.py`: a PYTHON CI step run BEFORE the generators, not a stored proc, because S–F tiers come from `tierMath.ckmeans` (Python-only). Each build it (a) writes current live records to a build-local JSON `TRENDS_LIVE_PATH` (default `assets/json/trends_live.json`), the fresh "now" side, NEVER in the DB; and (b) stores a write-once weekly baseline in the DB (guarded by `fetch_trend_week_exists`, frozen at the period's first build). `build_trends` reads "now" from the JSON and "prev" = previous period baseline (`fetch_prev_trend_week`). So the displayed % is fresh and the delta spans a full period. `--force` re-stores baseline; `--debug-fake-live` fakes ONLY the live JSON so the bar renders locally with no fake DB rows. Run snapshotTrends BEFORE the generators or the live JSON is absent and the bar hides.
- Archetype feed: `snapshotTrends` runs `compArchetypes.collapse_comps` + `build_dungeon_archetypes` off one `fetch_all_comps` scan. IDENTITY GOTCHA: the displayed core `c` is the highest-KEY member and flips whenever a sister comp posts a fresh top key. `_family` also exposes `key_c` (most-PLAYED member); the snapshot uses `key_c` as the stable `entity_key` and displayed `c` as the `label`.
- `pageGeneration.build_trends()` diffs the latest two snapshot weeks into movers, resolving icon/href from the same lookups pages already load. It opens its OWN tuple cursor because some generators (dungeon) pass a `dictionary=True` cursor.
- `pageGeneration.build_global_trends()` (own conn + lookups, cached, returns [] with no DB pool) feeds dashboard / routes / analyzer / tierlist / items. index/spec/dungeon/comps use their contextual feeds.
- `templates/trends_bar.html` is included after `notifications.html` in all templates. CI has a "Snapshot weekly trends" step in `buildPages.yml` before the generators.
- Bar is an icon-only marquee driven by `assets/js/trends-bar.js` (registered in `javascript_imports.html`): it clones the single `.trends-seq` to overfill the viewport and shifts by exactly one seq width for a seamless loop.

OPERATIONAL: the bar renders NOTHING until two weekly snapshots exist (~1 week into a season). Verified via ephemeral mysql:8 Docker + a monkeypatch harness. Archetype identity ties into [[spec-team-comps]] and [[frontend-json-dungeon-keys]].
