---
name: analyzer-page
description: Covers the "Am I Meta?" analyzer page: client-side talent decode/compare and the Gear/Talents card sync. Use when editing assets/js/analyzer.js, generateAnalyzerPage.py, assets/css/analyzer.css, or debugging talent decode, hero-tree rendering, or the gear/talents narrow-layout flip.
---

# Analyzer Page (Am I Meta?)

The analyzer (`assets/js/analyzer.js`, page built by `backend_scripts/generateAnalyzerPage.py`) decodes an in-game export string (created by the simulationcraft addon https://github.com/simulationcraft/simc-addon) client-side and scores the character against the meta.

## Talent decode + tree data
- `decodeLoadout()` parses the export's `talents=` string as Blizzard "serialization version 2" bitstream (charset `A-Za-z0-9+/`, 6 bits per char LSB-first; header = 8-bit version + 16-bit specId + 128-bit tree hash ignored; then per node in `fullNodeOrder`: selected bit, purchased bit, optional 6-bit rank, optional 2-bit choice index). A version guard fails loudly if Blizzard bumps the format.
- Tree GEOMETRY (`fullNodeOrder` + positioned nodes with x/y) ships DECOUPLED in `assets/json/talent_trees/<spec>.json`, baked by `generateAnalyzerPage.write_talent_trees()` from `data/static/talents/*.json`. `spec_meta.talents` carries ONLY meta loadouts (`meta_by_hero`, `popular_hero`) plus a `node_pct` pick-rate map from `build_talent_meta`/`build_ui_tree` in `generateSpecPages.py`. `buildTalentTree` in analyzer.js fetches BOTH and merges them.
- WHY decoupled: `processTalents.py` (emits geometry) runs in the WEEKLY `getStaticData.yml`; `generateSpecPages` bakes `spec_meta` in the frequent `buildPages.yml`. Geometry baked into `spec_meta` went stale and broke the tree. Build-order dep: committed `data/static/talents/*.json` must carry `fullNodeOrder`+nodes+x/y or `write_talent_trees` skips the spec and the client falls back to a flat chip list.
- Scoring: `talentDiff` (pure decode+diff) is split from `talentCardHtml` (view) so `combineScore`/`matchRings` can read the score; Overall ring = gear and talents averaged 50/50. `heroColumn` renders the hero switch and rewrites the Talents + Overall rings on switch.
- `pages/` is gitignored build output. After template/CSS changes rerun `generateAnalyzerPage.py` and hard-reload.

## Gear/Talents card sync (accepted tradeoff, do NOT "fix")
- Gear (`col-xl-5`) and Talents (`col-xl-7`) must flip to narrow layout at the SAME width. Per-card container queries can't fire together on an unequal 5/7 split, so `assets/css/analyzer.css` drives BOTH off the single shared container `#analyzer-results` (`container-name: an-results`), stacking at `@container an-results (max-width: 1260px)`. `.an-col` is `flex: 1 1 0`.
- Consequence: the fully-wide state only appears on large monitors (results ≥1260px). This is an accepted tradeoff; the 6/6 split and lower thresholds are deliberately not used. Do NOT re-pitch this as a bug.

Discord `/analyze` deliberately does NOT decode talents; it links to the website. See [[top-trends-bar]], [[spec-page-performance]].
