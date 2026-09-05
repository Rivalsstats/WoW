---
name: tierlist-gear-modal
description: The sim tierlist's per-bar gear/talents modal is client-rendered from a tierlist_gear.json emitted by the profile builder and reuses the analyzer's shipped catalogs. Use when editing the Popular/SimC BIS/Top 50 bar modal on the tierlist, assets/js/tierlist-modal.js, the tierlist_gear.json emit in generateSimcProfiles.py, the sim gearset matrix in buildPages.yml, or its assemble-job staging.
---

# Sim tierlist gear/talents modal

Each DPS bar on the sim tierlist (`templates/simc_tierlist.html`) is a keyboard-accessible button (`gear_bar` macro, `data-gear-open`) that opens one shared `#gearModal`. The modal shows the exact gear (spec-page double-column armory layout) and talents (positioned tree) that gear set was simmed with. It is rendered entirely CLIENT-SIDE by `assets/js/tierlist-modal.js`.

## The three gear sets

`GEAR_SETS` in `generateSimcProfiles.py` and `generateTierlistPage.py` (and the `gearset` matrix in `buildPages.yml`'s `simulate` job) list three sets, all built by `build_profiles`:

- `popular` — the spec-page baseline set (`simcBis._prepare_spec`: most-popular per-slot items + most-popular talent code).
- `simcbis` — the collector's persisted rank-1 per-slot Top-Gear set (`_simcbis_gear`), worn with the popular talents.
- `top50` — the top-50 verified players' actual loadout (`_top50_gear` + `_top50_talents`): the per-slot most-common equipped item (with its bonus ids) from `top_player_loadout_items`, worn with the players' most-common REAL Blizzard v2 export string. That string is `top_player_loadouts.loadout_text` — the genuine in-game code the players used, which the collector captures verbatim from raider.io (`characterDetails.character.talentLoadout.loadoutText`) and `fetch_top50_loadouts` surfaces in each entry's `meta`. `_top50_talents(loadouts)` just picks the MOST COMMON non-empty `loadout_text` (deterministic tie-break by the string) and returns it verbatim. Real in-game strings are exactly what simc and the game accept, so there is NO encoder, no per-node reconstruction, no choice-node bug and no committed-data-vs-simc skew (the same reason `popular`/`simcbis` sim from their real stored codes). It is NOT `loadout_key` — that is a separate synthetic collector token (`logged-mplus__<id>`, from `chosen.optionKey`), not a talent code. `_top50_gear` + `_top50_talents` are wrapped together in `build_profiles` in try/except so a top50-only failure records a per-spec manifest skip instead of aborting the whole profile build; top50 is also skipped (no bar) when no `loadout_text` is stored yet, so it degrades gracefully until the collector repopulates the column (the collector DELETEs+re-INSERTs `top_player_loadouts` every run). The per-node `top_player_loadout_talents` rows are still written and drive the spec page's top-50 node-usage stats; they are no longer used to build the tierlist talent code. Enchants and gems are NOT stored per slot in the top-50 tables (gems are a per-player `{gem_item_id, usage_count}` bag), so `top50` fills them the same way `popular`/`simcbis` do: the top-50 `enchant_map` + `gem_ranking` from `_prepare_spec`, applied over the equipped set by socket budget (`apply_enchants_and_gems`).

Adding a set touches every place the set list lives: `GEAR_SETS` in both generators, the `gearset` matrix in `buildPages.yml` (one `.simc` file per set per target count, so N sets x 4 targets sim legs), `build_mock_results` (the `--debug` preview), and the methodology copy in the template. The `gear_bar` macro and the desktop/mobile bar loops iterate `row.bars` (one per gear set) so a new bar renders automatically. `ACTOR_RE`, `parse_results`, `build_tab` ranking (`primary` = best of the sets) and `write_simdps_artifact` already generalize over any number of sets. The OG preview (`image_generation/tierlist_preview.py`) draws only ONE bar per row (the primary DPS), so extra gear sets do not change it, and `tierlist_card.py` is an unrelated index-style card. The Discord `simdps_tierlist.json` artifact carries DPS rows only (no gear sets), so it is unaffected.

## Why client-side, and the no-DB constraint

`generateTierlistPage.py` runs in the credential-free `assemble` job (jinja2 + pillow only, no DB, no `item_lookup`). It is NOT involved in the modal. Instead:

- `generateSimcProfiles.py` (the DB-having `prepare-sims` job that builds every gear set) emits `tierlist_gear.json` into its `--output_dir` next to `manifest.json`, keyed `{"<specId>": {"popular": {talents, slots}, "simcbis": {...}, "top50": {...}}}`. Each slot is `{id, name, icon, quality, bonus[], enchant?, gems?[]}` with item icon/name/rarity resolved server-side (uses `_gear_display`/`_resolve_quality` + `bonus_quality_map.json`); `talents` is a Blizzard v2 loadout code (`popular`/`simcbis` carry the popular code from `fetch_top_loadout`; `top50` carries the real `loadout_text` `_top50_talents` picks from the stored top-50 strings). `build_profiles` returns a 3-tuple that includes this `gear_data`.
- The `assemble` job in `buildPages.yml` downloads the existing `simc-profiles` artifact and copies `tierlist_gear.json` into `assets/json/` before `assets` is staged into `_site` and minified. No new artifact is created; the file rides the profiles artifact.

## What the modal reuses (do not duplicate)

`tierlist-modal.js` lazy-fetches and renders against the SAME shipped catalogs the analyzer uses, plus the spec-page CSS (`stat-colors.css` + `spec-page.css`, `.tt-*` / `border-quality-*`):

- `assets/json/tierlist_gear.json` — the per-spec gear/talent data above.
- `assets/json/talent_trees/<spec>.json` — tree geometry (`fullNodeOrder` + positioned nodes).
- `assets/json/gem_enchant_index.json` — enchant/gem icons + links.
- `assets/json/items_index.json` — `/items` slugs for item links.

These catalogs are baked by the analyzer/spec-page generators, so the tierlist modal depends on those pages being built in the same run. The ~40-line Blizzard v2 loadout decoder in `tierlist-modal.js` deliberately mirrors `decodeLoadout()` in `analyzer.js` (kept separate because the analyzer renderer is entangled with its meta-comparison state). A missing `tierlist_gear.json` (404) is treated as empty: the modal shows a per-spec "no gear recorded" notice rather than erroring, so the `--debug` template-preview path (which fabricates only bars) degrades gracefully.

The baked `talent_trees/<spec>.json` already has non-existent nodes filtered out of its `nodes` dict (via `commonUtils.filter_talent_tree_nodes` in `generateAnalyzerPage.write_talent_trees`, see [[analyzer-page]]), so the modal draws the same tree the spec page does. `fullNodeOrder` stays COMPLETE there for decode alignment; `ttNode()` returns null for a node absent from `nodes` and the render skips it, so no per-consumer JS filter is needed.

## Deep link (`#gearModal&gear=<specId>-<gearset>`)

The modal is deep-linkable through `window.MythiLink` (see [[deep-links]]), mirroring the comps page Details modal. `tierlist-modal.js` registers a `gear` state (`read` returns `<specId>-<gearset>` while open, `null` otherwise; `apply` finds a matching `[data-gear-open]` button and opens it) and tracks `openGear` on open / `hidden.bs.modal` on close, calling `MythiLink.sync()` each time. The modal keeps its `#gearModal` element target (so the auto-injected modal-header copy-link button works), giving the shared permalink `#gearModal&gear=<specId>-<gearset>`. `apply` uses `document.querySelector('[data-gear-open][data-spec-id=..][data-gearset=..]')`, which matches a button in ANY target-count tab (the same spec+gearset button exists in every tab), so a fresh load opens the right modal regardless of which tab is active. Scroll-to on load is instant via deep-link.js's `#gearModal` target reveal.

See [[analyzer-page]] for the catalog origins and decoder, [[deep-links]] for the permalink system, [[item-quality-from-bonus]] for rarity-from-bonus, [[minify-assets]] for asset wiring, [[bootstrap-capture-phase]] for bars nested in interactive controls.
