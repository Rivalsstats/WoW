---
name: spec-team-comps
description: Covers the spec page "Popular Team Comps" card and its archetype ranking. Use when editing backend_scripts/compArchetypes.py, generateSpecPages.py, templates/_team_comp_macros.html, or the comps-page Details modal in templates/comps.html.
---

# Spec Page "Popular Team Comps"

The spec page "Popular Team Comps" card (`templates/spec_page.html`, macro `comp_slots` in `templates/_team_comp_macros.html`) MUST only show comps that VISIBLY contain the spec. `comp_slots` renders a comp's 5 main spec icons, so a flex-only spec would be invisible (buried in the hover popup).

Logic lives in `backend_scripts/compArchetypes.py`, consumed by `generateSpecPages.py`:
- `spec_team_comps(families, spec_lookup, class_lookup, limit=2)` returns `{spec_id: [view]}`, ranking families by the highest key the spec REACHED (tie-break own play). When the spec is only a flex alternate it returns a `_swap_view` copy that places the spec into its slot. It NEVER mutates the shared family dict (one family surfaces under many specs).
- `top_comps_with_spec(collapsed_comps, spec_id, ...)` is the fallback to highest-key raw comps when the spec belongs to no family (sub-1% flex filtered out by `min_alt_frac`). Used when `spec_team_comps` is empty so the panel is never wrongly empty; a truly-absent spec yields empty and the panel hides.

## Key-based ranking invariant
- Archetypes order by HIGHEST KEY DONE, not popularity. The displayed core (rep) of a family is the member with the best `_core_rank`; per-slot alternates sort by `(max_key, runs)`.
- INVARIANT: `group_leader` SEED order and `_slot_alternates` REP pick MUST use the SAME `_core_rank(c) = (c.runs >= MIN_CORE_RUNS, c.max_key, c.weight)`. This keeps each family's core == its seed so cores stay >=2 swaps apart. If you change how the core is chosen, change the seed order to match or near-duplicate cores return in Hidden Gems. `MIN_CORE_RUNS` (=5) stops a fluke one-off key defining a core.

## Details modal = family data
- The comps-page Details modal (`showCompDetailsByC` → `showCompDetails` in `templates/comps.html`) must show FAMILY totals, not the single core comp's runs. `_family` emits merged per-dungeon `dungeons` ({t,d,runs,mk}), `bd`, `bdr` for the ctx=None ('all') pass only. `attachSlots` builds the whole comp object from the archetype, falling back to the raw comp only when the key heads no visible family. Needs a rebuild of `comp_archetypes.json` to appear.

`build_archetypes`/`spec_team_comps`/`top_comps_with_spec` are spec-page-only; the comps page uses `build_dungeon_archetypes`. The old `families_by_spec` was removed. See [[frontend-json-dungeon-keys]], [[top-trends-bar]].
