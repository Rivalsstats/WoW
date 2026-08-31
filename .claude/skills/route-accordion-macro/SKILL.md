---
name: route-accordion-macro
description: The keystone.guru route accordion item is one shared Jinja macro used by the spec, routes and dungeon pages, and assets/js/route-search.js must mirror its full markup. Use when editing templates/_route_macros.html, the route panels on spec_page.html / find_routes.html / dungeon_page.html, or route-search.js.
---

# Shared route accordion macro

`templates/_route_macros.html` `route_accordion_item(...)` renders the WHOLE keystone.guru route
accordion item (header button plus consent-gated embed body) and is the single source of that markup
for three pages:

- `spec_page.html` Top-Route modal (`top_routes[ds]`; passes `upgrade_css`/`upgrade_text` from
  `upgrade_info` so the modal keeps its chest "++N" badge instead of the plain green "+level").
- `find_routes.html` server render (first run per dungeon from `comp_routes_by_dungeon`).
- `dungeon_page.html` "Top Routes" card (`top_routes`; passes `iframe_height='60vh'` and
  `item_class='border'`).

The body is a single left-aligned "View full run details on Raider.io" button (Raider.IO logo +
`open_in_new`) over the iframe. All three pages keep the accordion parent id `#routeDungeonAccordion`
and a `shown.bs.collapse` hook calling `MythiConsent.loadEmbed(iframe[data-src])`; the iframe carries
`data-name="keystoneGuru"` + `data-src` and is never given `src` directly (see [[klaro-consent-embeds]]).
Permalinks use `data-share-id="route-<slug>-<route_key>"` (the run id in the element id changes each
refresh, so [[deep-links]] prefers the share id).

## route-search.js mirrors the macro

The route finder rebuilds results client-side, and Klaro cannot gate client-added iframes, so
`renderMatches()` in `assets/js/route-search.js` builds the SAME accordion-item markup as the macro
(header + body) and wires consent per iframe itself. Any change to the macro's markup must be mirrored
there; both sides carry a KEEP-IN-SYNC comment. The client path never has an `upgrade_text`, so it
always emits the plain green "+level" badge. It also refills each new `.timestamp[data-timestamp]` span
the same way the one-shot init in `javascript_imports.html` does, because that init has already run
before rows are appended.

## Raider.IO run permalink season slug

Run links key on the season slug. `find_routes.html` injects `window.current_season =
season_info.slug` (consumed by route-search.js). `season_info` has no `current_season` key, so reading
that instead silently yields empty and breaks the client-rendered `raider.io/mythic-plus-runs//<id>`
links.
