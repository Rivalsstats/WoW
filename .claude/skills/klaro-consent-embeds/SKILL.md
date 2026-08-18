---
name: klaro-consent-embeds
description: Klaro only manages embeds present at its init, so client-rendered keystone.guru iframes must gate consent themselves via MythiConsent.loadEmbed. Use when adding or debugging a dynamically rendered iframe embed, editing assets/js/consent.js, route-search.js, or the embed flow on spec/dungeon/routes pages.
---

# Klaro + Dynamic Embeds

Klaro swaps `data-src` → `src` only for elements that existed when it initialised. `klaro.getManager().applyConsents()` does NOT pick up an iframe inserted later (a freshly rendered route-result iframe stayed empty even with consent granted). Any page that renders keystone.guru embeds client-side must set `src` itself and therefore check consent itself.

All four surfaces (spec page, dungeon page, `find_routes.html`, `route-search.js`) go through `MythiConsent.loadEmbed(iframe)` in `assets/js/consent.js` on `shown.bs.collapse`. It owns the whole flow: consent check, deferring until granted, setting `src`, the `.iframe-spinner`, and a stand-in notice.

## Hard rules
- NEVER set `src` on an embed Klaro manages. Klaro re-enables one by cloning it, but `updateServiceElements` bails early on `consent && element.src === data-src`. Setting `src` first kills the swap and the embed loads at `display:none`. `loadEmbed` detects Klaro-managed elements via `data-modified-by-klaro` / a `data-type="placeholder"` sibling and then only drives the spinner.
- NEVER put `src=""` on these iframes. The browser loads the page's own URL into every one. Use `data-src` alone.
- NEVER test loaded-ness with the `.src` property. It resolves `src=""` to the document's own URL so it is never empty. Use `getAttribute('src')`. This bug blanked every route panel on the spec page.
- The spinner must stay hidden until a load is actually in flight, otherwise it spins on top of Klaro's "enable external content" notice.
- Klaro's accept-once ("Yes") path forgets to restore `display` on the embed the visitor clicked; `followKlaro` repairs that from `data-original-display`.
- `loadEmbed` renders its own notice (with a `klaro.show()` button) only when Klaro has not already put one there, so server-rendered embeds do not get two.
- `klaro.renderContextualConsentNotices()` is NOT usable for dynamic elements (5 positional args including internals, and it blanks `src` + sets `display:none`).
- `whenGranted` runs a watcher AND a poll on purpose: Klaro's watcher payload shape varies by version and can silently never match, while the poll alone misses the case where Klaro has not initialised yet.

Embeds are typically revealed by [[deep-links]] on `shown.bs.collapse`.
