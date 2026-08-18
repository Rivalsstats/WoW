---
name: deep-links
description: Covers site-wide \#fragment deep links, copy-link buttons, and the instant-scroll workaround in assets/js/deep-link.js. Use when making a modal/accordion/tab addressable, registering page state via window.MythiLink, or debugging a programmatic scroll that does not land.
---

# Deep-Link Permalinks + Instant Scroll

`assets/js/deep-link.js` (loaded for every page from `templates/javascript_imports.html`, right after `bootstrap.min.js`) makes modals/accordions/tabs addressable as `#<elementId>` plus optional `&key=value` state, keeps the hash in sync with `replaceState`, and injects the copy-link buttons. No template markup carries the buttons.

## Registering state
- Extra non-container state goes through `MythiLink.registerState(key, {read, apply})`. `read()` must return `null` at the page default so an untouched page keeps a bare URL. Non-Bootstrap containers use `registerRevealer` + `notifyShown`.
- Registration MUST happen before boot, which is one macrotask after `DOMContentLoaded`. A script block placed ABOVE the `javascript_imports.html` include (e.g. `dungeon_page.html`'s run-panel toggle) has no `window.MythiLink` yet and must register inside a `DOMContentLoaded` listener.
- Panels whose id embeds a `run_id` (dungeon page + routes results) carry `data-share-id` WITHOUT it. `deep-link.js` prefers that attribute so stale links no-op instead of opening the wrong route.
- Reveal always goes through the Bootstrap API. The spec page hydrates per-dungeon talent trees on `show.bs.collapse` and sets the keystone.guru iframe src on `shown.bs.collapse`.

## Scroll must be instant (animation never lands)
- `<html>` carries `scroll-behavior: smooth`, so `scrollIntoView({behavior: "auto"})` animates. On these pages the animation is cancelled and the page stays put (measured: `'smooth'` and `'auto'` both left `scrollY` at 0, `'instant'` landed exactly).
- WHY: pages ship hundreds of un-sized icons; anything above the target reflows while the scroll is in flight and the browser abandons it.
- HOW: any programmatic scroll must pass `behavior: "instant"`. If it runs before `readyState === "complete"` it should re-assert the position a few times as late images arrive (`settleInView` in `deep-link.js`), cancelling the re-assert on the first real user scroll input.

Dynamic keystone.guru embeds revealed by these deep links still need consent gating, see [[klaro-consent-embeds]]. Anchors inside accordion buttons need the capture shield in [[bootstrap-capture-phase]].
