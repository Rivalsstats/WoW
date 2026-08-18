---
name: bootstrap-capture-phase
description: Bootstrap 5 data-api handlers run at document capture phase, so stopPropagation on a link inside an accordion button is useless; use a window-capture shield. Use when a link nested inside a [data-bs-toggle] control gets swallowed / preventDefault'd instead of navigating.
---

# Bootstrap 5 Capture-Phase data-api

Bootstrap 5's `EventHandler` passes the delegation flag as `addEventListener`'s third argument, so all data-api handlers (e.g. collapse toggling on `[data-bs-toggle="collapse"]`) run on `document` in the CAPTURE phase.

A click on an `<a>` nested inside an accordion-header `<button>` gets toggled and `preventDefault()`ed by Bootstrap BEFORE the anchor's own `onclick` fires. `onclick="event.stopPropagation()"` on the link cannot prevent it (the old "Item details" chip on spec-page header rows had this latent bug).

WHY: capture on `document` beats everything except capture on `window`.

HOW to apply: register a `window`-level capture listener (any load order) that calls `e.stopPropagation()` for clicks on the link. Bootstrap never sees the event, while the anchor's native navigation and ctrl/middle-click behavior proceed. Implemented in `templates/spec_page.html` (inline script after `javascript_imports.html`) for `a.item-name-link` inside `button.accordion-button`.

These accordion controls are also the reveal surface for [[deep-links]] and [[klaro-consent-embeds]].
