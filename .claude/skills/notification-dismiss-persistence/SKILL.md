---
name: notification-dismiss-persistence
description: Dismissible site notifications persist per-browser in localStorage, keyed by content, and first-party functional storage is NOT gated behind Klaro consent. Use when touching assets/js/notifications.js, the notification block in templates/notifications.html, the #page-notification styling in responsive.css, or deciding whether client-side preference storage needs a consent check.
---

# Dismissible notification persistence

A notification in `data/static/notifications.json` with `"dismissible": true` renders a Bootstrap
`alert-dismissible` in `templates/notifications.html` (the shared include every page composes).
Without persistence Bootstrap's `data-bs-dismiss="alert"` only removes the DOM node, so the alert
returns on the next page load. `assets/js/notifications.js` makes a dismissal stick across all pages
and reloads.

## How it works
- Each alert carries a stable `data-notification-key` stamped at build time:
  `notification.id or (message ~ '|' ~ link)`. The key is identical on every page, so one dismissal
  hides the notification everywhere. Editing a notification's `message`/`link` (with no `id`) changes
  the key, so a previously-dismissed notification reappears; add an `"id"` to decouple the key from
  the copy.
- The alert also gets an `id="page-notification-{{ loop.index }}"` (unique per notification, so
  multiple notifications do not collide) plus the class `page-notification`.
- `assets/js/notifications.js` reads dismissed keys from `localStorage["mythistone.dismissedNotifications"]`
  (a JSON array), removes already-dismissed alerts on load, and records the key on Bootstrap's
  `close.bs.alert` event (which bubbles to `document`). It **removes** the node rather than setting
  `display:none`, because the alert's `d-flex` uses `display:flex !important`.
- The `<script>` tag lives at the tail of `templates/notifications.html`, right after the markup, as a
  **blocking classic script** (NOT deferred, NOT in `javascript_imports.html`). That DOM position lets
  it delete already-dismissed alerts before first paint, avoiding a flash-then-disappear. `base_template.html`
  is dead code (nothing includes it); wiring a script there loads it on no page.

## Style the alert by CLASS, not id
`assets/css/responsive.css` styles the notification via `.page-notification` (background-image reset,
border, `.btn-close` colour, and the `max-width:575.98px` wrap block). Do **not** use `#page-notification`
— the id is now per-loop (`page-notification-1`, ...) so an id selector matches nothing.

## First-party functional storage is NOT gated behind Klaro consent
Remembering a dismissal is treated as strictly-necessary first-party functional storage, exactly like
the light/dark theme preference (`localStorage['theme']`, written in `material-dashboard.js`, read
inline in `header_imports.html`). It writes to `localStorage` unconditionally, guarded only by
try/catch (private mode / disabled storage degrades gracefully). The Klaro banner governs **third-party
embeds** ([[klaro-consent-embeds]]); it does not gate the site's own preference storage. When adding
client-side preference storage, follow this pattern — do not add a Klaro service or a consent check for it.
