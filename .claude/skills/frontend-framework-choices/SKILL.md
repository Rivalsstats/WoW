---
name: frontend-framework-choices
description: Which frontend library to reach for per concern (modals, tables, selects, charts, search, consent) and the no-build-step conventions. Use when adding client-side UI to a page, picking a widget library, or wondering why jQuery is present.
---

# Frontend framework choices

The UI is **Material Dashboard 3 v3.2.0** (see the header comment in
`templates/base_template.html`) on **Bootstrap 5.3.3** (see the header comment in
`assets/css/material-dashboard.css`). There is **no build step**: plain `<script>` includes,
vanilla JS, no bundler or framework.

Pick by concern:
- **Modals, dropdowns, collapse, tabs, tooltips** to Bootstrap 5 via `data-bs-*` attributes.
  Tooltips auto-init in `material-dashboard.js`; do not hand-roll these.
- **Selects** to bootstrap-select (`plugins/bootstrap-select.min.js` + `plugins/bootstrap-select.min.css`), which adds search, multi-select, and icon support. Include per-page that uses it.
- **Icon multi-selects** to bootstrap-select (`.selectpicker`, `data-content` for icon markup).
  Loaded globally in `templates/javascript_imports.html`.
- **Sortable / searchable tables** to DataTables (Bootstrap 5 build). Include
  `plugins/dataTables.min.js` + `datatables.min.css` per page that uses it.
- **Charts** to Chart.js v4 with `plugins/chartjs.min.js`; theme chrome via [[chart-theming]].
- **Fuzzy search** to Fuse.js; `site-search.js` is the global site search built on it.
- **Consent-gated embeds** to Klaro via the `MythiConsent` helper (`assets/js/consent.js`), because
  Klaro only manages elements present at init. See [[klaro-consent-embeds]].

**Conventions.** App singletons use an IIFE namespace on `window.Mythi*` (e.g. `window.MythiChart`,
`window.MythiConsent`, `window.MythiLink`). jQuery IS present (`core/jquery.min.js`, required by
bootstrap-select) but app code is otherwise vanilla, so do not write new jQuery-dependent logic.
Off-main-thread comp/route computation runs in the web worker
`assets/js/comp-routes-worker.js`. See [[build-new-page]], [[frontend-design-tokens]].
