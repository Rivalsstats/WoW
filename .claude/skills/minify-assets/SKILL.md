---
name: minify-assets
description: How first-party CSS/JS assets are served and why the committed *.min files are untrustworthy. Use when editing assets/css/ or assets/js/, wiring a stylesheet or script into a template, or tempted to point a template at a *.min.css / *.min.js variant.
---

# Asset minification and the stale .min trap

Templates reference the **readable, non-minified** first-party source, never the committed
`*.min` variants. Confirmed in `templates/header_imports.html` (`/assets/css/material-dashboard.css`)
and `templates/javascript_imports.html` (`/assets/js/material-dashboard.js`).

Committed first-party `*.min` files still sit in the repo (`assets/css/material-dashboard.min.css`,
`assets/js/material-dashboard.min.js`, `assets/js/material-dashboard.js.map`) but are **unused and
drifted from source**. Verified 2026-07-24 the `.min.css` differed from source by ~20K chars and the
`.min.js` was an older, different build. Do NOT switch a template to one on the assumption it mirrors
source. If you ever must trust a `.min`, prove parity first (strip comments + whitespace, diff).

Vendor libraries shipped pre-minified by upstream are fine as-is: `jquery.min.js`,
`bootstrap.min.js`, `popper.min.js`, `bootstrap-select.min.js`, `dataTables.min.js`,
`chartjs.min.js`, `bootstrap-select.min.css`, `datatables.min.css`. The rule only concerns
first-party assets whose source we edit.

To add a new first-party asset, drop the readable source in `assets/css` or `assets/js` and reference
that source path from the template. Do not commit a pre-minified copy of it.

**Repo drift vs older notes:** a `backend_scripts/minifyAssets.py` plus a "Minify first-party CSS/JS"
step in `.github/workflows/buildPages.yml` existed around 2026-07-24, but both are **gone** from the
current repo. The `assemble` job in `buildPages.yml` now copies `assets` verbatim into `_site`
(`cp -r pages dungeons items classes assets _site/`) with no minification. So deployed first-party
bytes are currently the unminified source. Do not cite `minifyAssets.py` as if it exists.
The "fail loudly" working preference in `AGENTS.md` applies if you re-add a minify step.
