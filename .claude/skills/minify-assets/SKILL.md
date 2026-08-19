---
name: minify-assets
description: How first-party CSS/JS assets are served and minified. Use when editing assets/css/ or assets/js/, wiring a stylesheet or script into a template, or tempted to point a template at a *.min.css / *.min.js variant.
---

# Asset serving and minification

Templates reference the **readable, non-minified** first-party source, never a `*.min` variant
(for example `templates/header_imports.html` loads `/assets/css/material-dashboard.css` and
`templates/javascript_imports.html` loads `/assets/js/material-dashboard.js`). There are no committed
first-party `*.min` files in the repo, so do not point a template at one.

**Build-time minification:** `backend_scripts/minifyAssets.py` minifies first-party CSS/JS **in place
inside `_site`** during the `assemble` job in `.github/workflows/buildPages.yml`, right after the
`cp -r … assets _site/` step. It uses `rcssmin` + `rjsmin` (pure-Python, in that job's `pip install`),
walks `_site/assets` for `*.css`/`*.js`, and **skips** anything ending in `.min.css`/`.min.js` (upstream
vendor libs) or `.map`. Filenames are unchanged, so templates keep referencing the readable source path
and only the deployed bytes are minified. Because this runs on the ephemeral `_site` tree, no minified
first-party files are committed and committed source stays readable. Per the "fail loudly" preference in
`AGENTS.md`, any read/minify/write error aborts the run with a non-zero exit.

Vendor libraries shipped pre-minified by upstream are committed as-is and served directly:
`jquery.min.js`, `bootstrap.min.js`, `popper.min.js`, `bootstrap-select.min.js`, `dataTables.min.js`,
`chartjs.min.js`, `bootstrap-select.min.css`, `datatables.min.css`. The build-time minifier skips them.

To add a new first-party asset, drop the readable source in `assets/css` or `assets/js` and reference
that source path from the template. Do not commit a pre-minified copy of it.
