---
name: artifact-only-deploy
description: Built pages are never committed; the site deploys straight from the Pages artifact and generated paths are gitignored. Use when adding a generator, wiring output dirs, reasoning about deployment, or wondering why old commit hashes or committed HTML are missing.
---

# Artifact-only deploy, no committed build output

The pipeline is artifact-only. `.github/workflows/buildPages.yml` assembles
`_site/` in its `assemble` job and uploads it directly via `actions/upload-pages-artifact@v5`.
Nothing generated is committed. 

Generated output paths are gitignored. `.gitignore` ignores `/pages/`, `/classes/`, `/dungeons/`,
`/items/`, `assets/img/previews/`, and the volatile JSON (`assets/json/search_index.json`,
`items_index.json`, `compRoutes.json`, `comps_index.json`, `assets/json/item_icons/`). Never
`git add` generated pages or these JSON files.

**Fresh checkouts do not contain output dirs.** A CI run starts clean, so every generator must
`os.makedirs(..., exist_ok=True)` its own output directory before writing. Do not assume `pages/`,
`dungeons/`, etc. already exist.

A cold-storage per-season copy of the built site is handled separately (see
[[season-snapshot-archive]]).
