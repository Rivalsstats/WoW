---
name: infinite-scroll-pages
description: How paginated listing pages (blog, items, routes) load more entries via the shared window.MythiInfinite sentinel helper, and the server-render-page-1 + JSON-manifest + shared-card pattern. Use when adding or editing infinite scroll on a listing, touching assets/js/infinite-scroll.js, blog-infinite.js, items.js, route-search.js, generateBlogPage.py, or verifying scroll loading in the Browser pane.
---

# Infinite-scroll listing pages

Listing pages append the next batch of entries as a sentinel near the bottom scrolls into view, with no page-number clicks or "load more" button. Three pages use this: the blog, the items page, and the route finder.

## The shared sentinel helper (`assets/js/infinite-scroll.js`)

`window.MythiInfinite.create({sentinel, onLoadMore, rootMargin})` wires one `IntersectionObserver` (default `rootMargin: "800px"`, so the next batch loads before the visitor reaches the bottom) to a sentinel element. It is loaded site-wide from `templates/javascript_imports.html`, after `deep-link.js`, so any page can use it.

- `onLoadMore()` appends the next batch and returns — or resolves to — `true` when the list is exhausted. The helper then hides the sentinel (`d-none`) and stops firing.
- After each batch the observer is **unobserved then re-observed**, not left running. A plain observer only fires on a state change, so a sentinel that stays on screen (a short list on a tall window) would never load a second batch. Re-observing forces a fresh callback, so loading continues until the sentinel is pushed below the fold or the list ends.
- Returned controller: `reset()` re-arms for a fresh list (new filter/search) and clears the done latch; `finish()` marks the list complete with no further callback (e.g. a search that found nothing); `disconnect()` tears it down. A page that rebuilds its list on filter change renders the first batch itself, then calls `reset()` (more to load) or `finish()` (first batch held everything).

## Server-render page 1, feed the rest from a JSON manifest

The blog is the reference for a page whose data is generated offline. `backend_scripts/generateBlogPage.py` renders **only** `blog.html` with the first `per_page` posts (SEO, first paint, no-JS see real cards) and writes every later post to one manifest, `assets/json/blog_index.json`. No `blog-2.html … blog-N.html` exist. The manifest path is gitignored alongside `items_index.json` / `compRoutes.json` (the `.gitignore` lists these per-file, not a wildcard, so a new manifest must be added there). `assets/js/blog-infinite.js` fetches the manifest once and appends it in batches through `MythiInfinite`.

The items and routes pages hold all their data client-side already: `assets/js/items.js` slices a local filtered array; `assets/js/route-search.js` asks its web worker for the next page (`renderMatches(results, append=true)`) and tracks `loadedCount` against the worker's `total`.

## One card definition for both render paths (DRY)

The Jinja-rendered page-1 cards and the JS-appended cards share a single markup definition. In `blog.html` a `{% macro post_card(post) %}` carries the card, with `data-blog="…"` hooks on every field JS fills. Page 1 loops the macro; a hidden `<template id="blog-card-template">` renders the **same** macro once with a skeleton post (a truthy placeholder image so the image block is present, one placeholder paragraph as a clone prototype). `blog-infinite.js` clones the template content, fills the `data-blog` hooks from the post object (removing the image block when a post has none, cloning the paragraph prototype per paragraph), and appends. Never hand-write card HTML as a string in JS; clone the template so the two paths cannot drift.

Appended nodes miss the one-shot passes in `javascript_imports.html`, so re-init on just the new nodes: run the same `.timestamp` relative-time fill (using the global `timeAgo`) and `bootstrap.Tooltip.getOrCreateInstance` for `[data-bs-toggle="tooltip"]`.

## Verifying infinite scroll in the Browser pane

The Browser pane keeps its tabs in `visibilityState: hidden` with the rendering lifecycle paused, so `requestAnimationFrame` and `IntersectionObserver` never fire on scroll in-pane. A real scroll gesture cannot demonstrate loading there. Verify by driving the real `onLoadMore`/append code path directly instead:

- Routes exposes top-level `loadMoreRoutes()` / `doQuery()` / `loadedCount` / `lastResults`; call `loadMoreRoutes()` repeatedly and assert the accordion and summary grow and terminate.
- Items' `renderMore` first-batch + filter-change `reset()`/`finish()` are reachable by dispatching input/change events and reading the grid and sentinel.
- For a closure-only page like the blog, stub `window.IntersectionObserver` with one that calls its callback synchronously on `observe()`, then re-run the page script's source; this drives the real `MythiInfinite` loop and the real `buildCard`. Restore the real observer afterward.

See [[local-test-render]] for seeding and serving on 8099, [[minify-assets]] for how first-party JS is served, [[deep-links]] for the sentinel-adjacent share/scroll behavior, and [[artifact-only-deploy]] for why generated manifests stay gitignored.
