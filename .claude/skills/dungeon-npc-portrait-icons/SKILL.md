---
name: dungeon-npc-portrait-icons
description: How the dungeon page's per-NPC portrait icons are sourced — MDT static Lua for npc_id→displayId, then Wowhead zamimg webthumbs for the image (100% coverage). Use when touching fetchNpcIcons.py, the npc icons on the Least Played / Most Lusted cards, or choosing a creature-image source.
---

# Dungeon NPC Portrait Icons

`backend_scripts/fetchNpcIcons.py` builds per-NPC portrait icons for the dungeon page in
two hops, both over CDNs that are NOT Cloudflare-blocked server-side:

1. **npc_id → displayId** from **MDT static Lua** (`github.com/Nnoggie/MythicDungeonTools`).
   Each current dungeon file `<Expansion>/<Dungeon>.lua` (e.g. `Midnight/KingsRest.lua`)
   ships `["id"]` + `["displayId"]` per enemy — ~100% of current npcs, read off
   `raw.githubusercontent.com`. Detect the active expansion folder as a top-level dir
   having BOTH a `load_*.xml` AND a `Textures/` subfolder (uniquely `Midnight` today;
   `libs`/`Modules` have the xml but no Textures). Parse each `["id"]` with the
   `["displayId"]` that appears before the next `["id"]`.
2. **displayId → image** from **Wowhead zamimg webthumbs**:
   `https://wow.zamimg.com/modelviewer/live/webthumbs/npc/{displayId % 256}/{displayId}.png`
   — Wowhead's own offline model renders, 300×300 transparent PNG, **100% coverage** of
   current displayIds (incl. the newest creatures). The model fills only a median ~43% of
   that frame, so `fetchNpcIcons.py` **trims to the alpha bounding box** (Pillow) before
   saving to `data/icons/npc_<npc_id>.png` (keyed by npc_id) — otherwise the model looks
   tiny in a small icon.

Rendered in `templates/dungeon_page.html` on the "Least Played NPCs" (skip) and "Most
Lusted Pulls" cards, gated on `skip.npc_id|string in npc_icons` — a set the generator
builds by globbing `data/icons/npc_*.png`. Style: `object-fit:contain` (so the trimmed,
non-square model is never cropped), no border/rounding — ~44px in the skip table, ~30px in
the lust list. In the lust card, **bosses are indicated by the orange name only** (no skull
glyph — it looked bad). Missing icon → text-only fallback. Pillow is in the
`getStaticData.yml` pip install.

**Icon hover tooltips** (both cards): a **boss** icon carries `data-wowhead="npc=<id>"` →
the Wowhead npc tooltip (same consent-gated `power.js` path as the name links; `is_boss` =
`npc_id|int in bosses`). A **non-boss** icon carries a Bootstrap HTML tooltip
(`data-bs-toggle="tooltip" data-bs-html="true" data-bs-title="<img src=... width='150'>"`)
that shows the model bigger on hover. Bootstrap tooltips auto-init in `material-dashboard.js`;
its default sanitizer keeps `<img src width>` (so no `style` — size via the `width` attr,
which preserves the trimmed image's aspect).

## Image sources that do NOT work (don't switch back)
- **render.worldofwarcraft.com/.../creature-display-{id}.jpg** (Blizzard official): only
  ~44% coverage — 403s the newest creatures. This is what made an earlier attempt look
  broken (~90% missing). Do not use it.
- **Blizzard creature-display media API**: same render pool, same gap.
- **Wowhead NPC-page scrape** for displayId (keystone's `linksButton.dataset.displayId =`
  token): Cloudflare **403** from datacenter/CI IPs. **old.wow.tools** creature_api: dead.
  Use MDT instead.
- **keystone.guru portraits**: not in their repo; behind a `ksgAsset()`-hashed CDN;
  hotlinking is ToS-sensitive.

Note: zamimg webthumbs are Wowhead's rendered assets (over Blizzard's models); self-hosting
them is a mild ToS/etiquette gray area, accepted here for the 100% coverage. Recreating the
render ourselves (headless zamimg modelviewer / wow.export batch + CASC-extracted assets) is
possible but a heavy, brittle sub-project — unnecessary while Wowhead publishes the thumbs.

## Pipeline / local-test order (important)
CI (`getStaticData.yml`): `fetchNpcInfo.py` (builds `npcs.json`) → `fetchNpcIcons.py` (icons
for that set) → commit `data/`. Locally the seeder samples skip/pull npc_ids from
`npcs.json` keys (`localDev/seeders.py`), so **after seeding** you MUST run `fetchNpcInfo.py`
**then** `fetchNpcIcons.py`, in that order, before rendering — otherwise the icon set won't
match the npc_ids on the page and it looks icon-less (a red herring that is NOT the source
being wrong).

## On-demand self-heal in the dungeon generator
`npcs.json` and the icons are refreshed only by the weekly `getStaticData.yml`, keyed off the
npc ids the DB has recorded in pulls. In week 1 of a season that weekly run can fire before the
collector has recorded any routes, so it writes an empty/partial set — then the daily page build
references ids that have no name (which used to be a hard `ValueError`) or no icon. So
`generateDungeonPages.py` self-heals per dungeon: for the ids the Most Lusted / Least Played cards
reference, it fetches missing names from the live Wowhead `npc-names` dataset (rewriting
`npcs.json`) and missing portraits via the `fetchNpcIcons` pipeline (`build_display_map(None)` →
webthumb), adding them to the in-memory icon set. Both are gated on a missing set, so in steady
state (populated `npcs.json` + present icons) neither fires and there is zero extra network. It
imports `fetchNpcIcons` lazily and needs `aiohttp` in the build env (added to `buildPages.yml`).

Name and icon self-heal diverge on failure. A name id that is STILL unresolved after the
on-demand fetch (absent from both `npcs.json` and the live Wowhead npc-names dataset) is a genuine
data gap, not the week-1 race, so the generator hard-fails the build with a `ValueError` naming the
dungeon and the unresolved ids — self-heal removes the ordering race, it does not mask genuinely
missing data. Icons degrade gracefully: a still-missing icon falls back to text and never fails the
build. Self-heal does NOT remove the local-test order above: running the two fetchers after seeding
is still the fast, offline-friendly path.

## Self-healed icons must ride the deploy artifact
Self-heal writes new PNGs into `data/icons/` on the `buildPages` runner, but those files reach the
deployed site ONLY because `data/icons` is in the `generated-site` artifact upload `path:` list in
`buildPages.yml`. The `assemble` job downloads that artifact over its own `main` checkout and copies
the merged `data/icons` into `_site/data/`, so the healed PNGs (absent from `main`) survive into
`_site`. Drop `data/icons` from that upload list and week-1 self-healed icons 404 on deploy while
still working locally — the local `http.server` serves the repo's own `data/icons/` directly, which
masks the gap. See [[artifact-only-deploy]].

Related: [[keystone-guru-mapping-data]].
