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

Related: [[keystone-guru-mapping-data]].
