---
name: item-quality-from-bonus
description: Item quality/color (rarity) is resolved from the item's bonus ids, not the base item quality, on both the spec page and the items page, through one shared commonUtils helper. Use when an item's border/name color looks wrong, or when editing quality resolution in generateSpecPages.py / generateItemPages.py / commonUtils.py / templates/item.html / assets/js/items.js.
---

# Item Rarity Color Comes From The Bonus-Id Variant, Not The Base Item

The rarity color an item renders with is the quality of its bonus-id variant, not
the base item quality from `equippable-items.json`. Some bonus ids set an item's
quality (PvP tracks, upgrade tracks, crafted tiers); `processBonusIds.build_bonus_quality_map`
writes those as a `bonusId -> quality` map to `data/static/bonus_quality_map.json`
(keys are strings).

Both generators resolve the override through one shared helper,
`commonUtils.resolve_bonus_quality(bonus_ids, bonus_quality_lookup)`. It accepts a
list of ids or a comma/colon-delimited string, and when several bonus ids set a
quality the last one wins. Do not re-implement this lookup.

- **Spec page** (`generateSpecPages._apply_item_bonuses`): resolves each equipped
  item's `quality_override` from its comma-joined `bonus.ids`.
- **Items page** (`generateItemPages.build_payloads`): resolves the payload's
  `quality_override` from the item's most-used variant (`top_variant`, the
  colon-joined bonus string of `global.variants[0]`). The item page header link and
  Wowhead tooltip still point at the base item on purpose (no bonus track); only the
  color follows the variant.

Templates render rarity with the design-token classes `border-quality-{N}` (border)
and `item-quality-{N}` (text), choosing `quality_override` when set and the base
`quality` otherwise. `templates/item.html` sets `item_quality = item.quality_override
if item.quality_override is not none else item.quality` once and uses it for the
header icon border and name; `templates/spec_page.html` inlines the same
`quality_override`-or-`quality` choice. See [[frontend-design-tokens]] for the rarity
tokens.

For the browse grid, `generateItemPages` stores `quality_override` in
`items_index.json` **only when it differs from the base** (keeping the manifest
small), and `assets/js/items.js` mirrors the template with `effQuality(i)` (override
else base) for card border/name color, the quality-filter options, and the
quality-filter comparison. Because `effQuality` is used for both color and the
filter, the grid stays self-consistent (a card colored uncommon filters as
uncommon).

This is the same "one shared commonUtils lookup used by both the spec and item pages"
shape as [[enchant-lookup-filter]].
