---
name: enchant-link-icon-rune-fallback
description: Every enchant link/icon render surface must fall back to spellId/spellIcon because DK weapon runes have no scroll itemId. Use when rendering or editing any enchant Wowhead link or icon in templates/spec_page.html (gear-item macro, Enchantment Details accordion, Enchant Combos) or the enchant table in templates/item.html.
---

# Enchant Links And Icons Need A spellId/spellIcon Fallback

An enchant record carries a scroll `itemId` (normal enchants) OR only a `spellId` with no
`itemId` (Death Knight weapon runes). `_enchant_link_fields` in
`backend_scripts/generateSpecPages.py` bakes both `itemId` and `spellId` onto each enchant
record for exactly this reason, so every render surface can link the enchant itself rather than
`item=<enchant_id>` (which collides with an unrelated item).

Every place that renders an enchant Wowhead link and icon must handle both shapes:

- **Link:** `item=<itemId>` when `itemId` exists, else `spell=<spellId>`. Hardcoding
  `item=<itemId>` produces a broken `item=None` link for DK runes.
- **Icon:** `itemIcon` when present, else `spellIcon`. Using `itemIcon or icon` never reaches
  `spellIcon`, so it shows the wrong icon for runes.

The surfaces that must all follow this: the spec page gear-item macro, the spec page
"Enchantment Details" accordion (both the header and the per-enchant detail rows), the Enchant
Combos section (all in `templates/spec_page.html`), and the enchant table (`opt_table`) in
`templates/item.html`. Keep `spec_id`, the `onclick="event.preventDefault();"` guard when
`tag=='button'`, the quality border, and the `craftingQuality` overlay intact when adding the
fallback. Runes have no `quality`/`craftingQuality`, so the border falls back to `border-grey-100`
and no overlay renders, which is correct.

This is orthogonal to display filtering: runes have no `expansion` key so they are always kept
and slot-fit `WEAPON` (see [[enchant-lookup-filter]]). To test-render a rune through the real
pipeline, a Death Knight spec's top `WEAPON` enchant in `global_aggregated_enchantments_slot_group`
must be a rune id; the seeder picks enchants at random, so force one in the throwaway DB if the
seed happens to pick a scroll (see [[local-test-render]]).
