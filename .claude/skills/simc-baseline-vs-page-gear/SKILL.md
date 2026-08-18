---
name: simc-baseline-vs-page-gear
description: Why the simcBis baseline gear can legitimately differ from the spec page's displayed equipped items, and why not to "fix" it. Use when someone reports a mismatch between a spec page's per-slot item and the simc BiS reference set, or when editing set_is_valid / fetch_slot_rows in simcBis.py.
---

# simc Baseline vs Page Gear (Accepted Divergence)

simcBis's baseline (the base actor built in `build_profile`) is the most-popular **legal** gear combo. `set_is_valid` (`backend_scripts/simcBis.py`, line 748) enforces equip limits: at most 2 embellishments (itemLimit category 512), unique-equipped, other itemLimit categories.

The spec page (`generateSpecPages.py` `fetch_slot_info` + template) instead shows the most-popular item **per slot independently**, with NO global equip-limit enforcement, so the page can display an illegal set.


**Decision: leave as-is.** The baseline = most-popular legal set and may diverge from the page on slots where the page's per-slot pick conflicts with the embellishment/equip cap. Do NOT "fix" this by forcing the baseline to match the page, which would create an illegal 3-embellishment reference set. The page showing an unequippable loadout is a known, accepted display quirk.

Separately: finger/trinket baseline uses the slot-GROUP query (`fetch_slot_rows`) so FINGER_1/FINGER_2 and the two trinkets get the two most-popular DISTINCT items instead of the same item twice.

Related: [[simc-chunked-checkpoint]].
