---
name: loadout-v2-encoder
description: The shared Blizzard "serialization v2" talent-loadout encoder (commonUtils.encode_loadout), now used ONLY by the local seeder, and the rules that make its output both decode in the browser AND init in SimulationCraft. Use when synthesizing a talents= code (the local seeder), editing commonUtils.encode_loadout / decode_loadout / is_choice_node / load_talent_tree_geometry, or debugging a simc talent init error.
---

# Blizzard v2 loadout encoder (seeder-test-only)

`commonUtils.encode_loadout(spec_id, selected, full_node_order, nodes, ranks=None)` is the one
encoder for Blizzard "serialization version 2" talent strings (the value on a simc `talents=` line
and the string `analyzer.js`/`tierlist-modal.js` decode). It is now reachable ONLY from the local
seeder: `localDev/loadout_codec.py` re-exports it (and runs the import-time self-test) and
`localDev/seeders.py` calls it to synthesize both `members.loadout` and the seeded
`top_player_loadouts.loadout_text`, so the throwaway DB has decodable, simc-valid strings between
seasons. Production no longer synthesizes any talent code: the tierlist top50 set now uses the REAL
stored `top_player_loadouts.loadout_text` verbatim (`generateSimcProfiles._top50_talents`, see
[[tierlist-gear-modal]]), and `popular`/`simcbis` use the real most-popular stored code. Do NOT fork
a second copy; a divergent encoder is how the old top50 bug hid. `commonUtils.decode_loadout` and
`is_choice_node` live beside it; `load_talent_tree_geometry(spec_id, static_dir)` returns
`(fullNodeOrder, nodes)` from `data/static/talents/<spec>.json` (the processTalents output).

Because the encoder is seeder-only, the two hard rules and the data-skew caveat below are a
LOCAL-fidelity concern, never a production one: a seeded spec whose committed talent geometry has
drifted from the simc image's talent DB (e.g. a node with two real entries simc knows as single)
fails simc init on its seeded string, but production top50 never hits it because a real in-game
export string is authoritative for whatever simc build accepts it.

The bitstream, per node in `fullNodeOrder`: a selected bit; when set a purchased bit; then a
partial-rank flag (1 => a 6-bit rank follows) and a choice flag (1 => a 2-bit entry index follows).
Header = 8-bit version (2) + 16-bit spec id + 128-bit tree hash. Free nodes are forced selected.

## Two hard rules, both enforced by simc (not by the decoder)

The browser decoder is flag-driven and tolerant, so encode bugs only surface in simc, which strictly
validates. `encode_loadout` was written for the seeder and round-tripped only against its own decoder
and the browser, so it shipped these bugs latent until a real simc run exposed them:

- **The 128-bit tree hash may be zeros.** simc parses the hash and IGNORES it (it does not recompute
  Blizzard's checksum), so a synthesized code with an all-zero hash inits fine. Proven by decoding a
  real shipped simc Arcane profile string, re-encoding it with a zero hash, and getting an identical
  DPS sim. This is what lets us build a code from per-node data without Blizzard's checksum.
- **The choice flag/index goes ONLY on a genuine choice node.** `is_choice_node` = more than one
  IDENTITY-BEARING entry AND `type != "tiered"`. It counts entries through
  `commonUtils._talent_entry_has_identity` (an `id` / `definitionId` / nonzero `spellId`), so the
  identity-less padding entry the vendored raidbots talents.json appends to some single nodes (a
  `{spellId: 0}` whose name ends in " / ", e.g. WW Monk hero node 101235 "Inner Compass") does NOT
  count. A raw `len(entries) > 1` check miscounts that padding and emits a bogus 2-bit index on a
  single node, which fails simc init. The encoder feeds `is_choice_node` the RAW unfiltered
  `data/static/talents/<spec>.json` (via `load_talent_tree_geometry`), while the baked analyzer /
  tierlist trees already strip padding (`filter_talent_tree_nodes`); the identity check is what
  keeps the encoder agreeing with those decoders and simc, which all see the node as single. A
  `tiered` node (e.g. Arcane's "Prismatic Bolt", `maxRanks 4`, multiple rank-tier entries) and a
  single node are NOT choice nodes — emitting a 2-bit index on one makes simc fail init
  (`Node <id> is not a choice node but has index selection`). A multi-rank node bought below max
  instead carries its rank via the partial-rank flag (pass `ranks={node_id: rank}`); the seeder
  supplies real ranks.
- **A genuine 2-real-entry choice node simc treats as single is DATA SKEW, not this bug.** When the
  committed raidbots `data/static/talents/<spec>.json` carries a node with two real (identity-bearing)
  entries but the simc image's bundled talent DB knows that node as single, simc still errors
  `not a choice node but has index selection` and the encoder cannot detect it locally (both entries
  look real). This is a version gap between the weekly getStaticData talent dump and the deployed
  simc build, self-healing when they realign. It now only affects the SEEDER's synthetic strings
  (production top50 uses real stored `loadout_text` and never encodes), so it is a local-fidelity
  quirk a seeded spec can hit, not a production failure. Do not try to "fix" it in `is_choice_node`.

Also: a node id must be selectable by the spec, or simc errors `Selected node <id> entry <id> is not
available to player's spec`. The seeder's synthetic selections are drawn from the spec's own tree
geometry, so they satisfy this by construction.

## simc validation is MANDATORY for any encoder change

The decoder/browser agreeing is NOT proof. Any change to `encode_loadout` MUST be run through the
repo's simc image (`simulationcraftorg/simc:latest`, the WoW `12.x` build matching the committed
static data). A bare actor (`mage="t" \n source=default \n spec=arcane \n level=90 \n
talents=<code>`) that inits with no `Initialization error` is the gate. Locally, synthetic seeded
gear still fails simc init on armor class (a cloth spec handed a leather item is `Invalid type`), so
validate the TALENTS with a gearless actor. A gearless MELEE actor then fails init on its missing
main-hand weapon (`has no weapon equipped in the Main-Hand slot` / `No active players in sim!`) BEFORE
the sim runs, but that is a gear-shape artifact, not a talent error: simc reports a bad talent hash
independently (`... is not a choice node ...`), so a spec whose only error is the missing weapon has a
valid talent string. On Windows Git Bash, `docker run -v` needs `MSYS_NO_PATHCONV=1` or the `/work`
path is mangled to `C:/Program Files/Git/work`. The authoritative full-profile simc gate is the
collector smoke test's real spec-62 live slice (see [[collector-smoke-test]]). Related:
[[tierlist-gear-modal]], [[analyzer-page]].
