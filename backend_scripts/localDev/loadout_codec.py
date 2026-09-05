"""Re-export of the shared Blizzard v2 loadout encoder for the local seeder.

The encoder itself lives in ``commonUtils`` (the single, production-reachable copy
used by both this seeder and the CI tierlist profile builder
``generateSimcProfiles``). This thin module keeps the seeder's historical import
site (``from loadout_codec import encode_loadout``) working and runs a self-test on
import so a drift from the client decoder (assets/js/analyzer.js ``decodeLoadout``)
or a regression in the choice/tiered/rank handling fails loudly rather than
silently shipping an undecodable / simc-invalid seed string.
"""

from commonUtils import (  # noqa: F401  (re-exported for the seeder)
    TALENT_CHARS,
    LOADOUT_VERSION,
    encode_loadout,
    decode_loadout,
    is_choice_node,
)

_CHAR_IDX = {c: i for i, c in enumerate(TALENT_CHARS)}


def _self_test():
    """Encode a sample build and prove it round-trips through the decoder, so a
    drift from analyzer.js ``decodeLoadout`` fails loudly on import. Covers a free
    single node, a passive, two real choice nodes, a TIERED multi-rank node (whose
    entries are rank tiers, NOT alternatives, so it must carry a rank rather than a
    choice index), and a granted id absent from ``nodes``."""
    full_order = [10, 20, 30, 40, 50, 60, 70, 80]
    nodes = {
        "10": {"entries": [{"spellId": 1}], "free": True},                 # free single
        "20": {"entries": [{"spellId": 2}]},                               # passive
        "30": {"entries": [{"spellId": 3}, {"spellId": 4}]},               # choice (2)
        "40": {"entries": [{"spellId": 5}, {"spellId": 6}, {"spellId": 7}]},  # choice (3)
        "50": {"entries": [{"spellId": 8}]},                               # passive, unselected
        "60": {"entries": [{"spellId": 9}]},                               # hero passive
        # 80 tiered: 3 rank-tier entries, maxRanks 4 — NOT a choice node.
        "80": {"type": "tiered", "maxRanks": 4,
               "entries": [{"spellId": 11}, {"spellId": 12}, {"spellId": 13}]},
        # 70 intentionally absent from `nodes` (granted id) -> not selected
    }
    selected = {20: 0, 30: 1, 40: 2, 60: 0, 80: 0}
    ranks = {80: 2}  # tiered node bought to rank 2 of 4
    code = encode_loadout(102, selected, full_order, nodes, ranks=ranks)
    assert all(c in _CHAR_IDX for c in code), f"non-alphabet char in {code!r}"
    assert "-" not in code and "=" not in code, f"unexpected char in {code!r}"

    decoded = decode_loadout(code, full_order, nodes)
    assert decoded is not None, "decode returned None"
    # Free node 10 is selected even though the caller did not list it.
    assert 10 in decoded and decoded[10]["purchased"]
    # Every listed choice pick round-trips with its entry index.
    for nid, idx in ((30, 1), (40, 2)):
        assert decoded[nid]["entry_index"] == idx, (
            f"node {nid} entry index {decoded[nid]['entry_index']} != {idx}")
    # The tiered node carries a rank, not a choice index.
    assert 80 in decoded and decoded[80]["rank"] == 2, "tiered rank did not round-trip"
    assert decoded[80]["entry_index"] == 0, "tiered node must not carry a choice index"
    assert not is_choice_node(nodes["80"]), "tiered node misdetected as a choice node"
    # Unselected / granted ids stay out of the taken set.
    assert 50 not in decoded and 70 not in decoded


_self_test()


if __name__ == "__main__":
    print("self-test passed")
