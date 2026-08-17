"""Encode Blizzard "serialization version 2" talent loadout strings for the seeder.

The live members.loadout column holds a real Blizzard talent export string, which
only the client analyzer (assets/js/analyzer.js ``decodeLoadout``) ever decodes.
Between seasons the DB is empty, so the local seeder has to synthesize those
strings itself, and a placeholder like ``seed-102-000123`` fails the analyzer
(it is not base64 and never decodes). This module produces a real, decodable v2
string so the seeded meta build round-trips through the same bitstream the client
reads.

The bitstream (mirroring ``decodeLoadout``):
  * a 6-bit value per output char, packed LSB-first, over the base64 alphabet
    ``A-Za-z0-9+/`` (note: real base64, so ``+`` and ``/`` can appear; ``-`` and
    ``=`` never do);
  * header: 8-bit version (2), 16-bit spec id, then 128 bits of tree hash which
    the client ignores, so we emit zeros;
  * then, for every node id in ``fullNodeOrder`` in order: a ``selected`` bit;
    when set, a ``purchased`` bit; when purchased, a partial-rank flag (we always
    emit 0 = full rank, so no rank bits follow) and a choice flag (1 for a choice
    node, followed by a 2-bit entry index; 0 otherwise).

``encode_loadout`` and the ``decode_loadout`` below are kept in lock-step by the
self-test at the bottom, which also runs on import so a drift from the client
decoder fails loudly rather than silently shipping an undecodable seed string.
"""

TALENT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_CHAR_IDX = {c: i for i, c in enumerate(TALENT_CHARS)}
LOADOUT_VERSION = 2


class _BitWriter:
    def __init__(self):
        self.bits = []

    def write(self, value, nbits):
        """Append ``nbits`` of ``value``, least-significant bit first."""
        for i in range(nbits):
            self.bits.append((value >> i) & 1)

    def encode(self):
        # Pad the tail up to a whole char (6 bits); the client tolerates a
        # truncated / zero-padded tail, so zeros are safe filler.
        bits = self.bits
        while len(bits) % 6 != 0:
            bits.append(0)
        out = []
        for i in range(0, len(bits), 6):
            v = 0
            for b in range(6):
                v |= bits[i + b] << b
            out.append(TALENT_CHARS[v])
        return "".join(out)


def _is_choice(node):
    return len(node.get("entries") or []) > 1


def encode_loadout(spec_id, selected, full_node_order, nodes):
    """Encode a v2 loadout string.

    ``selected``      -- {node_id: entry_index} for every purchased node. Free /
                         granted nodes are forced selected regardless (they are
                         part of every build), so callers need not list them.
    ``full_node_order`` -- the spec's flat decode order (INCLUDES ids absent from
                         ``nodes``; those consume a not-selected bit each so the
                         stream stays aligned with the client decoder).
    ``nodes``         -- {str(node_id): {entries, free, ...}} geometry map.
    """
    # Normalize keys so callers can pass int or str node ids.
    sel = {int(k): int(v or 0) for k, v in (selected or {}).items()}

    def node_for(nid):
        return nodes.get(str(nid)) or nodes.get(nid)

    w = _BitWriter()
    w.write(LOADOUT_VERSION, 8)
    w.write(int(spec_id), 16)
    for _ in range(16):
        w.write(0, 8)  # 128-bit tree hash, ignored by the client

    for nid in full_node_order:
        node = node_for(nid)
        is_free = bool(node and node.get("free"))
        is_sel = is_free or int(nid) in sel
        if not is_sel:
            w.write(0, 1)
            continue
        w.write(1, 1)  # selected
        w.write(1, 1)  # purchased
        w.write(0, 1)  # partial-rank flag: 0 => full rank, no rank bits follow
        if node and _is_choice(node):
            w.write(1, 1)  # choice flag
            w.write(sel.get(int(nid), 0), 2)  # entry index (0..3)
        else:
            w.write(0, 1)  # not a choice node
    return w.encode()


def decode_loadout(code, full_node_order, nodes):
    """Round-trip inverse of :func:`encode_loadout`, mirroring the JS decoder.

    Returns {node_id: {"entry_index": int, "purchased": bool}} for the selected
    nodes, or ``None`` when the string is malformed. Present only so the self-test
    can prove encode/decode agree with the client contract.
    """
    if not code:
        return None
    bits = []
    for ch in code:
        v = _CHAR_IDX.get(ch)
        if v is None:
            return None
        for b in range(6):
            bits.append((v >> b) & 1)

    pos = [0]

    def read(n):
        r = 0
        for i in range(n):
            if pos[0] >= len(bits):
                return None
            r |= bits[pos[0]] << i
            pos[0] += 1
        return r

    version = read(8)
    if version != LOADOUT_VERSION:
        return None
    read(16)  # spec id
    for _ in range(16):
        read(8)  # tree hash

    selected = {}
    for nid in full_node_order:
        is_sel = read(1)
        if is_sel is None:
            break
        if not is_sel:
            continue
        is_purchased = read(1)
        entry_index = 0
        if is_purchased:
            if read(1):
                read(6)  # partial rank
            if read(1):
                entry_index = read(2) or 0
        selected[int(nid)] = {"entry_index": entry_index, "purchased": bool(is_purchased)}
    return selected


def _self_test():
    """Encode a sample build and prove it round-trips through the decoder, so a
    drift from analyzer.js ``decodeLoadout`` fails loudly on import."""
    full_order = [10, 20, 30, 40, 50, 60, 70]
    nodes = {
        "10": {"entries": [{"name": "A"}], "free": True},          # free single
        "20": {"entries": [{"name": "B"}]},                        # passive
        "30": {"entries": [{"name": "C"}, {"name": "D"}]},         # choice (2)
        "40": {"entries": [{"name": "E"}, {"name": "F"}, {"name": "G"}]},  # choice (3)
        "50": {"entries": [{"name": "H"}]},                        # passive, unselected
        "60": {"entries": [{"name": "I"}]},                        # hero passive
        # 70 intentionally absent from `nodes` (granted id) -> not selected
    }
    selected = {20: 0, 30: 1, 40: 2, 60: 0}
    code = encode_loadout(102, selected, full_order, nodes)
    assert all(c in _CHAR_IDX for c in code), f"non-alphabet char in {code!r}"
    assert "-" not in code and "=" not in code, f"unexpected char in {code!r}"

    decoded = decode_loadout(code, full_order, nodes)
    assert decoded is not None, "decode returned None"
    # Free node 10 is selected even though the caller did not list it.
    assert 10 in decoded and decoded[10]["purchased"]
    # Every listed pick round-trips with its entry index.
    for nid, idx in selected.items():
        assert nid in decoded, f"node {nid} missing from decode"
        assert decoded[nid]["entry_index"] == idx, (
            f"node {nid} entry index {decoded[nid]['entry_index']} != {idx}")
    # Unselected / granted ids stay out of the taken set.
    assert 50 not in decoded and 70 not in decoded


_self_test()


if __name__ == "__main__":
    # Ad-hoc encode of one seeded-style build, printed for eyeballing.
    order = [1, 2, 3]
    nodes = {"1": {"entries": [{"n": 1}], "free": True},
             "2": {"entries": [{"n": 2}, {"n": 3}]},
             "3": {"entries": [{"n": 4}]}}
    print(encode_loadout(102, {2: 1, 3: 0}, order, nodes))
    print("self-test passed")
