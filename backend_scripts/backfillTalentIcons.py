"""Backfill talent icons that the raw Raidbots export shipped empty.

Some real talents arrive in ``data/static/talents.json`` with ``"icon": ""``
(for example Ascendance, Aimed Shot, Javelineer). They still carry a valid
``spellId``, so we resolve the icon through Blizzard's spell media API, cache it
under ``data/icons/`` following the repo's bare-name + ``.png`` convention, and
write the resolved name back into ``talents.json`` so the downstream
``processTalents.py`` lookup and the spec overview image both pick it up.

Runs in the getStaticData workflow after the icon downloads and before
``processTalents.py``. Fails loudly if a spell's icon cannot be resolved, so a
genuine gap is never silently swallowed into a broken page.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List

from PIL import Image

from aggregateData import get_access_token
from fetchSpellInfo import fetch_spell_icon

TALENTS_PATH = Path("data") / "static" / "talents.json"
ICON_DIR = Path("data") / "icons"


def iter_icon_entries(obj: Any) -> Iterator[Dict[str, Any]]:
    """Yield every talent entry dict (those carrying both 'icon' and 'spellId')."""
    if isinstance(obj, dict):
        if "icon" in obj and "spellId" in obj:
            yield obj
        for value in obj.values():
            yield from iter_icon_entries(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_icon_entries(item)


def normalize_icon(icon_filename: str) -> str:
    """Ensure data/icons/<bare>.png exists and return the bare name.

    ``fetch_spell_icon`` caches the media file under its own extension (often
    .jpg); the lookup stores a bare name and the overview renderer opens
    ``f"{icon}.png"``, so convert to a .png sibling.
    """
    bare = os.path.splitext(icon_filename)[0]
    src = ICON_DIR / icon_filename
    dst = ICON_DIR / f"{bare}.png"
    if dst != src:
        with Image.open(src) as im:
            im.convert("RGBA").save(dst)
    return bare


def main() -> None:
    client_id = os.environ.get("BLIZ_CLIENT_ID")
    client_secret = os.environ.get("BLIZ_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "BLIZ_CLIENT_ID and BLIZ_CLIENT_SECRET must be set in environment"
        )

    with TALENTS_PATH.open(encoding="utf-8") as f:
        data = json.load(f)

    # Group the empty-icon entries by spellId so each spell is fetched once.
    by_spell: Dict[int, List[Dict[str, Any]]] = {}
    for entry in iter_icon_entries(data):
        if not entry.get("icon") and (entry.get("spellId") or 0) > 0:
            by_spell.setdefault(int(entry["spellId"]), []).append(entry)

    if not by_spell:
        print("No empty-icon talents with a spellId to backfill. Nothing to do.")
        return

    token = get_access_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}

    ICON_DIR.mkdir(parents=True, exist_ok=True)

    unresolved: List[int] = []
    for spell_id, entries in sorted(by_spell.items()):
        name = next((e.get("name") for e in entries if e.get("name")), "")
        icon_filename = fetch_spell_icon(spell_id, headers)
        if not icon_filename:
            print(f"  FAILED to resolve icon for spell {spell_id} ({name!r})")
            unresolved.append(spell_id)
            continue
        bare = normalize_icon(icon_filename)
        for entry in entries:
            entry["icon"] = bare
        print(f"  Backfilled spell {spell_id} ({name!r}) -> {bare} ({len(entries)} entries)")

    if unresolved:
        raise RuntimeError(
            "Could not resolve icons for spellIds "
            f"{unresolved}. These talents would render as a broken icon; fix the "
            "spell media source before continuing."
        )

    with TALENTS_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Backfilled {len(by_spell)} spell icons into {TALENTS_PATH}")


if __name__ == "__main__":
    main()
