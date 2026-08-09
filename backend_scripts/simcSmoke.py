"""Emit a self-contained SimulationCraft *smoke* profile covering every
simulated spec, used to validate a freshly built simc binary/image before we
trust it (see .github/workflows/buildSimcImage.yml and build-simc in
buildPages.yml).

One default actor per DPS/Tank spec — ``source=default`` so simc builds its
built-in APL and pets for that spec. That is exactly what reproduces upstream
regressions like the Unholy DK ``army_ghoul`` "could not find spell data"
abort (run 31325084815): the broken action lives in simc's own default action
list, so a bare default actor at ``iterations=1`` trips it during init.

Deliberately dependency-free (stdlib + the committed ``data/static/*.json``): it
must run in the image-build workflow, which installs no DB driver. The tiny
class-token / race maps mirror simcBis.build_header — WoW classes change rarely,
so the duplication is cheap insurance against coupling this to the DB-bound
collector module. Keep the header format in sync with simcBis.build_header.
"""

import os
import json
import argparse
from pathlib import Path

STATIC_DIR = Path("data") / "static"

# Only DPS (2) and Tank (0) are simmed; healers (1) never are. Mirrors
# simcBis.SIMULATED_ROLES. Augmentation (1473) is skipped there too — but a
# smoke init is cheap and catches its spell data too, so we include it here.
SIMULATED_ROLES = {0, 2}

# simc class assignment keyword (no spaces), keyed by lowercased Blizzard class
# name. Mirror of simcBis.CLASS_TOKENS.
CLASS_TOKENS = {
    "death knight": "deathknight",
    "demon hunter": "demonhunter",
    "druid": "druid",
    "evoker": "evoker",
    "hunter": "hunter",
    "mage": "mage",
    "monk": "monk",
    "paladin": "paladin",
    "priest": "priest",
    "rogue": "rogue",
    "shaman": "shaman",
    "warlock": "warlock",
    "warrior": "warrior",
}

# A valid race per class token (race is irrelevant to an init smoke; it only
# needs to be legal). Mirror of simcBis.DEFAULT_RACE.
DEFAULT_RACE = {
    "deathknight": "orc",
    "demonhunter": "blood_elf",
    "druid": "night_elf",
    "evoker": "dracthyr",
    "hunter": "orc",
    "mage": "gnome",
    "monk": "pandaren",
    "paladin": "blood_elf",
    "priest": "human",
    "rogue": "orc",
    "shaman": "orc",
    "warlock": "orc",
    "warrior": "orc",
}


def resolve_level():
    """Mirror of simcBis._resolve_level (SIMC_LEVEL env -> seasonInfo
    max_character_level -> fallback), so the smoke actor level matches what the
    real profiles use for the same game data."""
    env = os.environ.get("SIMC_LEVEL")
    if env:
        return str(env)
    try:
        si = json.loads((STATIC_DIR / "seasonInfo.json").read_text(encoding="utf-8"))
        lvl = si.get("max_character_level")
        if lvl:
            return str(int(lvl))
    except Exception:
        pass
    return "90"


def spec_slug(name):
    return (name or "").lower().replace("'", "").strip().replace(" ", "_")


def build_actor(spec_id, spec_name, class_token, primary_stat, level):
    """One default actor block — same header shape as simcBis.build_header."""
    race = DEFAULT_RACE.get(class_token, "orc")
    role = "spell" if (primary_stat or "").upper() == "INTELLECT" else "attack"
    return [
        f'{class_token}="spec{spec_id}_smoke"',
        "source=default",
        f"spec={spec_slug(spec_name)}",
        f"level={level}",
        f"race={race}",
        f"role={role}",
        "position=back",
    ]


def build_smoke(static_dir=STATIC_DIR):
    specs = json.loads((static_dir / "specs.json").read_text(encoding="utf-8"))
    classes = json.loads((static_dir / "classes.json").read_text(encoding="utf-8"))
    level = resolve_level()

    # Global options: one iteration, one second — we only care that every actor
    # initializes (create_actions runs before iteration 0, which is where the
    # missing-spell-data abort fires). single_actor_batch keeps them independent.
    lines = ["iterations=1", "max_time=1", "single_actor_batch=1", ""]

    count = 0
    for spec_id, info in sorted(specs.items(), key=lambda kv: int(kv[0])):
        try:
            role = int(info.get("role", 2))
        except (TypeError, ValueError):
            role = 2
        if role not in SIMULATED_ROLES:
            continue
        class_name = classes.get(str(info.get("classID")), {}).get("name", "")
        token = CLASS_TOKENS.get(class_name.lower())
        if not token:
            # Unknown class name would emit an invalid actor and fail the smoke
            # for the wrong reason. Fail loudly instead of silently dropping it.
            raise ValueError(
                f"no simc class token for class {class_name!r} (spec {spec_id}); "
                "update CLASS_TOKENS"
            )
        lines.extend(build_actor(spec_id, info.get("name"), token, info.get("primary_stat"), level))
        lines.append("")
        count += 1

    if count == 0:
        raise ValueError("no simulated specs found in specs.json")
    return "\n".join(lines) + "\n", count


def main():
    parser = argparse.ArgumentParser(description="Generate a simc smoke profile covering every simulated spec")
    parser.add_argument("--output", default="smoke.simc", help="Path to write the smoke .simc profile")
    parser.add_argument("--static_dir", default=str(STATIC_DIR), help="Directory holding specs.json / classes.json")
    args = parser.parse_args()

    text, count = build_smoke(Path(args.static_dir))
    Path(args.output).write_text(text, encoding="utf-8")
    print(f"Wrote {args.output} covering {count} simulated spec(s)")


if __name__ == "__main__":
    main()
