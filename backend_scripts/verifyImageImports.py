#!/usr/bin/env python
"""Verify the collector image ships everything its modules reference.

The image copies hand-maintained whitelists into /app -- one of backend scripts,
one of data/static JSON files (see the COPY blocks in Dockerfile). Both drift:
a module that grows a new import or reads a new lookup file keeps working in the
repo and only fails inside the container. That is how
`from commonUtils import DUAL_WIELD_TWOHAND_SPECS` became a crash-loop and how
crafting.json/bonuses.json/enchantments.json went missing from the sims. This
script turns that class of mistake into a failed image build.

Two checks, both static (ast only, nothing is imported or executed -- which
matters because collectLeaderboardData.py has import-time side effects:
load_dotenv, argparse.parse_args, databaseConnector.init_connection_pool):

1. every imported module resolves to a local file or an installed package;
2. every `STATIC_DIR / "x.json"` / `... / "static" / "x.json"` path literal
   exists under <root>/data/static.

Run as `python verifyImageImports.py [root]` (root defaults to /app). Exits
non-zero and lists everything unresolved; there is no warn-and-continue path.
"""

import ast
import importlib.util
import sys
from pathlib import Path


def collect_imported_names(tree):
    """Top-level module names imported anywhere in `tree`.

    ast.walk (rather than just the module body) is deliberate: it also catches
    function-local imports such as simcBis.py's lazy `import docker`, which are
    just as fatal at runtime and just as invisible at build time.

    Relative imports (`from . import x`) are skipped -- the image flattens every
    module into /app, so nothing there is a package.
    """
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names


def collect_static_json_refs(tree):
    """Names of data/static JSON files referenced by pathlib `/` expressions.

    Matches `STATIC_DIR / "crafting.json"` and `DATA_DIR / "static" / "specs.json"`
    -- the two forms the collector modules use. Deliberately narrow: only a
    literal filename whose left-hand path is anchored to STATIC_DIR or a "static"
    segment counts, so runtime-built paths (SIMC_IO_DIR / f"{token}.json",
    data/discord_status.json) are not mistaken for shipped lookup data.
    """
    names = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)):
            continue
        right = node.right
        if not (isinstance(right, ast.Constant) and isinstance(right.value, str)):
            continue
        if not right.value.endswith(".json"):
            continue
        anchored = any(
            (isinstance(n, ast.Name) and n.id == "STATIC_DIR")
            or (isinstance(n, ast.Constant) and n.value == "static")
            for n in ast.walk(node.left)
        )
        if anchored:
            names.add(right.value)
    return names


def resolves(name, root):
    """Can `name` be imported from `root`? Local module first, then installed."""
    if (root / f"{name}.py").exists() or (root / name).is_dir():
        return True
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        # find_spec raises rather than returning None for some malformed or
        # partially-installed names; treat that as unresolvable.
        return False


def main(argv):
    root = Path(argv[1] if len(argv) > 1 else "/app")
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory", file=sys.stderr)
        return 1

    sources = sorted(root.glob("*.py"))
    if not sources:
        print(f"ERROR: no python modules found in {root}", file=sys.stderr)
        return 1

    static_dir = root / "data" / "static"
    missing_imports = []
    missing_data = []
    static_refs = 0
    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for name in sorted(collect_imported_names(tree)):
            if not resolves(name, root):
                missing_imports.append(f"{path.name}: {name}")
        for name in sorted(collect_static_json_refs(tree)):
            static_refs += 1
            if not (static_dir / name).exists():
                missing_data.append(f"{path.name}: data/static/{name}")

    if missing_imports or missing_data:
        if missing_imports:
            print(
                f"ERROR: unresolvable imports in {root} "
                f"(missing COPY in Dockerfile, or missing pip dependency):",
                file=sys.stderr,
            )
            for entry in missing_imports:
                print(f"  {entry}", file=sys.stderr)
        if missing_data:
            print(
                f"ERROR: static data files referenced but not shipped in {root} "
                f"(missing COPY in Dockerfile):",
                file=sys.stderr,
            )
            for entry in missing_data:
                print(f"  {entry}", file=sys.stderr)
        return 1

    print(
        f"image check OK: {len(sources)} modules, "
        f"{static_refs} static data references in {root}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
