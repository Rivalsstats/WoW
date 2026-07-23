#!/usr/bin/env python
"""Verify every import in the collector image resolves.

The collector image copies a hand-maintained whitelist of backend scripts into
/app (see the COPY block in Dockerfile). A module that grows a new local import
without its file being added to that whitelist produces a ModuleNotFoundError at
container startup, not at build time -- which is exactly how
`from commonUtils import DUAL_WIELD_TWOHAND_SPECS` turned into a production
crash-loop. This script turns that class of mistake into a failed image build.

Imports are resolved statically via ast; nothing is executed. That matters
because collectLeaderboardData.py has import-time side effects (load_dotenv,
argparse.parse_args, databaseConnector.init_connection_pool) that cannot run at
build time or in a preflight check.

Run as `python verifyImageImports.py [root]` (root defaults to /app). Exits
non-zero and lists every unresolvable import; there is no warn-and-continue path.
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

    missing = []
    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for name in sorted(collect_imported_names(tree)):
            if not resolves(name, root):
                missing.append(f"{path.name}: {name}")

    if missing:
        print(
            f"ERROR: unresolvable imports in {root} "
            f"(missing COPY in Dockerfile, or missing pip dependency):",
            file=sys.stderr,
        )
        for entry in missing:
            print(f"  {entry}", file=sys.stderr)
        return 1

    print(f"import check OK: {len(sources)} modules in {root}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
