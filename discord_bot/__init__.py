"""MythiStone Discord bot package.

The bot ships the same way the collector image does: the shared backend modules
(``databaseConnector``, ``commonUtils``, ``chartData``, ``tierMath``) are flat-copied
next to this package into ``/app`` rather than imported as ``backend_scripts.*``.
Locally they live under ``backend_scripts/``. This bootstrap puts whichever of the
two exists on ``sys.path`` so ``import databaseConnector`` resolves in both, and
fails loudly at import time if it cannot.
"""

import os
import sys

_here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root or /app
_backend = os.path.join(_here, "backend_scripts")
for _candidate in (_backend, _here):
    if os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import databaseConnector  # noqa: E402,F401 — fail loudly now if the module isn't shipped
