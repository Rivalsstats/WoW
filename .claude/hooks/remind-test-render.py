#!/usr/bin/env python3
"""PostToolUse hook: remind to test-render after editing a rendered-page input.

Repo convention (CLAUDE.md / AGENTS.md): any change to a template, page
generator, shared generator module, or page asset must be verified by seeding
the local test DB and rendering the affected page. This hook nudges after such
an edit. It never blocks; it only adds context. Fails open on any parse error.
"""
import json
import re
import sys

# repo-relative path patterns that feed a rendered page
PAGE_INPUT_PATTERNS = (
    r"templates/.*\.html$",
    r"backend_scripts/generate.*\.py$",
    r"backend_scripts/(pageGeneration|aggregateData|commonUtils)\.py$",
    r"assets/(js|css|scss)/",
)
MATCHERS = [re.compile(p, re.IGNORECASE) for p in PAGE_INPUT_PATTERNS]


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    if payload.get("tool_name") not in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
        sys.exit(0)

    file_path = (payload.get("tool_input") or {}).get("file_path", "")
    if not isinstance(file_path, str) or not file_path:
        sys.exit(0)

    normalized = file_path.replace("\\", "/")
    if not any(m.search(normalized) for m in MATCHERS):
        sys.exit(0)

    context = (
        "Rendered-page input changed (" + normalized + "). Before finishing, "
        "test-render the affected page against a locally seeded DB: follow the "
        "'local-test-render' skill (seed via backend_scripts/localDev/seed_test_db.py, "
        "render with the matching generate*.py, serve on port 8099, inspect in the "
        "Browser pane). Do not hand back page work that was only reasoned about."
    )
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": context,
        }
    }))
    sys.exit(0)


if __name__ == "__main__":
    main()
