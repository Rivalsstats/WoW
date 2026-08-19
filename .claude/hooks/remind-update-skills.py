#!/usr/bin/env python3
"""PostToolUse hook: remind to update the knowledgebase after touching a documented subsystem.

Repo convention (AGENTS.md 'Recording new knowledge'): durable knowledge lives
in .claude/skills/ and AGENTS.md, and must be kept current when the behavior it
describes changes. This hook nudges after an edit to a documented subsystem. It
never blocks; it only adds context. Fails open on any parse error.
"""
import json
import re
import sys

# repo-relative path patterns for subsystems covered by a skill or AGENTS.md
DOCUMENTED_PATTERNS = (
    r"backend_scripts/",
    r"templates/.*\.html$",
    r"database\.sql$",
    r"assets/(js|css|scss)/",
    r"discord_bot/",
    r"\.github/workflows/",
)
MATCHERS = [re.compile(p, re.IGNORECASE) for p in DOCUMENTED_PATTERNS]


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
    # Never nudge about edits to the knowledgebase itself.
    if ".claude/skills/" in normalized or normalized.endswith("AGENTS.md"):
        sys.exit(0)
    if not any(m.search(normalized) for m in MATCHERS):
        sys.exit(0)

    context = (
        "Documented subsystem changed (" + normalized + "). Before finishing, check "
        "whether a .claude/skills/ skill or AGENTS.md still matches this change and "
        "needs updating. Follow the 'knowledgebase-authoring' skill for when and how to "
        "record (ask the user first, present-tense only, no history or line numbers)."
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
