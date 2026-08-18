#!/usr/bin/env python3
"""PreToolUse hook: block git write commands.

Repo convention (AGENTS.md): the user handles all git. Agents must never run git
write commands. Read-only git (status/log/diff/show/branch) stays allowed.

Reads the Claude Code PreToolUse payload from stdin and denies any Bash command
that mutates the repo via git. Fails open (exit 0, no decision) on any parse
error so it can never wedge tool use.
"""
import json
import re
import sys

# git subcommands that mutate the repo or working tree
WRITE_VERBS = (
    "commit", "push", "add", "reset", "checkout", "switch", "restore",
    "merge", "rebase", "cherry-pick", "revert", "stash", "clean",
    "rm", "mv", "tag", "am", "apply", "fetch", "pull", "gc", "filter-repo",
)
# Match `git <write-verb>` only when `git` starts a command: at the beginning of
# the string or right after a shell separator (; & | newline backtick opening-paren).
# This still catches compound commands like `cd x && git commit` while not flagging
# a git verb that merely appears inside an echo/grep string argument.
PATTERN = re.compile(
    r"(?:^|[\n;&|`(])\s*git\s+(?:" + "|".join(re.escape(v) for v in WRITE_VERBS) + r")\b",
    re.IGNORECASE,
)


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    if payload.get("tool_name") != "Bash":
        sys.exit(0)

    command = (payload.get("tool_input") or {}).get("command", "")
    if not isinstance(command, str) or not PATTERN.search(command):
        sys.exit(0)

    reason = (
        "Blocked by repo convention (AGENTS.md: 'No git interaction'). "
        "The user commits and pushes. Do not run git write commands "
        "(commit, push, add, reset, checkout, merge, rebase, etc.). "
        "Read-only git such as 'git status', 'git log', 'git diff' is fine."
    )
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }))
    sys.exit(0)


if __name__ == "__main__":
    main()
