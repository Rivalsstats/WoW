---
name: knowledgebase-authoring
description: How to record and write MythiStone durable knowledge (when to record, and the present-tense / no-history / no-line-number style rules). Use when creating or editing a skill under .claude/skills/, editing AGENTS.md, or deciding whether to record a finding after a task.
---

# Authoring the knowledgebase

Durable repo knowledge lives in two places: narrow **skills** under `.claude/skills/` (one gotcha per
skill) and **`AGENTS.md`** (always-on repo truth). Never use a tool's private auto-memory. It does not
transfer across machines or tools; only in-repo notes do.

## When to record

After finishing a task that produced newly gained durable knowledge (non-obvious mechanics, cross-file
coupling, gotchas, corrected assumptions), **ask the user for permission to record it as the last line
of your final message**. Do not record first and ask later, and do not record silently. The user
decides. Phrase the ask as a short recap of what is worth recording, then the question:

> While working this task I found that X and Y, we confirmed Z, and changed Z2. Would you like me to
> record this knowledge in the knowledgebase?

Only once they agree, store it as a narrow skill (update an existing skill rather than duplicating it)
or a section of `AGENTS.md`.

- **Skip the ask entirely when there is nothing new to record:** planning-only work that changed
  nothing, or a bug / wrong behavior whose mechanics are already documented in the knowledgebase.
- **Gauge "new" against the knowledgebase, not the code.** Record when this file + the skills do not
  already capture it, *not* merely when it is absent from the code. Much of the codebase is still
  undocumented; a finding being derivable from the code is not a reason to skip it if the
  knowledgebase does not yet capture it.

## How to write it

- **State only the current truth, never history.** Skills and `AGENTS.md` describe how the project
  works *right now* and what caveats exist *right now*. Do not write "this used to work like X and we
  changed it to Y", changelog entries, migration notes, or dated "verified/changed on <date>" lines.
  When something changes, edit the affected notes so they read as if the new behavior was always the
  behavior, and delete any prose that only existed to contrast against the old way.
- **No line-number references.** Never anchor a note to a line ("around line 182", "line 20"). Lines
  shift and the reference rots into noise. Identify code by stable names: file, function, proc, SQL
  constant, class, or CSS selector.
- **One gotcha per skill, narrowly scoped.** Give it a `description` that names when to use it. Link
  related skills with `[[skill-name]]`.
