# CLAUDE.md

All repo knowledge lives in root [AGENTS.md](../AGENTS.md). **Read it first.** It is the single,
always-on source of truth: pipeline, directory map, hard constraints, templating, frontend stack,
tooling, working preferences, and every subsystem gotcha. There is no skills tree.

Record new durable knowledge in `AGENTS.md` (see its "How knowledge is handled" section for when and
how), never in Claude's auto-memory. Auto-memory does not transfer between machines or tools; in-repo
notes do. Do not duplicate `AGENTS.md` content here; this file holds only the two Claude Code rules
below.

## Two rules specific to Claude Code here

### 1. Always route repo work through the `mythistone-expert` agent

For any change to `backend_scripts/`, `templates/`, `database.sql`, or client-side `assets/`,
delegate to the **`mythistone-expert`** subagent (Agent tool, `subagent_type: "mythistone-expert"`).
If one is already running or was recently spawned, continue it with `SendMessage` rather than
starting a new one. Keep a single expert: the pipeline is tightly coupled through
`databaseConnector.py`, so cross-domain changes are best held in one context. Use other subagents
for task shapes (explore, plan), not for domain splits.

### 2. Always test-render any page you touch

Whenever you change something that affects a rendered page (a `templates/*.html`, a
`backend_scripts/generate*.py`, `pageGeneration.py`/`aggregateData.py`/`commonUtils.py`, or page
CSS/JS in `assets/`), seed the local test DB and render the affected page before considering it
done. Do not hand back page work that was only reasoned about. Render it. Full workflow is in
`AGENTS.md` ("Tooling reality and verification") and
[backend_scripts/localDev/README.md](../backend_scripts/localDev/README.md).

Serve the repo root on **port 8099** (matches `.claude/launch.json`) and open the page in the
Browser pane:

```bash
python -m http.server 8099 --bind 127.0.0.1
```

Output dirs (`dungeons/`, `classes/`, `items/`, `pages/`) are gitignored, safe to write.
