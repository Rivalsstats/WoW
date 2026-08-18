---
name: season-snapshot-archive
description: Each season's built _site is force-pushed to a season-<id> branch in a separate archive repo as cold storage before the DB wipe. Use when touching the "Archive snapshot to season branch" step in buildPages.yml or reasoning about where old-season sites live.
---

# Season Snapshot Archive

To retain old-season data before the per-season DB wipe, the CI `assemble` job in `.github/workflows/buildPages.yml` (step "Archive snapshot to season branch (separate repo)") turns the built `_site` into a fresh single-commit git repo and force-pushes it to branch `season-<blizzard_season_id>` in a SEPARATE repo `MythiStone/Mythistone_Archive` over SSH (deploy-key secret `ARCHIVE_DEPLOY_KEY`, `ARCHIVE_REPO=git@github.com:MythiStone/Mythistone_Archive.git`). A `SNAPSHOT.json` provenance marker is written into the site root first.

- **Separate repo** keeps the main repo's clones unaffected. Season branches in the main repo would otherwise be pulled by every clone/fetch. Force-push keeps each season branch history-free. Git blob dedup means unchanged icons are not re-uploaded.
- **Season key** comes from `data/static/seasonInfo.json` `blizzard_season_id`. When it increments, the next build targets a NEW branch, freezing the old season's branch. Every build archives, so the latest snapshot is at most one build old.
- **Cold storage only.** The site is NOT relocatable (hardcoded domain, root-absolute `/assets/` links), so snapshots serve correctly only from a URL root. Sub-path hosting would need URL rewriting (deferred).
- **Prereqs (manual):** the archive repo plus a write-enabled deploy key.

Paired with the DB wipe that runs after it, see [[season-rollover-wipe]].
