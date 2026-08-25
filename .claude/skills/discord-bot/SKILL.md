---
name: discord-bot
description: The standalone dockerized Discord slash-command bot in discord_bot/ that serves MythiStone stats, plus its application-owned custom-emoji layer. Use when editing anything under discord_bot/ (cogs, embeds.py, emojis.py, db.py, site_data.py, social_render.py, guards.py, charts.py), Dockerfile.bot, or buildBotImage.yml.
---

# Discord bot (discord_bot/)

Standalone Discord slash-command bot (discord.py 2.x, `app_commands`, slash-only, no
message-content intent). Ships as its own image `ghcr.io/mythistone/mythistone-bot` via
`Dockerfile.bot` + `entrypoint_bot.sh` + `.github/workflows/buildBotImage.yml`, a separate compose
service auto-updated by watchtower. Needs only the populated MySQL DB plus `DISCORD_BOT_TOKEN`.
Season id comes from a baked `seasonInfo.json`.

**Hybrid data.** Aggregates come live from MySQL through `databaseConnector.py` via
`discord_bot/db.py` (async bridge: checkout to `configure_read_session` to query to release on a
ThreadPoolExecutor). CI-computed features come from published site JSON (`comps_index`,
`compRoutes`, `items_index`, `gem_enchant_index`, `spec_meta/<id>`, `simdps_tierlist`) via
`site_data.py` with a TTL cache. Reuses `commonUtils`, `chartData`, `tierMath` (flat-copied into the
image like the collector).

**Cogs** live in `discord_bot/cogs/`: `analyze.py`, `comps.py`, `dungeon.py`, `items.py`, `meta.py`,
`routes.py`, `season.py`, `spec.py`, `stats.py`. Several top-level commands (`/lust`, `/routes`,
`/analyze`) live on cogs as `@app_commands.command`, not group subcommands. `/meta specs|dungeons|
popularity|simdps` render the site's exact og:images by reusing `image_generation` renderers via
`social_render.py`, not text embeds. `/analyze` ("Am I meta?") ports `assets/js/analyzer.js`,
scoring a pasted simc string against `spec_meta`.

**Guards.** A tree-wide `bot.tree.interaction_check = guards.season_guard` short-circuits every
command to a friendly "season hasn't started" embed during the pre-season gap, keyed off
`databaseConnector.season_has_runs` (cached, fail-open). Related: [[season-rollover-wipe]].

**Custom emojis** (`discord_bot/emojis.py`): spec/buff/role/meta/item icons are **application-owned**
custom emojis (work in every server). Deterministic names ≤32 chars: `spec_<SpellIconFileId>`,
`buff_<buff_id>`, `role_tank|healer|dps`, `meta`, `item_<item_id>`. `emojis.populate(bot,
create_missing=True)` runs in `on_ready` and auto-uploads missing ones (spec/buff from
`SITE_BASE/data/icons`, role/meta from vendored `discord_bot/emoji_assets/*.png`, which the
user vendors manually per the `AGENTS.md` working preferences). Class icons are NOT hosted by the
site pipeline, so `class_*` are
excluded and inert. **GOTCHA:** custom emoji render only in embed **description** and **field
values**, never in titles or field names. Registry is empty when unpopulated so render helpers
return `""` and `build_*` fall back to text, keeping `db_smoke_test` valid.

**Brand & support.** `embeds.base_embed` puts the brand in the **author slot** via
`embeds.brand_author` (name "Mythistone", icon, `url=SITE_BASE`) so it is a clickable link, and sets
a fixed **support footer** (`embeds.support_footer` / `SUPPORT_FOOTER_TEXT`) asking people to back
Patreon — footers are plain text (no hyperlink) so the fixed string doubles as a consistent
minimum embed width. The clickable Patreon link (`config.PATREON_URL`) lives on the standalone
`embeds.patreon_embed`. Every cog sends its result through the shared `embeds.respond(interaction,
embed, **kwargs)` helper (never `interaction.followup.send` directly), which appends
`patreon_embed` as a **second embed on the same reply message** once every `config.PATREON_EMBED_EVERY`
commands — same message so the nudge can't be dismissed independently of the result. The counter
(`bot._support_counts`, read off `interaction.client`) keys **per-guild** in a server (any member
advances it) and **per-user** otherwise (user-installed usage). Only successful commands reach a
`respond` call (the season gate and errors reply through `errors._respond`), so the nudge never
piggybacks on an error or the off-season notice. Errors/gate embeds intentionally skip the nudge.

**Sync signature includes choice values.** `bot._sync_tree` hashes each command's parameter names
**and their `Choice` (name, value) pairs**, not just names. This is required: a season rollover
changes `lookups.DUNGEON_CHOICES` (new dungeon ids/names) without changing any command/parameter
name, so a names-only hash would skip the global sync (the hash persists in the `bot_cache` volume)
and leave Discord serving the previous season's dungeon list. Dungeon/class options are static
`@app_commands.choices`; only `item`/`spec` use live autocomplete.

**Pull signatures carry counts.** `fetch_dungeon_lust_timeline`'s `top_npcs` column is a
`GROUP_CONCAT(CONCAT(npc_id, ':', count))` string (`"134388:3,134389:1"`), so any parse must take
`tok.split(":")[0]` for the npc id, exactly like the dungeon page template — a raw `int(tok)` throws.

**Verify.** `python -m discord_bot.db_smoke_test` drives the bot's real data paths: it runs every
`db.run(databaseConnector.fetch_*)` / `commonUtils.fetch_stat_info` against the seeded local test DB
(source A) and fetches the site JSON artifacts live over HTTP (source B, soft-skipped off-season),
feeding the real results into every `build_*_embed` and rendering all six social images. Seed first
and export the printed `DATABASE_*`: `python backend_scripts/localDev/seed_test_db.py` (see
[[local-test-render]]). No `DISCORD_BOT_TOKEN` needed. `docker build -f Dockerfile.bot` keeps the
in-image import guards only (the DB test can't reach MySQL during a build, so it runs as the
`db-smoke-test` job in `buildBotImage.yml`, gating the image). Manual emoji resync:
`python -m discord_bot.emoji_sync` (needs only `DISCORD_BOT_TOKEN`). Related:
[[social-image-mock-harness]], [[pooled-connection-gotchas]].
