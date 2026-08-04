# Emoji source images (locally vendored)

The bot **auto-provisions** its application emojis on startup (`on_ready`) — any
missing emoji is uploaded, then subsequent starts are fast no-ops. Spec, class and
buff icons are downloaded automatically from the site's `/data/icons` endpoint. The
**role** and **meta** badges have no source on the site, so drop those image files
here (they're picked up on the next start; `python -m discord_bot.emoji_sync` can also
resync manually without restarting the bot):

| File | Emoji name | Used for |
|---|---|---|
| `role_tank.png`   | `role_tank`   | Tank line prefix in comp cards |
| `role_healer.png` | `role_healer` | Healer line prefix |
| `role_dps.png`    | `role_dps`    | DPS line prefix |
| `meta.png`        | `meta`        | "Meta comp" badge on the top comp |

Requirements: PNG (or JPG/GIF), square, ideally 128×128, under 256 KB (Discord's
application-emoji limit). Missing files are warned about and skipped — those entities
simply fall back to text until provided.
