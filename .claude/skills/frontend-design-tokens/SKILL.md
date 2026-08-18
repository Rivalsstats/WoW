---
name: frontend-design-tokens
description: Never hardcode domain hex (class, item-rarity, stat, scrollbar colors) in page CSS/templates; use the CSS custom properties and utility classes. Use when styling anything colored by WoW class, item quality, or stat, when theming, or when adding icons.
---

# Frontend design tokens

Do not hardcode domain hex values. The palette lives in CSS custom properties defined in
`assets/css/material-dashboard.css`, `assets/css/classes.css`, and `assets/css/stat-colors.css`.

- **Brand:** `var(--bs-primary)` = `#e91e63`.
- **WoW class colors:** dual tokens in `classes.css`: `--class-<Name>` is contrast-tuned for TEXT
  on the current theme's surfaces, `--class-<Name>-raw` is the true Blizzard hex for FILLS (bars,
  chips, swatches). Names are PascalCase (`--class-DeathKnight`, `--class-DemonHunter`). Utility
  classes `.class-<Name>-text` and `.class-<Name>-bg` are provided.
- **Item rarity:** `--quality-0` .. `--quality-8` in `stat-colors.css`, with utility classes
  `.item-quality-N` (text) and `.border-quality-N` (border).
- **Stat tiers:** `--stat-*` (and `--stat-*-raw`) in `stat-colors.css` (`--stat-crit`,
  `--stat-haste`, `--stat-mastery`, `--stat-vers`, ...), with `.stat-<name>` utility classes
  (both short and long forms exist, e.g. `.stat-crit`, `.stat-int` / `.stat-intellect`).
- **Scrollbars:** `--mythi-scrollbar-track`, `--mythi-scrollbar-thumb`, `--mythi-scrollbar-thumb-hover`
  (defined in `material-dashboard.css`, consumed by `assets/css/custom-scrollbars.css`).

**Theming.** Dark is the **unconditional default** via `data-bs-theme` on `<html>`, set by an
inline pre-paint script in `templates/header_imports.html` that reads `localStorage.theme`; the OS
`prefers-color-scheme` is deliberately ignored. Every token is defined twice: under
`:root,[data-bs-theme=light]` and again under `[data-bs-theme=dark]`. On toggle,
`material-dashboard.js` dispatches a `window` `mythistone:themechange` event (see [[chart-theming]]).

**Icons.** UI glyphs use Material Symbols Rounded (`<i class="material-symbols-rounded">name</i>`).
Game icons are served from `/data/icons/<id>.jpg` (specs, buffs) or `.png` (items) as
`<img class="avatar avatar-sm">`. See [[build-new-page]], [[frontend-framework-choices]].
