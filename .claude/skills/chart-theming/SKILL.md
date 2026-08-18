---
name: chart-theming
description: Use window.MythiChart (assets/js/chart-theme.js) for Chart.js chrome colors, icon preloading, patch annotations, and live re-theming; do not re-copy chart boilerplate. Use when adding or editing any Chart.js chart on a page.
---

# Chart theming with MythiChart

`window.MythiChart` (`assets/js/chart-theme.js`) centralizes reusable Chart.js pieces. It is
loaded globally in `templates/javascript_imports.html` and only DEFINES helpers, so it is safe
BEFORE Chart.js is loaded. Per page, include `plugins/chartjs.min.js` (plus
`chartjs-plugin-zoom.min.js` + `hammer.min.js` and/or `chartjs-plugin-annotation` as needed).

**Use theme-aware chrome, not literal hex.** `MythiChart.colors` is a **live** object refreshed on
theme change; read from it for ticks, grid, legend, tooltip, patch lines. Keys: `grid`, `gridDark`,
`axisText`, `tickText`, `legendText`, `patchLine`, `tooltipBg`, `tooltipText`.

**Helpers:**
- `refreshColors()`: repopulates `colors` for the active theme (called internally on themechange).
- `rgba(colorObj, alpha)`: rgba string from a `{r,g,b}` class-color object.
- `loadIcons(urls)`: returns `Promise<(Image|null)[]>`, resolving null for any that fail.
- `buildPatchAnnotations([{x, label}])`: annotation config for vertical patch-release lines.
- `makeIconLabelsPlugin(id)`: a plugin that draws preloaded icons next to one axis's ticks
  (`axis: "x"` bottom, `axis: "y"` left with optional labels).
- `registerChart(chart)`: optional explicit registration.

**Live re-theming.** On the `window` `mythistone:themechange` event, MythiChart re-themes and
redraws every live chart. It auto-discovers charts via `Chart.getChart()` internally, so inline
charts are covered without changes and you rarely need `registerChart`. (Note: the discovery
routine is internal and not exposed on the public `MythiChart` object.)

**Series colors are deliberately hardcoded** and left untouched by re-theming, because they encode
class / rarity meaning and are chosen to read on both themes. Do not route series colors through the
chrome palette: pull the meaningful colors from the class/quality tokens instead (see
[[frontend-design-tokens]]).

**Zoom gating.** Gate zoom/pan interaction to large screens (`min-width: 992px`) so it does not
fight touch scrolling on mobile. See [[frontend-framework-choices]].
