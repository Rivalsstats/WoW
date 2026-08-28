---
name: sidenav-footer-flow
description: The left sidenav uses a flex-column flow layout so its scrollable nav list is contained above the support-buttons footer; do not restore absolute positioning or a fixed 72vh list height. Use when editing the sidenav footer/scroll region in templates/sidenav.html or the sidenav rules in assets/css/responsive.css / material-dashboard.css.
---

# Sidenav footer must stay in flow

The left sidenav (`#sidenav-main` in `templates/sidenav.html`) is laid out as a single flex column
in `assets/css/responsive.css`:

- `#sidenav-main` → `display:flex; flex-direction:column; flex-wrap:nowrap; overflow:hidden`
- `#sidenav-collapse-main` (the scrollable nav list) → `flex:1 1 auto; min-height:0; height:auto`,
  which overrides the `height: calc(72vh)` set on it in `material-dashboard.css`
- `.sidenav-footer` (the Patreon / Discord / share-buttons block) → `position:static;
  flex:0 0 auto; height:auto`, overriding the `position:absolute; height:20vh` from
  `material-dashboard.css`

This makes the list scroll inside its own box that ends exactly at the footer's top edge, at any
viewport height and in the mobile offcanvas.

WHY: `material-dashboard.css` pins `.sidenav .sidenav-footer` to `position:absolute` (out of flow,
`z-index:1100`) and `#sidenav-collapse-main` to a flat `height: calc(72vh)`. Absolutely positioned,
the footer reserves no space, so the fixed-height list box physically extends *behind* it. The
sidenav aside is transparent (the dark backdrop is painted by `<body>`), so nav items paint through
the footer and its `border-top` slices across an item mid-row whenever the footer's real content
(~295px: two buttons + the share row) is taller than the gap left below the 72vh box. A
`padding-bottom` value only reserves scroll distance and cannot stop the paint-through; it was
replaced.

TWO TRAPS when touching this:

- The aside also carries Bootstrap's `.navbar` class (`flex-wrap: wrap`) and the list carries
  `.navbar-collapse` (`flex-basis: 100%`). Without `flex-wrap: nowrap` on `#sidenav-main`, the
  footer wraps onto a new flex line *back over the top of the list* — a worse overlap. Keep the
  `nowrap`.
- Do not "fix" a reported overlap by reintroducing absolute positioning, a magic `padding-bottom`,
  or a fixed vh height on the list. The flow layout is the fix; those are what caused the bug.

The Browser pane could not composite raster frames in the render environment used here, so verify
sidenav geometry with DOM measurement (`listBottom <= footerTop`, zero items straddling
`footerTop`, footer + all six share buttons on-screen) at a short (~800px tall) and a tall desktop
height plus the `<1200px` offcanvas, rather than by eye. See [[local-test-render]].

The footer's share buttons are wired in [[deep-links]]-aware `assets/js/share.js`; the accordion
class list above it interacts with [[bootstrap-capture-phase]] and [[klaro-consent-embeds]].
