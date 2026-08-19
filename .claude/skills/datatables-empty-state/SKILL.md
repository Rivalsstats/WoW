---
name: datatables-empty-state
description: A DataTables-initialized table whose empty state is a colspan placeholder <tr> inside the same table throws "Requested unknown parameter '1' for row 0, column 1"; wrap the whole <table> in {% if rows %} and put the empty state in a plain <p>. Use when adding or editing a curated table that gets .DataTable(), or when a page logs that DataTables warning on load.
---

# DataTables empty-state placeholder rows crash the table

Several pages init curated tables with `$(el).DataTable({ columnDefs: [...] , ordering:false, paging:false, ... })` and target column indices in `columnDefs` (e.g. `templates/dungeon_page.html` inits `lust-table`, `popular-comps-table`, `best-loot-table`, `skip-npcs-table`).

If the table's empty state is a single `<tr><td colspan="N">No data yet.</td></tr>` rendered **inside** that same `<table>`, DataTables sees a row with only one cell but the `columnDefs` reference columns 1..N-1, so on load it throws:

```
DataTables warning: table id=<id> - Requested unknown parameter '1' for row 0, column 1
```

WHY: the `colspan` row is one physical `<td>`; DataTables indexes cells positionally and cannot find the columns its `columnDefs`/responsive config expect.

HOW to apply: never leave the empty-state row inside a DataTable. Wrap the entire `<table>...</table>` in `{% if rows %}` and render the empty state as a plain `<p class="text-center text-sm text-secondary p-3 mb-0">No data yet.</p>` in the `{% else %}` branch, so no table element exists when empty. The init loop's `if (el)` guard then skips it. Delete any now-dead placeholder `<tr><td colspan>` inside the tbody, whether it came from a `{% for %}...{% else %}` or a separate `{% if rows|length == 0 %}` guard.

All four curated tables on `dungeon_page.html` (`lust-table`, `popular-comps-table`, `best-loot-table`, `skip-npcs-table`) now follow this pattern. `best-loot-table` was always written this way; the other three were retrofitted after they threw this warning. Watch for the trap in two disguises: an inline `{% for x in seq if cond %}...{% else %}<tr colspan>` (popular-comps, skip-npcs), AND a `{% for %}` immediately followed by a separate `{% if seq|length == 0 %}<tr colspan>{% endif %}` sitting in the same tbody (lust-table) - the second form is easy to miss because the loop looks like it has no empty branch. The skip table's dead placeholder had also rotted to render just the text `"yet."`.

Two other pages init DataTables and are already safe - use them as the reference for how to do it right: `templates/comps.html` wraps each `glue-table-{role}` in `{% if role_specs %}` (server-side, same as above), and `assets/js/item.js` `mountTable()` builds tables in JS - it `clear()`s the tbody and rebuilds rows BEFORE calling `.DataTable()`, and sets `language: { emptyTable: "—" }` so the empty case is DataTables-native with no placeholder row at all. If you build a table's rows in JS, prefer that pattern.

Test with the [[local-test-render]] workflow, or render the template with empty context and assert the `id="<table>"` element is absent (see the render check used when this was fixed).
