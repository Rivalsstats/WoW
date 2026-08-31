---
name: comp-routes-specs-fallback
description: fetch_comp_routes collapses runs by physical route_signature and the surfaced winner borrows a signature-sibling's comp when its own run logged no route_specs. Use when editing fetch_comp_routes in databaseConnector.py or debugging route rows that show no comp spec icons.
---

# Comp-routes signature-sibling specs fallback

`fetch_comp_routes` in `databaseConnector.py` groups runs that share the same physical route
(`route_signature`, built in the CTE from each pull's enemies + spells) and surfaces one row per
signature (`rn = 1`, the highest key / fastest run), with `usage_count` = how many runs share that
route.

A run can share a signature with siblings yet have no rows in `route_specs` (only some runs logged a
group). When the `rn = 1` winner is that spec-less run, its comp icon bar would be empty even though
sibling runs of the same route recorded a comp. To avoid that, the query threads a `specs_route_key`
per signature: a `FIRST_VALUE` over the whole partition ordered by `(has route_specs) DESC,
keystone_level DESC, duration ASC, route_key ASC`. The winner keeps its own comp when it has one,
otherwise it deterministically borrows the highest-key sibling that logged a comp. Python then reads
`route_specs_map.get(route_key) or route_specs_map.get(specs_route_key)`.

`compRoutes.json` carries the resolved per-route `specs`, so this single backend fix fixes both the
server-rendered rows and the client-rendered search rows (see [[route-accordion-macro]]); the frontend
was only ever emitting an empty bar because the backend handed it `[]`.
