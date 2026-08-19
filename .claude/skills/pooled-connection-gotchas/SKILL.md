---
name: pooled-connection-gotchas
description: Two mysql.connector pooled-connection traps in MythiStone. Use when writing DB code in backend_scripts (databaseConnector.py, collectors, simcBis.py), debugging silent write loss / rolled-back upserts, or a long-running loop that dies with "MySQL Connection not available".
---

# Pooled connection gotchas

Two independent traps with `mysql.connector`'s `PooledMySQLConnection` in `backend_scripts/`.

## 1. `.autocommit` attribute does not reach the server

The pooled wrapper swallows the `.autocommit` setter: `conn.autocommit = True` sets a Python attribute but the server session stays `autocommit=0` (confirmed: `SELECT @@autocommit` returns 0 after the attribute set). Consequence: any WRITE on such a session sits in an open transaction and is rolled back when the connection returns to the pool, with no error. This is what made `snapshotTrends.py` report "upsert 10885 rows" yet leave `trend_snapshot` empty. Reads were unaffected, so it hid for a long time, and it also meant `configure_read_session`'s per-statement MDL release never actually happened on pooled connections.

Apply: `databaseConnector.configure_read_session` now also runs `cursor.execute("SET SESSION autocommit = 1")` (SQL reaches the server, the attribute set does not) alongside READ UNCOMMITTED and the lock timeouts. For any writer on a pooled connection, still call `commit_with_retry(conn)` explicitly rather than trusting the attribute.

## 2. Never hold a pooled connection across a long blocking op

The pool only revalidates a connection when it is checked out (a returned stale slot transparently reconnects on next use). A connection held checked out across a long blocking operation does NOT auto-recover: when the server drops it (`wait_timeout`) the next query raises `OperationalError: MySQL Connection not available`. For example, holding one pooled connection across `optimize_spec` (an hours-long simc container) lets the idle-held connection die mid-run, so the closing `persist()` write fails and surfaces as paired Discord alerts.

Apply: for any collector work spanning a slow step, use read -> release -> work -> reacquire. Read under a short-lived connection, close it, do the slow work holding NO connection, then check out a fresh one, preferring `databaseConnector.get_live_connection()` (it pings/reconnects on checkout), to write. `simcBis.run_simc_bis` does this.

Related: [[aggregation-pipeline]], [[item-page-aggregate-perf]].
