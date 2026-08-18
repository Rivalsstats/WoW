---
name: verify-database-sql
description: How to syntax and runtime verify backend_scripts/database.sql stored procedures against an ephemeral MySQL 8 Docker container. Use when changing a stored proc (sp_agg_*, sp_run_agg_pipeline, sp_swap_public_table) or any routine in database.sql, since agents have no live DB but Docker Desktop is installed.
---

# Verify database.sql procs in ephemeral MySQL

Agents have no live-DB creds, but Docker Desktop is installed, so the stored procedures in `backend_scripts/database.sql` can be verified locally against a throwaway MySQL 8.

Recipe:
1. Start Docker if needed, poll `docker info` until up (~20s).
2. `docker run -d --name X -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=Mythistone mysql:8`, then wait on `docker exec X mysqladmin ping -proot`.
3. Syntax check catches most bugs. MySQL validates a routine body at `CREATE` time but does NOT check table existence or `CREATE TABLE ... LIKE` (those are runtime), so you can create the changed procs in an empty schema. `database.sql` has no `DELIMITER` statements, so extract the routines you changed and wrap them: `DELIMITER $$ ... END$$ DELIMITER ;`. A Python script that slices each `CREATE ... PROCEDURE` block up to its standalone `END;` line works well.
4. Runtime tests need the definer user. Routines are `DEFINER=\`Test\`@\`%\``, so that user must exist or `CALL` fails with 1449: `CREATE USER 'Test'@'%' IDENTIFIED BY 'x'; GRANT ALL ... WITH GRANT OPTION;`.

Harness gotchas (not proc bugs):
- `docker exec` needs `-i` to pass stdin (heredoc). Without it mysql gets no input and silently no-ops.
- Creating a proc via `mysql <<SQL` without DELIMITER truncates it at the first inner `;`, so the proc is never created and a later `CALL` under `2>/dev/null` fails invisibly and returns instantly. Always DELIMITER-wrap proc creation.
- When writing the temp .sql from Python, use a real Windows path (`C:\...`), not a `/c/...` MSYS path, or `docker cp` cannot find it.
- To hold an idle in-transaction metadata lock for a lock-contention test, run in a bg client: `START TRANSACTION; SELECT ...; \! sleep 40`. The `\!` shell-escape makes the client sleep so the server connection sits idle with the txn (and MDL) open. `SELECT SLEEP()` would show as Running instead.

Note: the localDev seeder ([[local-test-render]]) does the same schema load end-to-end, and its `schema_loader.py` handles the DELIMITER-wrapping and tablespace stripping for you. Use this manual recipe for isolated proc changes; use the seeder for full-pipeline verification. Related: [[aggregation-pipeline]].
