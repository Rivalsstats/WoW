"""Load backend_scripts/database.sql into a throwaway MySQL 8 for local test renders.

``database.sql`` is a plain schema+routine dump that cannot be replayed as-is:

1. It has **no ``DELIMITER`` statements**, so every stored-procedure / event body would
   be truncated at its first inner ``;`` by any client that splits on semicolons. We wrap
   the routines section in ``DELIMITER $$`` and rewrite each routine's terminating
   column-0 ``END;`` to ``END$$`` (inner ``END IF;`` / ``END WHILE;`` are indented and
   left untouched), then feed the whole thing to the ``mysql`` CLI which understands
   ``DELIMITER``.
2. Nine tables carry ``/*!50100 TABLESPACE `name` */`` clauses that execute on MySQL 8 and
   reference named tablespaces that do not exist in a fresh container. We strip them.

The routines are ``DEFINER=`Test`@`%```; that user must exist before they are created or
``CALL`` later fails with 1449, so we create it first.

This module only prepares and loads the schema. It does NOT enable the event scheduler --
the caller starts mysqld with ``--event-scheduler=DISABLED`` so the purge/wipe events
(which would delete freshly seeded rows older than 14 days, or fire a season wipe) never
run. See seed_test_db.py.
"""

import os
import re
import subprocess

# The routines section begins at the first stored procedure. Everything before it is
# CREATE TABLE / index DDL; everything from here to EOF is CREATE PROCEDURE / CREATE EVENT.
_ROUTINE_START_RE = re.compile(r"^CREATE\s+DEFINER=", re.MULTILINE)

# `/*!50100 TABLESPACE `whatever` */` -- version-gated, so it runs on 8.0 and fails.
_TABLESPACE_RE = re.compile(r"/\*!50100 TABLESPACE `[^`]+` \*/")

# A routine's own terminator: a line that is exactly `END;` or `END <label>;` at column 0.
# Inner block ends (`  END IF;`, `  END WHILE;`, `  END LOOP;`) are always indented, so
# anchoring to column 0 with no leading whitespace never matches them.
_ROUTINE_END_RE = re.compile(r"^(END(?:\s+[A-Za-z_]\w*)?);[ \t]*$", re.MULTILINE)


def preprocess_sql(raw_sql):
    """Return a single SQL script the ``mysql`` CLI can replay against a fresh DB.

    Strips the tablespace clauses, then DELIMITER-wraps the routine section.
    """
    sql = _TABLESPACE_RE.sub("", raw_sql)

    match = _ROUTINE_START_RE.search(sql)
    if not match:
        # No routines in the dump: just the table DDL, replayable as-is.
        return sql

    tables_sql = sql[: match.start()]
    routines_sql = sql[match.start():]

    # Convert each routine's terminating `END;` -> `END$$` while `DELIMITER $$` is active.
    wrapped = _ROUTINE_END_RE.sub(r"\1$$", routines_sql)

    return (
        tables_sql
        + "\nDELIMITER $$\n"
        + wrapped
        + "\nDELIMITER ;\n"
    )


def _run_mysql(container, root_password, database, sql_text=None, sql_argument=None):
    """Pipe SQL into ``mysql`` inside the container. Raises on non-zero exit."""
    cmd = [
        "docker", "exec", "-i", container,
        "mysql", f"-uroot", f"-p{root_password}", "--binary-mode",
    ]
    if database:
        cmd.append(database)
    if sql_argument is not None:
        cmd += ["-e", sql_argument]
        proc = subprocess.run(cmd, capture_output=True, text=True)
    else:
        proc = subprocess.run(
            cmd, input=sql_text, capture_output=True, text=True, encoding="utf-8"
        )
    if proc.returncode != 0:
        raise RuntimeError(
            f"mysql load failed (exit {proc.returncode}):\n{proc.stderr.strip()}"
        )
    return proc.stdout


def create_definer_user(container, root_password, definer_user="Test", definer_password="test"):
    """Create the ``Test`` definer the routines run as, with full privileges."""
    stmt = (
        f"CREATE USER IF NOT EXISTS '{definer_user}'@'%' IDENTIFIED BY '{definer_password}'; "
        f"GRANT ALL PRIVILEGES ON *.* TO '{definer_user}'@'%' WITH GRANT OPTION; "
        f"FLUSH PRIVILEGES;"
    )
    _run_mysql(container, root_password, database=None, sql_argument=stmt)


def load_schema(container, root_password, database, sql_path):
    """Create the definer user, then replay the preprocessed database.sql.

    Returns the preprocessed SQL string (useful for debugging / dumping to a file).
    """
    if not os.path.isfile(sql_path):
        raise FileNotFoundError(f"database.sql not found at {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as fh:
        raw_sql = fh.read()

    create_definer_user(container, root_password)

    processed = preprocess_sql(raw_sql)
    # `USE <db>` up top so the unqualified CREATE TABLEs land in the target schema even
    # though the routines self-qualify with `Mythistone.`.
    script = f"USE `{database}`;\n" + processed
    _run_mysql(container, root_password, database=None, sql_text=script)
    return processed
