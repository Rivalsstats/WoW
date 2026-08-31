#!/usr/bin/env python3
"""Runtime smoke test for the freshly-built collector image.

Runs the collector image against an ALREADY-SEEDED throwaway test DB for a
bounded window and decides whether the image is safe to publish. It never
touches the live DB: the only database it opens is the local test DB whose
DATABASE_* env the seeder (backend_scripts/localDev/seed_test_db.py) prints.

Always-required signals (a build that fails any of these is broken):
  * the container starts and does not crash-exit during the window,
  * the logs carry no 'Traceback' and no 'CRITICAL STARTUP ERROR',
  * the collector reaches its main loop
    ('Starting data collection for regions:').

Extra signals, required ONLY with --require-rows true (the live season has
data, decided upstream by seasonHasData.py):
  * the 'runs' COUNT(*) in the test DB grew over the pre-run baseline
    (the collector actually collected and wrote leaderboard rows), and
  * the collector launched at least one SimulationCraft sibling container
    (docker label mythistone.role=simc-sim), i.e. it started running simc.

Off-season (--require-rows false) those two are logged for information only and
never fail the run, mirroring how buildPages skips itself when the season has
no data.

Usage (Docker Desktop / CI; test DB already seeded, its DATABASE_* exported,
and the runtime env below present in this process's environment):

    python backend_scripts/localDev/collector_smoke.py \\
        --image mythistone-collector:smoke --seconds 240 --require-rows true

Exit code 0 = pass, non-zero = fail. The container's full logs are always
printed, and the container plus any simc sibling containers are always stopped
and removed, before the script returns.
"""

import argparse
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
# databaseConnector lives in backend_scripts (same layout seed_test_db.py uses).
sys.path.insert(0, BACKEND_DIR)

import databaseConnector as db  # noqa: E402

# The collector reaches its async main loop only after env validation, DB pool
# init and talent loading all succeed, so this one line is a reliable
# "clean startup" marker.
STARTUP_MARKER = "Starting data collection for regions:"
# Crash markers emitted by entrypoint.sh / a Python traceback.
CRASH_MARKERS = ("Traceback", "CRITICAL STARTUP ERROR")
# simcBis tags every sibling sim container with this label.
SIMC_LABEL = "mythistone.role=simc-sim"
# Corroborating log prefix for a simc launch (simcBis._stat_log messages).
SIMC_LOG_MARKER = "simc:"

# Runtime env forwarded from this process into the collector container. Only
# vars actually present are passed through: a genuinely missing required var
# (e.g. no Blizzard creds) then makes the entrypoint exit 2, which this script
# correctly reports as a crash. SIMC_IO_VOLUME is passed explicitly (from
# --simc-volume) and so is deliberately absent from this list.
FORWARD_ENV = [
    # Test DB the collector writes to (never the live DB).
    "DATABASE_HOST", "DATABASE_PORT", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME",
    # Entrypoint-required API creds / webhook.
    "WEBHOOK_URL", "RAIDERIO_API_KEY", "KEYSTONE_GURU_USER", "KEYSTONE_GURU_PW",
    # Region selection + poll cadence.
    "REGIONS", "CHECK_INTERVAL",
    # SimulationCraft: enable it and point it at the shared io dir, tuned by the
    # caller for a fast launch (we only assert simc STARTS, never that it finishes).
    "SIMC_ENABLED", "SIMC_DOCKER_IMAGE", "SIMC_IO_DIR",
    "SIMC_MAX_COMBINATIONS", "SIMC_TARGET_ERROR", "SIMC_RUN_TIMEOUT", "SIMC_THREADS",
    "SIMC_COMBO_ITERATIONS", "SIMC_CANDIDATES_PER_SLOT", "SIMC_SPEC_SLEEP",
    "SIMC_CPUSET", "SIMC_CPUS",
]

# Tables the collector writes. 'runs' is the primary leaderboard-write signal;
# the others are printed for corroboration only.
PRIMARY_TABLE = "runs"
CORROBORATING_TABLES = ("members", "equipment")


def _run(cmd, check=False):
    """Run a command, capturing text output. Never raises unless check=True."""
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\n{proc.stderr}")
    return proc


def _docker_available():
    try:
        return _run(["docker", "info"]).returncode == 0
    except Exception:
        return False


def _init_db():
    """Open the test-DB pool from DATABASE_* env. Fails loudly if it is absent."""
    keys = ["DATABASE_HOST", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME", "DATABASE_PORT"]
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        sys.exit(
            "collector_smoke needs the seeded test DB. Missing " + ", ".join(missing) + ".\n"
            "Seed it first: python backend_scripts/localDev/seed_test_db.py, then export the "
            "printed DATABASE_* vars."
        )
    db.init_connection_pool(
        os.environ["DATABASE_HOST"], os.environ["DATABASE_USER"],
        os.environ["DATABASE_PASSWORD"], os.environ["DATABASE_NAME"],
        os.environ["DATABASE_PORT"], pool_size=4,
    )


def count_rows(table):
    """COUNT(*) of a table using a FRESH pooled connection each call.

    A fresh connection avoids the REPEATABLE READ snapshot trap: a long-lived
    connection would keep re-reading its first snapshot and never see the rows
    the collector commits mid-window. Table names are hardcoded constants, so
    the f-string carries no user input.
    """
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        rows = db.fetch_with_retry(conn, cur, f"SELECT COUNT(*) FROM {table}")
        cur.close()
        return int(rows[0][0])
    finally:
        conn.close()


def container_state(name):
    """Return (running, exit_code). running is None if the container is gone."""
    proc = _run(["docker", "inspect", "-f", "{{.State.Running}}|{{.State.ExitCode}}", name])
    if proc.returncode != 0:
        return None, None
    running_s, _, exit_s = proc.stdout.strip().partition("|")
    return running_s == "true", int(exit_s or 0)


def container_logs(name):
    """Combined stdout+stderr of the container (docker splits the two streams)."""
    proc = _run(["docker", "logs", name])
    return (proc.stdout or "") + (proc.stderr or "")


def simc_container_ids():
    proc = _run(["docker", "ps", "-a", "--filter", f"label={SIMC_LABEL}", "-q"])
    return proc.stdout.split()


def ensure_volume(name):
    _run(["docker", "volume", "create", name])  # idempotent


def remove_container(name):
    _run(["docker", "rm", "-f", name])


def cleanup_simc_siblings():
    ids = simc_container_ids()
    if ids:
        _run(["docker", "rm", "-f", *ids])


def build_run_cmd(args):
    cmd = [
        "docker", "run", "-d", "--name", args.container_name,
        # host network so the container reaches the test MySQL at 127.0.0.1:<port>
        "--network", "host",
        # the docker socket + shared named volume let the collector launch simc
        # sibling containers exactly as it does in production
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "-v", f"{args.simc_volume}:/app/data/simc_io",
        "-e", f"SIMC_IO_VOLUME={args.simc_volume}",
    ]
    for var in FORWARD_ENV:
        if os.environ.get(var) is not None:
            cmd += ["-e", var]  # value taken from this process's environment
    cmd.append(args.image)
    return cmd


def _bool_arg(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--image", required=True,
                        help="Collector image tag to run (e.g. mythistone-collector:smoke)")
    parser.add_argument("--seconds", type=int, default=240,
                        help="Max seconds to watch the collector before deciding (default 240)")
    parser.add_argument("--require-rows", default="false",
                        help="true|false: require a runs-count increase and a simc launch "
                             "(set from seasonHasData.py has_data)")
    parser.add_argument("--container-name", default="collector-smoke",
                        help="Name for the collector container (default collector-smoke)")
    parser.add_argument("--simc-volume", default="mythistone_simc_io_smoke",
                        help="Named docker volume shared with simc siblings at /app/data/simc_io")
    parser.add_argument("--poll", type=int, default=5,
                        help="Seconds between polls of the container/DB (default 5)")
    args = parser.parse_args()
    require_rows = _bool_arg(args.require_rows)

    if not _docker_available():
        sys.exit("Docker is not available. Start Docker Desktop and retry.")

    _init_db()

    base_counts = {t: count_rows(t) for t in (PRIMARY_TABLE, *CORROBORATING_TABLES)}
    print("Baseline test-DB row counts:")
    for table, n in base_counts.items():
        print(f"  {table}: {n}")
    print(f"require_rows={require_rows} (runs-growth + simc launch are "
          f"{'REQUIRED' if require_rows else 'informational only'})")

    ensure_volume(args.simc_volume)
    remove_container(args.container_name)  # clear any stale container of this name

    run_cmd = build_run_cmd(args)
    print("\nStarting collector container:")
    print("  " + " ".join(run_cmd))
    start = _run(run_cmd)
    if start.returncode != 0:
        print(start.stdout)
        print(start.stderr, file=sys.stderr)
        sys.exit(f"FAIL: could not start container {args.container_name}.")

    # Signals accumulated across the window.
    startup_seen = False
    crash_marker = None      # the crash log marker found, if any
    crashed = False          # container exited during the window
    exit_code = None
    simc_seen = False
    runs_now = base_counts[PRIMARY_TABLE]

    deadline = time.time() + args.seconds
    try:
        while time.time() < deadline:
            time.sleep(args.poll)

            running, code = container_state(args.container_name)
            logs = container_logs(args.container_name)

            if STARTUP_MARKER in logs:
                startup_seen = True
            for marker in CRASH_MARKERS:
                if marker in logs:
                    crash_marker = marker
                    break
            if simc_container_ids() or (SIMC_LOG_MARKER in logs):
                simc_seen = True
            runs_now = count_rows(PRIMARY_TABLE)

            if running is None or running is False:
                crashed = True
                exit_code = code
                print(f"\nContainer is no longer running (exit code {code}) "
                      f"after {int(args.seconds - (deadline - time.time()))}s.")
                break
            if crash_marker:
                print(f"\nCrash marker '{crash_marker}' found in logs; stopping early.")
                break

            # Early exit once every REQUIRED signal is satisfied.
            rows_ok = runs_now > base_counts[PRIMARY_TABLE]
            required_ok = startup_seen and (not require_rows or (rows_ok and simc_seen))
            if required_ok:
                print("\nAll required signals satisfied; stopping early.")
                break
    finally:
        # Always surface the full logs and tear the container(s) down.
        print("\n" + "=" * 70)
        print(f"docker logs {args.container_name}:")
        print("=" * 70)
        print(container_logs(args.container_name))
        # Re-read simc siblings before cleanup so the count is accurate.
        if simc_container_ids():
            simc_seen = True
        remove_container(args.container_name)
        cleanup_simc_siblings()

    # Final counts (fresh read).
    final_counts = {t: count_rows(t) for t in (PRIMARY_TABLE, *CORROBORATING_TABLES)}
    rows_grew = final_counts[PRIMARY_TABLE] > base_counts[PRIMARY_TABLE]

    print("\n" + "=" * 70)
    print("Smoke test signals")
    print("=" * 70)
    print(f"  clean startup marker seen : {startup_seen}")
    print(f"  crashed / exited early    : {crashed}"
          + (f" (exit code {exit_code})" if crashed else ""))
    print(f"  crash marker in logs      : {crash_marker or 'none'}")
    for table in (PRIMARY_TABLE, *CORROBORATING_TABLES):
        print(f"  {table} count {base_counts[table]} -> {final_counts[table]}")
    print(f"  runs count grew           : {rows_grew}")
    print(f"  simc sibling launched     : {simc_seen}")
    print(f"  require_rows              : {require_rows}")

    # Pass/fail evaluation.
    failures = []
    if crashed:
        failures.append(f"container crash-exited during the window (exit code {exit_code})")
    if crash_marker:
        failures.append(f"crash marker '{crash_marker}' found in logs")
    if not startup_seen:
        failures.append(f"clean-startup marker never appeared ('{STARTUP_MARKER}')")
    if require_rows:
        if not rows_grew:
            failures.append("no new 'runs' rows written to the test DB "
                            "(collector failed to collect while the season has data)")
        if not simc_seen:
            failures.append("no simc sibling container was launched "
                            "(collector did not start running SimulationCraft)")
    else:
        # Off-season: log the relaxed signals but never fail on them.
        if not rows_grew:
            print("\ninfo: no new 'runs' rows (relaxed; season has no data).")
        if not simc_seen:
            print("info: no simc launch observed (relaxed; season has no data).")

    print("\n" + "=" * 70)
    if failures:
        print("COLLECTOR SMOKE TEST FAILED:")
        for f in failures:
            print(f"  - {f}")
        print("=" * 70)
        return 1
    print("COLLECTOR SMOKE TEST PASSED.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
