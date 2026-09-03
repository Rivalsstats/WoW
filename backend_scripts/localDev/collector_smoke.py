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

Leaderboard-write signal, required ONLY with --require-rows true (the live
season has data, decided upstream by seasonHasData.py):
  * the 'runs' COUNT(*) in the test DB grew over the pre-run baseline
    (the collector actually collected and wrote leaderboard rows).

SimulationCraft signal, required ONLY with --require-simc true (real gear/talents
for a spec were seeded from the live DB by seed_test_db.py --simc-live-spec, so a
valid profile can be built):
  * at least one spec's simc chunk SUCCEEDED, detected as a fresh simc_bis_meta
    row with baseline_dps > 0 (or new simc_bis_items rows) written to the test DB
    during the window. A launched simc sibling container (docker label
    mythistone.role=simc-sim) / a 'simc:' log line is kept as a corroborating
    signal but is NOT sufficient on its own.

When a requirement's flag is false (off-season, or no real spec data was seeded)
its signal is logged for information only and never fails the run, mirroring how
buildPages skips itself when the season has no data.

Usage (Docker Desktop / CI; test DB already seeded, its DATABASE_* exported,
and the runtime env below present in this process's environment):

    python backend_scripts/localDev/collector_smoke.py \\
        --image mythistone-collector:smoke --seconds 360 \\
        --require-rows true --require-simc true

Exit code 0 = pass, non-zero = fail. The container's full logs are always
printed, and the container plus any simc sibling containers are always stopped
and removed, before the script returns.
"""

import argparse
import json
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

# Current-season dungeon set the collector now filters top-player loadouts
# against: data/static/dungeons.json keys ARE the current map_challenge_mode_ids,
# and the collector stores a loadout only for these dungeons (off-rotation
# dungeons a top player still has runs in would otherwise pollute
# top_player_loadouts and skew the spec-page hero-tree badge). The smoke test
# reads the SAME file the image ships, so a regressed filter that writes an
# off-rotation loadout row fails this gate.
DUNGEON_STATIC = os.path.join(os.path.dirname(BACKEND_DIR), "data", "static", "dungeons.json")


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


def simc_success_snapshot():
    """Snapshot the set of (spec_id, season, updated_at) for simc_bis_meta rows
    with baseline_dps > 0, using a FRESH pooled connection (same REPEATABLE READ
    reasoning as count_rows).

    The collector deletes+reinserts a spec's meta row with a fresh updated_at when
    a chunk succeeds, so any tuple absent from the baseline snapshot means a real
    simc success landed during the window. Comparing DB-written timestamps to each
    other (never to the host clock) keeps this immune to clock skew."""
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        rows = db.fetch_with_retry(
            conn, cur,
            "SELECT spec_id, season, updated_at FROM simc_bis_meta WHERE baseline_dps > 0")
        cur.close()
        return {(r[0], r[1], str(r[2])) for r in rows}
    finally:
        conn.close()


def current_dungeon_ids():
    """The current-season map_challenge_mode_ids (keys of data/static/dungeons.json)."""
    with open(DUNGEON_STATIC, "r", encoding="utf-8") as f:
        return {int(k) for k in json.load(f).keys()}


def offrotation_loadout_ids(current_ids):
    """Distinct top_player_loadouts.map_challenge_mode_id NOT in the current
    dungeon set, on a FRESH pooled connection (same REPEATABLE READ reasoning as
    count_rows). The collector must never write an off-rotation dungeon loadout,
    so any id here that was absent at baseline means its current-dungeon filter
    regressed."""
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        rows = db.fetch_with_retry(
            conn, cur, "SELECT DISTINCT map_challenge_mode_id FROM top_player_loadouts")
        cur.close()
        return {int(r[0]) for r in rows
                if r[0] is not None and int(r[0]) not in current_ids}
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
    # Blizzard creds are per-region (BLIZ_CLIENT_ID_US, ..._EU, ...) and which
    # ones exist depends on REGIONS, so they cannot live in a fixed list.
    # Forward every explicitly-set one by prefix; the entrypoint requires the
    # pair for each region in REGIONS, and a genuinely absent pair correctly
    # makes it exit 2 (which this script reports as a crash).
    forwarded_bliz = set()
    for var in sorted(os.environ):
        if var.startswith("BLIZ_CLIENT_ID_") or var.startswith("BLIZ_CLIENT_SECRET_"):
            cmd += ["-e", var]  # value taken from this process's environment
            forwarded_bliz.add(var)
    # The repo only has ONE region-agnostic Blizzard pair (BLIZ_CLIENT_ID /
    # BLIZ_CLIENT_SECRET). The collector wants per-region names, so derive them
    # here from that single pair for each region in REGIONS. An explicitly-set
    # suffixed var above always wins (never overwritten / double-added).
    regions = [r.strip().upper() for r in os.environ.get("REGIONS", "us").split(",") if r.strip()]
    for region in regions:
        for base in ("BLIZ_CLIENT_ID", "BLIZ_CLIENT_SECRET"):
            suffixed = f"{base}_{region}"
            if suffixed not in forwarded_bliz and os.environ.get(base) is not None:
                cmd += ["-e", f"{suffixed}={os.environ[base]}"]
                forwarded_bliz.add(suffixed)
    cmd.append(args.image)
    return cmd


def _bool_arg(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--image", required=True,
                        help="Collector image tag to run (e.g. mythistone-collector:smoke)")
    parser.add_argument("--seconds", type=int, default=360,
                        help="Max seconds to watch the collector before deciding (default 360, "
                             "enough for spec 62 to complete one tiny simc chunk)")
    parser.add_argument("--require-rows", default="false",
                        help="true|false: require a runs-count increase "
                             "(set from seasonHasData.py has_data)")
    parser.add_argument("--require-simc", default="false",
                        help="true|false: require at least one real simc chunk to SUCCEED "
                             "(set true only when seed_test_db.py --simc-live-spec seeded real "
                             "gear/talents, i.e. its simc_live_seeded output)")
    parser.add_argument("--container-name", default="collector-smoke",
                        help="Name for the collector container (default collector-smoke)")
    parser.add_argument("--simc-volume", default="mythistone_simc_io_smoke",
                        help="Named docker volume shared with simc siblings at /app/data/simc_io")
    parser.add_argument("--poll", type=int, default=5,
                        help="Seconds between polls of the container/DB (default 5)")
    args = parser.parse_args()
    require_rows = _bool_arg(args.require_rows)
    require_simc = _bool_arg(args.require_simc)

    if not _docker_available():
        sys.exit("Docker is not available. Start Docker Desktop and retry.")

    _init_db()

    cur_dungeon_ids = current_dungeon_ids()
    base_offrotation = offrotation_loadout_ids(cur_dungeon_ids)
    base_counts = {t: count_rows(t) for t in (PRIMARY_TABLE, *CORROBORATING_TABLES)}
    base_simc = simc_success_snapshot()
    print("Baseline test-DB row counts:")
    for table, n in base_counts.items():
        print(f"  {table}: {n}")
    print(f"Baseline simc_bis_meta rows (baseline_dps>0): {len(base_simc)}")
    print(f"Baseline off-rotation top_player_loadouts dungeon ids: "
          f"{sorted(base_offrotation) or 'none'}")
    print(f"require_rows={require_rows} (runs-growth is "
          f"{'REQUIRED' if require_rows else 'informational only'})")
    print(f"require_simc={require_simc} (a real simc success is "
          f"{'REQUIRED' if require_simc else 'informational only'})")

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
    simc_seen = False        # corroborating: a simc sibling launched / log line
    simc_success = False     # hard: a fresh simc_bis result row landed
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
            if simc_success_snapshot() - base_simc:
                simc_success = True
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
            required_ok = (startup_seen
                           and (not require_rows or rows_ok)
                           and (not require_simc or simc_success))
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

    # Final reads (fresh connections).
    final_counts = {t: count_rows(t) for t in (PRIMARY_TABLE, *CORROBORATING_TABLES)}
    rows_grew = final_counts[PRIMARY_TABLE] > base_counts[PRIMARY_TABLE]
    if simc_success_snapshot() - base_simc:  # a chunk may have landed just before shutdown
        simc_success = True
    # Any off-rotation dungeon id that appeared during the window means the
    # collector's current-dungeon filter regressed (the seeder writes only
    # current dungeons, so a new id can only come from this collector run).
    new_offrotation = offrotation_loadout_ids(cur_dungeon_ids) - base_offrotation

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
    print(f"  simc sibling launched     : {simc_seen} (corroborating)")
    print(f"  simc chunk succeeded (DB) : {simc_success}")
    print(f"  new off-rotation dungeons : {sorted(new_offrotation) or 'none'}")
    print(f"  require_rows              : {require_rows}")
    print(f"  require_simc              : {require_simc}")

    # Pass/fail evaluation.
    failures = []
    if crashed:
        failures.append(f"container crash-exited during the window (exit code {exit_code})")
    if crash_marker:
        failures.append(f"crash marker '{crash_marker}' found in logs")
    if not startup_seen:
        failures.append(f"clean-startup marker never appeared ('{STARTUP_MARKER}')")
    if require_rows and not rows_grew:
        failures.append("no new 'runs' rows written to the test DB "
                        "(collector failed to collect while the season has data)")
    elif not require_rows and not rows_grew:
        print("\ninfo: no new 'runs' rows (relaxed; season has no data).")
    if require_simc and not simc_success:
        failures.append("no simc chunk succeeded (no fresh simc_bis_meta row with "
                        "baseline_dps>0) despite real gear/talents being seeded "
                        f"(simc sibling launched={simc_seen})")
    elif not require_simc and not simc_success:
        print("info: no simc success observed (relaxed; no real spec data seeded).")
    if new_offrotation:
        failures.append(
            "collector wrote off-rotation top_player_loadouts rows for dungeon(s) "
            f"{sorted(new_offrotation)} not in data/static/dungeons.json "
            "(current-dungeon filter regressed)")

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
