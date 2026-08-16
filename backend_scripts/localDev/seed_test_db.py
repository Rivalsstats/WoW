"""Stand up a throwaway MySQL 8, load database.sql, seed plausible test data, and build the
real aggregates -- so the page generators can be run locally between seasons when the live
DB is empty.

Pipeline:
  1. Provision a `mysql:8` Docker container (event scheduler DISABLED so the purge/wipe
     events can't touch the seed).
  2. Load backend_scripts/database.sql (schema_loader handles DELIMITER + tablespaces + the
     `Test` definer user).
  3. Introspect every base table and classify it (table_registry raises on any unknown one).
  4. Seed reference -> raw -> routes -> standalone -> control tables from data/static.
  5. CALL sp_run_agg_pipeline() (+ sp_agg_class_talent) to build every aggregated_* table.
  6. Populate the Top-Trends bar's previous week (best effort).
  7. Print the DATABASE_* exports so you can run the generators unchanged.

Usage:
  python backend_scripts/localDev/seed_test_db.py                 # fresh throwaway DB
  python backend_scripts/localDev/seed_test_db.py --runs-per-dungeon 300
  python backend_scripts/localDev/seed_test_db.py --reuse         # reprint env for a live container
  python backend_scripts/localDev/seed_test_db.py --teardown      # remove the container
"""

import argparse
import os
import random
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
REPO_ROOT = os.path.dirname(BACKEND_DIR)
# seeders imports databaseConnector/commonUtils (backend_scripts) and table_registry (here).
sys.path.insert(0, BACKEND_DIR)
sys.path.insert(0, SCRIPT_DIR)

import databaseConnector as db  # noqa: E402
import schema_loader  # noqa: E402
import seeders  # noqa: E402
from table_registry import classify_all, REFERENCE, RAW, STANDALONE, CONTROL, PIPELINE, IGNORE  # noqa: E402

DEFAULT_CONTAINER = "mythistone-testdb"
DEFAULT_HOST_PORT = 3399
ROOT_PW = "root"
DB_NAME = "Mythistone"
DEFINER_USER = "Test"
DEFINER_PW = "test"


def _run(cmd, check=True, capture=True):
    proc = subprocess.run(cmd, capture_output=capture, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\n{proc.stderr}")
    return proc


def _container_exists(name):
    out = _run(["docker", "ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}"]).stdout
    return name in out.split()


def _docker_available():
    try:
        _run(["docker", "info"])
        return True
    except Exception:
        return False


def provision_container(name, host_port):
    if _container_exists(name):
        print(f"Removing existing container {name}...")
        _run(["docker", "rm", "-f", name])
    print(f"Starting {name} (mysql:8) on host port {host_port}...")
    _run([
        "docker", "run", "-d", "--name", name,
        "-e", f"MYSQL_ROOT_PASSWORD={ROOT_PW}",
        "-e", f"MYSQL_DATABASE={DB_NAME}",
        "-p", f"{host_port}:3306",
        "mysql:8",
        "--event-scheduler=DISABLED",
    ])


def wait_for_mysql(name, timeout=120):
    print("Waiting for MySQL to accept connections...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        proc = _run(["docker", "exec", name, "mysqladmin", "ping", f"-p{ROOT_PW}", "--silent"],
                    check=False)
        if proc.returncode == 0 and "alive" in proc.stdout.lower():
            time.sleep(2)  # a beat past first ping so the server is fully ready for TCP
            print("MySQL is up.")
            return
        time.sleep(2)
    raise RuntimeError("MySQL did not become ready in time")


def connect(host_port):
    db.init_connection_pool("127.0.0.1", DEFINER_USER, DEFINER_PW, DB_NAME, str(host_port),
                            pool_size=4)
    conn = db.get_connection()
    cur = conn.cursor()
    return conn, cur


def list_base_tables(conn, cur):
    rows = db.fetch_with_retry(conn, cur,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_type = 'BASE TABLE'", (DB_NAME,))
    return [r[0] for r in rows]


def run_pipeline(conn, cur):
    print("Running aggregation pipeline (sp_run_agg_pipeline)...")
    cur.execute("CALL sp_run_agg_pipeline()")
    while cur.nextset():
        pass
    # class_talent only runs on Tuesdays inside the pipeline; force it so its aggregate exists.
    cur.execute("CALL sp_agg_class_talent()")
    while cur.nextset():
        pass
    db.commit_with_retry(conn)
    # surface any per-step errors the pipeline logged
    rows = db.fetch_with_retry(conn, cur,
        "SELECT step, error FROM agg_pipeline_log WHERE error IS NOT NULL "
        "AND error NOT LIKE '[ok after%' ORDER BY id")
    if rows:
        print("  WARNING: aggregation steps reported errors:")
        for step, err in rows:
            print(f"    - {step}: {err}")
    else:
        print("  pipeline completed with no step errors.")


def seed_trends(env, conn, cur, skip):
    if skip:
        print("Skipping Top-Trends seeding (--skip-trends).")
        return
    print("Seeding Top-Trends previous week...")
    try:
        proc = subprocess.run([sys.executable, os.path.join(BACKEND_DIR, "snapshotTrends.py")],
                              env=env, capture_output=True, text=True, cwd=REPO_ROOT)
        if proc.returncode != 0:
            print(f"  snapshotTrends.py failed (bar will stay hidden):\n{proc.stderr.strip()[:500]}")
            return
        seeders.duplicate_latest_trend_week(conn, cur)
    except Exception as exc:  # trends are optional; never fail the whole seed over them
        print(f"  trend seeding skipped: {exc}")


def build_env(host_port):
    env = dict(os.environ)
    env.update({
        "DATABASE_HOST": "127.0.0.1",
        "DATABASE_USER": DEFINER_USER,
        "DATABASE_PASSWORD": DEFINER_PW,
        "DATABASE_NAME": DB_NAME,
        "DATABASE_PORT": str(host_port),
    })
    return env


def print_env_exports(host_port):
    print("\n" + "=" * 70)
    print("Test DB ready. Point the generators at it with these env vars:\n")
    exports = {
        "DATABASE_HOST": "127.0.0.1", "DATABASE_USER": DEFINER_USER,
        "DATABASE_PASSWORD": DEFINER_PW, "DATABASE_NAME": DB_NAME,
        "DATABASE_PORT": str(host_port),
    }
    print("cmd:")
    for k, v in exports.items():
        print(f"  set {k}={v}")
    print("\nPowerShell:")
    for k, v in exports.items():
        print(f'  $env:{k}="{v}"')
    print("\nbash:")
    for k, v in exports.items():
        print(f'  export {k}={v}')
    print("\nThen e.g.:")
    print("  python backend_scripts/generateSpecPages.py --template templates/spec_page.html \\")
    print("    --output_dir classes --CLIENT_ID <blizz_id> --CLIENT_SECRET <blizz_secret>")
    print("  python backend_scripts/generateDungeonPages.py --template templates/dungeon_page.html --output_dir dungeons")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Seed a throwaway MySQL for local page renders")
    parser.add_argument("--runs-per-dungeon", type=int, default=150)
    parser.add_argument("--routes-per-dungeon", type=int, default=20)
    parser.add_argument("--top-player-ranks", type=int, default=12)
    parser.add_argument("--simc-bis-ranks", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1337, help="RNG seed for reproducible data")
    parser.add_argument("--container-name", default=DEFAULT_CONTAINER)
    parser.add_argument("--host-port", type=int, default=DEFAULT_HOST_PORT)
    parser.add_argument("--lookup-dir", default=os.path.join(REPO_ROOT, "data", "static"))
    parser.add_argument("--reuse", action="store_true",
                        help="Container already seeded; just reprint the env exports")
    parser.add_argument("--teardown", action="store_true", help="Remove the container and exit")
    parser.add_argument("--skip-trends", action="store_true")
    args = parser.parse_args()

    if not _docker_available():
        sys.exit("Docker is not available. Start Docker Desktop and retry.")

    if args.teardown:
        if _container_exists(args.container_name):
            _run(["docker", "rm", "-f", args.container_name])
            print(f"Removed {args.container_name}.")
        else:
            print(f"No container named {args.container_name}.")
        return

    if args.reuse:
        if not _container_exists(args.container_name):
            sys.exit(f"No container named {args.container_name} to reuse.")
        print_env_exports(args.host_port)
        return

    provision_container(args.container_name, args.host_port)
    wait_for_mysql(args.container_name)

    sql_path = os.path.join(BACKEND_DIR, "database.sql")
    print("Loading schema from database.sql...")
    schema_loader.load_schema(args.container_name, ROOT_PW, DB_NAME, sql_path)

    conn, cur = connect(args.host_port)

    # The guarantee: every table must be classifiable, or we stop before seeding.
    tables = list_base_tables(conn, cur)
    buckets = classify_all(tables)
    print(f"Classified {len(tables)} tables: "
          f"{len(buckets[REFERENCE])} reference, {len(buckets[RAW])} raw, "
          f"{len(buckets[STANDALONE])} standalone, {len(buckets[CONTROL])} control, "
          f"{len(buckets[PIPELINE])} pipeline-built, {len(buckets[IGNORE])} ignored.")

    rng = random.Random(args.seed)
    static = seeders.StaticData(args.lookup_dir)
    print(f"Season {static.season}, {len(static.specs)} specs, {len(static.dungeons)} dungeons.")

    cfg = {
        "runs_per_dungeon": args.runs_per_dungeon,
        "routes_per_dungeon": args.routes_per_dungeon,
        "top_player_ranks": args.top_player_ranks,
        "simc_bis_ranks": args.simc_bis_ranks,
    }

    print("Building bounded item/talent/enchant pools...")
    pools = seeders.build_pools(static, rng)

    ref = seeders.seed_reference(conn, cur, static, rng)
    seeders.seed_runs(conn, cur, static, rng, cfg, pools)
    seeders.seed_routes(conn, cur, static, rng, cfg, ref)
    seeders.seed_standalone(conn, cur, static, rng, cfg, pools)
    seeders.seed_control(conn, cur, static)

    run_pipeline(conn, cur)
    seed_trends(build_env(args.host_port), conn, cur, args.skip_trends)

    cur.close()
    conn.close()
    print_env_exports(args.host_port)


if __name__ == "__main__":
    main()
