#!/usr/bin/env python3
"""
Run the full HBDB test suite with one command:

    python tests/run_all.py [--skip-cluster]

Each verify script runs in its own temporary working directory, so WAL /
snapshot files never pollute the repo and never leak between scripts.
The chaos tests (tests/chaos_monkey.py) are long-running and stay manual.
POSIX-only (uses process groups to reap spawned cluster servers).
"""

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)

# Belt and suspenders: the scripts bootstrap sys.path themselves, but a
# future script without the boilerplate should still find hbdb.
CHILD_ENV = {**os.environ, "PYTHONPATH": os.pathsep.join(
    p for p in (REPO_ROOT, os.environ.get("PYTHONPATH")) if p)}

UNIT_SUITES = [
    "test_hbdb.py",
    "test_resolver.py",
    "test_snapshot.py",
    "test_backend.py",
]

# CWD-isolated verification scripts (write transaction.log/snapshot.bin)
VERIFY_SCRIPTS = [
    "verify_durability.py",
    "verify_recovery.py",
    "verify_snapshot.py",
    "verify_truncation.py",
    "verify_wal_corruption.py",
    "verify_range.py",
    "verify_sql_index.py",
]

# Spawn local coordinator/storage subprocesses on ports 9000-9004
CLUSTER_SCRIPTS = [
    "verify_sharding.py",
    "verify_replication.py",
]


def run_script(name: str, timeout: int = 180) -> tuple:
    """Run one test script in a fresh temp CWD. Returns (ok, seconds, output)."""
    script = os.path.join(TESTS_DIR, name)
    start = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="hbdb_test_") as tmpdir:
        proc = subprocess.Popen(
            [sys.executable, script],
            cwd=tmpdir,
            env=CHILD_ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            output, _ = proc.communicate(timeout=timeout)
            ok = proc.returncode == 0
        except subprocess.TimeoutExpired:
            # Kill the whole process group: the cluster suites spawn
            # server subprocesses that would otherwise outlive the
            # timeout and keep ports 9000-9004 bound.
            os.killpg(proc.pid, signal.SIGKILL)
            output, _ = proc.communicate()
            output = (output or "") + f"\nTIMEOUT after {timeout}s"
            ok = False
    return ok, time.monotonic() - start, output


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all HBDB test suites.")
    parser.add_argument("--skip-cluster", action="store_true",
                        help="Skip the integration tests that spawn local "
                             "server subprocesses on ports 9000-9004.")
    args = parser.parse_args()

    suites = UNIT_SUITES + VERIFY_SCRIPTS
    if not args.skip_cluster:
        suites += CLUSTER_SCRIPTS

    results = []
    for name in suites:
        print(f"[run_all] {name} ... ", end="", flush=True)
        ok, secs, output = run_script(name)
        results.append((name, ok, secs))
        print(f"{'PASS' if ok else 'FAIL'} ({secs:.1f}s)")
        if not ok:
            tail = "\n".join(output.strip().splitlines()[-15:])
            print("-" * 60)
            print(tail)
            print("-" * 60)

    failed = [name for name, ok, _ in results if not ok]
    total_secs = sum(secs for _, _, secs in results)

    print()
    print("=" * 60)
    print(f"Results: {len(results) - len(failed)}/{len(results)} suites passed "
          f"in {total_secs:.1f}s")
    if failed:
        print("Failed: " + ", ".join(failed))
    print("=" * 60)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
