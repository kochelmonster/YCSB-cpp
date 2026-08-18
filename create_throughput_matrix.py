#!/usr/bin/env python3
"""Extract a throughput matrix CSV from raw YCSB-cpp benchmark log files.

Parses all .log files in a given directory (default: benchmark_results),
extracts load / run throughput from each, groups by (database, scenario, workload),
computes median run throughput across repeat runs, and writes a CSV matching
the format produced by run_all_benchmarks.sh's generate_matrix_csv().

Usage:
    python3 create_throughput_matrix.py
    python3 create_throughput_matrix.py --results-dir benchmark_results --output throughput_matrix.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Sequence


# ---- Known database names and scenario/workload labels ----
# These must match what run_all_benchmarks.sh uses.

DATABASES = [
    "rocksdb",
    "leveldb",
    "lmdb",
    "wiredtiger",
    "leaves",
    "sqlite",
    "redis",
    "badger",
    "dragonfly",
]

SCENARIOS = [
    "baseline",
    "batch_insert_1",
    "batch_insert_8",
    "batch_insert_32",
    "batch_insert_64",
    "batch_update_1",
    "batch_update_8",
    "batch_update_32",
    "batch_update_64",
    "acid_aci",
    "acid_txn",
    "concurrent_write",
    "concurrent_session",
    "value_size_100",
    "value_size_1024",
    "value_size_4096",
]

WORKLOADS = [
    "workload_kv_session",
    "workload_kv_cache",
    "workload_kv_analytics_read",
    "workload_kv_ingest",
    "workload_kv_latest",
    "workload_kv_range10",
    "workload_kv_range100",
    "workload_kv_rmw",
    "workload_kv_batch_insert",
    "workload_kv_batch_update",
    "workload_kv_acid_aci",
    "workload_kv_acid_txn",
    "workload_kv_concurrent_write",
    "workload_kv_concurrent_session",
]

# Sort by length (longest first) so regex alternation matches greedily
SCENARIOS_SORTED = sorted(SCENARIOS, key=len, reverse=True)
WORKLOADS_SORTED = sorted(WORKLOADS, key=len, reverse=True)

# ---- Regex patterns ----

# Build alternation groups from known lists
_DB_PAT = r"(?P<db>" + "|".join(re.escape(d) for d in DATABASES) + r")"
_SCEN_PAT = r"(?P<scenario>" + "|".join(re.escape(s) for s in SCENARIOS_SORTED) + r")"
_WL_PAT = r"(?P<workload>" + "|".join(re.escape(w) for w in WORKLOADS_SORTED) + r")"

FILENAME_RE = re.compile(
    rf"^{_DB_PAT}_{_SCEN_PAT}_{_WL_PAT}_(?P<phase>load|run)"
    rf"(?:_r(?P<repeat>\d+))?_"
    rf"\d{{8}}_\d{{6}}\.log$"
)

# Throughput line patterns: one for Load, one for Run
THROUGHPUT_RE = re.compile(
    r"^(?:Load|Run) throughput\(ops/sec\):\s*([\d.eE+\-]+)$",
    re.MULTILINE,
)

# Map scenario -> batch_size (same logic as run_all_benchmarks.sh)
BATCH_SIZE_MAP: dict[str, int] = {}
for s in SCENARIOS:
    m = re.match(r"(?:batch_insert|batch_update)_(\d+)", s)
    BATCH_SIZE_MAP[s] = int(m.group(1)) if m else 1


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        default="benchmark_results",
        help="Directory containing .log benchmark output files (default: benchmark_results)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path. If omitted, writes throughput_matrix_<timestamp>.csv in results-dir.",
    )
    return parser.parse_args(argv)


def extract_throughput(filepath: Path) -> float | None:
    """Read a log file and return the throughput value (ops/sec)."""
    try:
        text = filepath.read_text()
    except (OSError, UnicodeDecodeError) as exc:
        print(f"Warning: cannot read {filepath}: {exc}", file=sys.stderr)
        return None
    matches = THROUGHPUT_RE.findall(text)
    if not matches:
        return None
    # There should be exactly one match per file (either Load or Run)
    try:
        return float(matches[-1])
    except ValueError:
        return None


def compute_median(values: Sequence[float]) -> float:
    """Return the median of a list of numbers."""
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n % 2 == 1:
        return sorted_vals[n // 2]
    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    results_dir = Path(args.results_dir)
    if not results_dir.is_dir():
        raise SystemExit(f"Error: directory not found: {results_dir}")

    log_files = sorted(results_dir.glob("*.log"))
    if not log_files:
        raise SystemExit(f"No .log files found in {results_dir}")

    # Collect parsed entries:
    #   key = (db, scenario, workload)
    #   values = dict with "load_tp" and list of "run_tps"
    groups: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {"load_tp": None, "run_tps": []}
    )

    skipped = 0
    for fpath in log_files:
        m = FILENAME_RE.match(fpath.name)
        if not m:
            skipped += 1
            continue

        db = m.group("db")
        scenario = m.group("scenario")
        workload = m.group("workload")
        phase = m.group("phase")
        key = (db, scenario, workload)

        tp = extract_throughput(fpath)
        if tp is None:
            skipped += 1
            continue

        if phase == "load":
            groups[key]["load_tp"] = tp
        else:  # run
            groups[key]["run_tps"].append(tp)

    if skipped:
        print(f"Skipped {skipped} files (unrecognised name or unreadable)", file=sys.stderr)

    if not groups:
        raise SystemExit("No valid benchmark log entries were parsed.")

    # Build output rows
    rows: list[dict[str, str | int | float]] = []
    for (db, scenario, workload), data in sorted(groups.items()):
        load_tp = data["load_tp"]
        run_tps = data["run_tps"]
        if load_tp is None or not run_tps:
            # Skip groups missing load or all run data
            continue
        median_tp = compute_median(run_tps)
        rows.append({
            "scenario": scenario,
            "batch_size": BATCH_SIZE_MAP.get(scenario, 1),
            "workload": workload,
            "database": db,
            "load_throughput_ops_sec": load_tp,
            "run_throughput_ops_sec": median_tp,
        })

    if not rows:
        raise SystemExit("No complete (load + run) benchmark entries found.")

    # Determine output path
    if args.output:
        out_path = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = results_dir / f"throughput_matrix_{ts}.csv"

    # Write CSV
    fieldnames = [
        "scenario",
        "batch_size",
        "workload",
        "database",
        "load_throughput_ops_sec",
        "run_throughput_ops_sec",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} entries to {out_path}")
    print(f"Databases: {sorted(set(r['database'] for r in rows))}")
    print(f"Scenarios: {sorted(set(r['scenario'] for r in rows))}")
    print(f"Workloads: {sorted(set(r['workload'] for r in rows))}")


if __name__ == "__main__":
    main()