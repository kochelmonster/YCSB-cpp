#!/usr/bin/env python3
"""Generate comparison graphs from YCSB-cpp benchmark CSV outputs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


WORKLOAD_LABELS = {
    "workload_kv_session": "Session",
    "workload_kv_cache": "Cache",
    "workload_kv_analytics_read": "Analytics Read",
    "workload_kv_ingest": "Ingest",
    "workload_kv_latest": "Latest",
    "workload_kv_range10": "Range 10",
    "workload_kv_range100": "Range 100",
    "workload_kv_rmw": "RMW",
    "workload_kv_batch_insert": "Batch Insert",
    "workload_kv_batch_update": "Batch Update",
    "workload_kv_acid_aci": "ACID A/C/I",
    "workload_kv_acid_txn": "ACID Txn",
}

ACID_WORKLOAD_LABELS = {"ACID A/C/I", "ACID Txn"}

DATABASE_ORDER = ["leaves", "lmdb", "leveldb", "rocksdb", "wiredtiger", "sqlite", "redis"]
COLORS = {
    "leaves": "#15616d",
    "lmdb": "#2a9d8f",
    "leveldb": "#e9c46a",
    "rocksdb": "#f4a261",
    "wiredtiger": "#e76f51",
    "sqlite": "#5c7cfa",
    "redis": "#8d99ae",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        default="benchmark_results",
        help="Directory containing throughput_matrix_*.csv and durability_session_matrix_*.csv",
    )
    parser.add_argument(
        "--throughput-csv",
        default=None,
        help="Path to a throughput_matrix CSV. Defaults to the newest one in results-dir.",
    )
    parser.add_argument(
        "--durability-csv",
        default=None,
        help="Path to a durability_session_matrix CSV. Defaults to the newest one in results-dir if present.",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmark_graphs",
        help="Directory for generated graph images",
    )
    parser.add_argument(
        "--scenario",
        default="baseline",
        help="Scenario name to graph when CSV contains scenario column (default: baseline)",
    )
    return parser.parse_args()


def latest_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern))
    if not matches:
        return None
    return matches[-1]


def choose_input_file(explicit: str | None, directory: Path, pattern: str) -> Path | None:
    if explicit:
        return Path(explicit)
    return latest_file(directory, pattern)


def ordered_columns(columns: Iterable[str]) -> list[str]:
    columns = list(columns)
    preferred = [name for name in DATABASE_ORDER if name in columns]
    remaining = sorted(name for name in columns if name not in preferred)
    return preferred + remaining


def prepare_matrix(csv_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(csv_path)
    if "scenario" not in frame.columns:
        frame["scenario"] = "baseline"
    if "batch_size" not in frame.columns:
        frame["batch_size"] = 1
    if "binary_key" not in frame.columns:
        frame["binary_key"] = False
    frame["load_throughput_ops_sec"] = pd.to_numeric(frame["load_throughput_ops_sec"])
    frame["run_throughput_ops_sec"] = pd.to_numeric(frame["run_throughput_ops_sec"])
    frame["workload_label"] = frame["workload"].map(WORKLOAD_LABELS).fillna(frame["workload"])
    return frame


def save_grouped_bars(
    pivot: pd.DataFrame,
    output_path: Path,
    title: str,
    ylabel: str,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 7))

    x = np.arange(len(pivot.index))
    columns = ordered_columns(pivot.columns)
    width = 0.82 / max(len(columns), 1)

    for idx, db in enumerate(columns):
        heights = pivot[db].to_numpy()
        offset = (idx - (len(columns) - 1) / 2) * width
        bars = ax.bar(
            x + offset,
            heights,
            width,
            label=db,
            color=COLORS.get(db, None),
            alpha=0.9,
        )
        for bar, value in zip(bars, heights):
            if value <= 0:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                value + max(pivot.max()) * 0.01,
                f"{value/1000:.0f}k",
                ha="center",
                va="bottom",
                fontsize=8,
                rotation=90,
            )

    ax.set_title(title, fontsize=15, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Database")
    plt.tight_layout()
    plt.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    throughput_csv = choose_input_file(args.throughput_csv, results_dir, "throughput_matrix_*.csv")

    if throughput_csv is None or not throughput_csv.exists():
        raise SystemExit("No throughput_matrix CSV found. Run the benchmark matrix first.")

    # Load full dataset for all-workloads chart (includes all scenarios)
    throughput_df_all = prepare_matrix(throughput_csv)

    generated_files: list[Path] = []

    # Generate all-workloads comparison: aggregate across scenarios to show best performance per workload
    # Group by workload and database, taking max throughput across scenarios
    full_run_pivot = throughput_df_all.groupby(['workload_label', 'database'])['run_throughput_ops_sec'].max().unstack(fill_value=0)
    all_run_pivot = full_run_pivot.loc[
        [label for label in full_run_pivot.index if label not in ACID_WORKLOAD_LABELS]
    ]
    
    comparison_chart = output_dir / "workload_comparison.png"
    save_grouped_bars(
        all_run_pivot,
        comparison_chart,
        "Workload Performance Comparison (Best Across All Scenarios)",
        "Run throughput (ops/sec)",
    )
    generated_files.append(comparison_chart)

    full_avg_run_pivot = throughput_df_all.groupby(["workload_label", "database"])["run_throughput_ops_sec"].mean().unstack(fill_value=0)
    avg_run_pivot = full_avg_run_pivot.loc[
        [label for label in full_avg_run_pivot.index if label not in ACID_WORKLOAD_LABELS]
    ]
    average_chart = output_dir / "workload_comparison_average.png"
    save_grouped_bars(
        avg_run_pivot,
        average_chart,
        "Workload Performance Comparison (Average Across All Scenarios)",
        "Run throughput (ops/sec)",
    )
    generated_files.append(average_chart)

    # Generate a focused acid-only chart so low-throughput bars are readable.
    acid_run_pivot = full_run_pivot.loc[
        [label for label in full_run_pivot.index if label in ACID_WORKLOAD_LABELS]
    ]
    if not acid_run_pivot.empty:
        acid_chart = output_dir / "acid_workload_comparison.png"
        save_grouped_bars(
            acid_run_pivot,
            acid_chart,
            "ACID Workload Performance Comparison (Best Across All Scenarios)",
            "Run throughput (ops/sec)",
        )
        generated_files.append(acid_chart)

    acid_avg_run_pivot = full_avg_run_pivot.loc[
        [label for label in full_avg_run_pivot.index if label in ACID_WORKLOAD_LABELS]
    ]
    if not acid_avg_run_pivot.empty:
        acid_average_chart = output_dir / "acid_workload_comparision_average.png"
        save_grouped_bars(
            acid_avg_run_pivot,
            acid_average_chart,
            "ACID Workload Performance Comparison (Average Across All Scenarios)",
            "Run throughput (ops/sec)",
        )
        generated_files.append(acid_average_chart)

    print(f"Using throughput CSV: {throughput_csv}")
    print(f"Graphs written to: {output_dir}")
    print("Generated files:")
    for path in generated_files:
        print(f"  - {path.name}")


if __name__ == "__main__":
    main()
