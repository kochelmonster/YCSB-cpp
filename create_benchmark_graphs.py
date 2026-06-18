#!/usr/bin/env python3
"""Generate comparison graphs from YCSB-cpp benchmark CSV outputs.

Charts produced:
  workload_comparison.png          — baseline scenario only, grouped bars
  batch_insert_scaling.png         — line chart: throughput vs insert batch size
  batch_update_scaling.png         — line chart: throughput vs update batch size
  value_size_scaling.png           — line chart: throughput vs value size
  acid_workload_comparison.png     — ACID workloads, best across scenarios
  concurrent_workload_comparison.png — concurrent workloads, grouped bars
"""

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
    "workload_kv_concurrent_write": "Concurrent Write (8T)",
    "workload_kv_concurrent_session": "Concurrent Session (8T)",
}

ACID_WORKLOAD_LABELS = {"ACID A/C/I", "ACID Txn"}
CONCURRENT_WORKLOAD_LABELS = {"Concurrent Write (8T)", "Concurrent Session (8T)"}

DATABASE_ORDER = ["leaves", "lmdb", "leveldb", "rocksdb", "wiredtiger", "sqlite", "redis", "badger", "dragonfly"]
COLORS = {
    "leaves": "#15616d",
    "lmdb": "#2a9d8f",
    "leveldb": "#e9c46a",
    "rocksdb": "#f4a261",
    "wiredtiger": "#e76f51",
    "sqlite": "#5c7cfa",
    "redis": "#8d99ae",
    "badger": "#9b59b6",
    "dragonfly": "#e74c3c",
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
    frame["load_throughput_ops_sec"] = pd.to_numeric(frame["load_throughput_ops_sec"])
    frame["run_throughput_ops_sec"] = pd.to_numeric(frame["run_throughput_ops_sec"])
    frame["workload_label"] = frame["workload"].map(WORKLOAD_LABELS).fillna(frame["workload"])
    # Append " DW" suffix for dedicated_writer scenarios so they appear as separate bars
    dw_mask = frame["scenario"].str.endswith("_dw")
    frame.loc[dw_mask, "workload_label"] = frame.loc[dw_mask, "workload_label"] + " DW"
    return frame


def save_pivot_csv(pivot: pd.DataFrame, output_path: Path) -> None:
    """Save a pivot DataFrame as a CSV file alongside the graph."""
    csv_path = output_path.with_suffix(".csv")
    pivot.to_csv(csv_path)


def save_grouped_bars(
    pivot: pd.DataFrame,
    output_path: Path,
    title: str,
    ylabel: str,
    *,
    sig_figs: int = 0,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 9))

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
                f"{value/1000:.{sig_figs}g}k" if sig_figs > 0 else f"{value/1000:.0f}k",
                ha="center",
                va="bottom",
                fontsize=8,
                rotation=90,
            )

    # Add 20% headroom above the tallest bar so rotated labels aren't clipped.
    ax.set_ylim(top=ax.get_ylim()[1] * 1.20)
    ax.set_title(title, fontsize=15, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Database")
    plt.tight_layout()
    plt.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def save_line_chart(
    pivot: pd.DataFrame,
    output_path: Path,
    title: str,
    ylabel: str,
    xlabel: str,
    *,
    log_scale: bool = False,
) -> None:
    """Line chart from a pivot with x-axis = index, one line per column (database)."""
    fig, ax = plt.subplots(figsize=(10, 7))

    columns = ordered_columns(pivot.columns)
    x = np.arange(len(pivot.index))

    for db in columns:
        values = pivot[db].to_numpy(dtype=float)
        marker = "o" if len(pivot.index) <= 6 else ""
        ax.plot(
            x, values, marker=marker, label=db,
            color=COLORS.get(db, None), linewidth=2, markersize=8,
        )

    ax.set_title(title, fontsize=15, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Database")
    if log_scale:
        ax.set_yscale("log")
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

    # Load full dataset
    throughput_df_all = prepare_matrix(throughput_csv)

    generated_files: list[Path] = []

    # -----------------------------------------------------------------------
    # 1. Baseline scenario chart — clean apples-to-apples comparison
    # -----------------------------------------------------------------------
    baseline_df = throughput_df_all[throughput_df_all["scenario"] == "baseline"]
    if not baseline_df.empty:
        base_run_pivot = baseline_df.pivot_table(
            index="workload_label",
            columns="database",
            values="run_throughput_ops_sec",
            aggfunc="max",
            fill_value=0,
        )
        comparison_chart = output_dir / "workload_comparison.png"
        save_grouped_bars(
            base_run_pivot,
            comparison_chart,
            "Workload Performance (Baseline — Default Settings)",
            "Run throughput (ops/sec)",
        )
        save_pivot_csv(base_run_pivot, comparison_chart)
        generated_files.append(comparison_chart)

    # -----------------------------------------------------------------------
    # 2. Batch scaling line chart — throughput vs batch size
    # -----------------------------------------------------------------------
    batch_df = throughput_df_all[throughput_df_all["scenario"].str.match(r"^batch_(insert|update)_\d+$")]
    if not batch_df.empty:
        batch_df["batch_size"] = pd.to_numeric(batch_df["batch_size"])
        batch_df["batch_op"] = batch_df["scenario"].str.extract(r"^batch_(insert|update)", expand=False)

        for op_type, op_label in [("insert", "Insert"), ("update", "Update")]:
            subset = batch_df[batch_df["batch_op"] == op_type]
            if subset.empty:
                continue
            batch_pivot = subset.pivot_table(
                index="batch_size",
                columns="database",
                values="run_throughput_ops_sec",
                aggfunc="max",
                fill_value=0,
            )
            # Sort by batch size ascending
            batch_pivot = batch_pivot.sort_index()
            chart_name = f"batch_{op_type}_scaling.png"
            chart_path = output_dir / chart_name
            save_line_chart(
                batch_pivot,
                chart_path,
                f"Batch {op_label} Scaling — Throughput vs Batch Size",
                "Run throughput (ops/sec)",
                "Batch size",
            )
            save_pivot_csv(batch_pivot, chart_path)
            generated_files.append(chart_path)

    # -----------------------------------------------------------------------
    # 3. Value size scaling line chart — throughput vs value size
    # -----------------------------------------------------------------------
    value_df = throughput_df_all[throughput_df_all["scenario"].str.match(r"^value_size_\d+$")]
    if not value_df.empty:
        value_df["value_size"] = value_df["scenario"].str.extract(r"value_size_(\d+)", expand=False).astype(int)
        value_pivot = value_df.pivot_table(
            index="value_size",
            columns="database",
            values="run_throughput_ops_sec",
            aggfunc="max",
            fill_value=0,
        )
        value_pivot = value_pivot.sort_index()
        # Rename index to human-readable labels
        value_pivot.index = [f"{s}B" for s in value_pivot.index]
        value_chart = output_dir / "value_size_scaling.png"
        save_line_chart(
            value_pivot,
            value_chart,
            "Value Size Scaling — Throughput vs Payload Size",
            "Run throughput (ops/sec)",
            "Value size (fieldlength × fieldcount)",
            log_scale=True,
        )
        save_pivot_csv(value_pivot, value_chart)
        generated_files.append(value_chart)

    # -----------------------------------------------------------------------
    # 4. ACID workloads — keep as separate chart (same as before)
    # -----------------------------------------------------------------------
    acid_df = throughput_df_all[throughput_df_all["workload_label"].isin(ACID_WORKLOAD_LABELS)]
    if not acid_df.empty:
        acid_run_pivot = acid_df.groupby(["workload_label", "database"])["run_throughput_ops_sec"].max().unstack(fill_value=0)
        acid_run_pivot = acid_run_pivot.loc[:, (acid_run_pivot > 0).any()]
        acid_chart = output_dir / "acid_workload_comparison.png"
        save_grouped_bars(
            acid_run_pivot,
            acid_chart,
            "ACID Workload Performance (Best Across All Scenarios)",
            "Run throughput (ops/sec)",
            sig_figs=3,
        )
        save_pivot_csv(acid_run_pivot, acid_chart)
        generated_files.append(acid_chart)

    # -----------------------------------------------------------------------
    # 5. Concurrent workloads — keep as separate chart (same as before)
    # -----------------------------------------------------------------------
    concurrent_df = throughput_df_all[throughput_df_all["workload_label"].isin(CONCURRENT_WORKLOAD_LABELS)]
    if not concurrent_df.empty:
        concurrent_run_pivot = concurrent_df.groupby(["workload_label", "database"])["run_throughput_ops_sec"].max().unstack(fill_value=0)
        concurrent_run_pivot = concurrent_run_pivot.loc[:, (concurrent_run_pivot > 0).any()]
        concurrent_chart = output_dir / "concurrent_workload_comparison.png"
        save_grouped_bars(
            concurrent_run_pivot,
            concurrent_chart,
            "Concurrent Workload Performance (8 threads)",
            "Run throughput (ops/sec)",
        )
        save_pivot_csv(concurrent_run_pivot, concurrent_chart)
        generated_files.append(concurrent_chart)

    print(f"Using throughput CSV: {throughput_csv}")
    print(f"Graphs written to: {output_dir}")
    print("Generated files:")
    for path in generated_files:
        print(f"  - {path.name}")


if __name__ == "__main__":
    main()