#!/usr/bin/env python3
"""
Merge multiple benchmark CSV files, with priority given to newer data.

Priority (highest to lowest):
  1. BATCH-FIX (20260523_212912): batch_insert/batch_update with binary_key=true fix
  2. NEW run  (20260523_191726): leaves/rocksdb/leveldb/lmdb/wiredtiger x 6 scenarios
  3. POST-FIX (20260522_191322): leaves concurrent_session / concurrent_session_dw
  4. APRIL    (20260405_combined): everything else (sqlite/redis/badger/dragonfly, batch 1/8/32, concurrent_write...)
"""
import pandas as pd
import sys
import os

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "benchmark_results")

# Sources in priority order (first wins on duplicate keys)
SOURCES = [
    os.path.join(RESULTS_DIR, "throughput_matrix_20260523_212912.csv"),
    os.path.join(RESULTS_DIR, "throughput_matrix_20260523_191726.csv"),
    os.path.join(RESULTS_DIR, "throughput_matrix_20260522_191322.csv"),
    os.path.join(RESULTS_DIR, "throughput_matrix_20260405_combined.csv"),
]

OUTPUT = os.path.join(RESULTS_DIR, "throughput_matrix_merged.csv")

KEY_COLS = ["scenario", "workload", "database"]


def main():
    frames = []
    for path in SOURCES:
        if not os.path.exists(path):
            print(f"WARNING: missing {path}, skipping", file=sys.stderr)
            continue
        df = pd.read_csv(path)
        # Ensure expected columns exist
        for col in KEY_COLS + ["load_throughput_ops_sec", "run_throughput_ops_sec"]:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' missing in {path}")
        frames.append(df)
        print(f"Loaded {len(df)} rows from {os.path.basename(path)}")

    if not frames:
        print("ERROR: no source files found", file=sys.stderr)
        sys.exit(1)

    # Concatenate all; higher-priority frames come first.
    combined = pd.concat(frames, ignore_index=True)

    # Drop duplicates keeping first occurrence (highest priority source).
    merged = combined.drop_duplicates(subset=KEY_COLS, keep="first")

    merged = merged.sort_values(["database", "scenario", "workload"]).reset_index(drop=True)

    merged.to_csv(OUTPUT, index=False)
    print(f"\nMerged {len(merged)} rows -> {OUTPUT}")

    # Summary
    print("\nDatabases in merged CSV:")
    for db in sorted(merged["database"].unique()):
        rows = merged[merged["database"] == db]
        scenarios = sorted(rows["scenario"].unique())
        print(f"  {db}: {len(rows)} rows, scenarios: {', '.join(scenarios)}")


if __name__ == "__main__":
    main()
