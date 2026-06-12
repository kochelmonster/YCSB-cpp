#!/usr/bin/env python3
"""
Merge multiple benchmark CSV files, with priority given to newer data.

Sources are auto-discovered from the benchmark_results directory by globbing
for ``throughput_matrix_*.csv``.  Files are sorted by the timestamp embedded
in their filename (newest first), so a newer run automatically takes
precedence over an older one on duplicate (scenario, workload, database) keys.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).resolve().parent / "benchmark_results"
DEFAULT_GLOB = "throughput_matrix_*.csv"
DEFAULT_OUTPUT = RESULTS_DIR / "throughput_matrix_merged.csv"

KEY_COLS = ["scenario", "workload", "database"]
REQUIRED_COLS = KEY_COLS + ["load_throughput_ops_sec", "run_throughput_ops_sec"]

# Matches an optional leading path and a timestamp like 20260523_212912
_TS_RE = re.compile(r"(\d{8}_\d{6})")


def _extract_timestamp(path: Path) -> str:
    """Return the timestamp string embedded in *path*, or '' if none found."""
    m = _TS_RE.search(path.name)
    return m.group(1) if m else ""


def _discover_sources(glob_pattern: str, results_dir: Path) -> list[Path]:
    """Find CSV files matching *glob_pattern* in *results_dir*, newest first."""
    paths = sorted(results_dir.glob(glob_pattern), key=_extract_timestamp, reverse=True)
    if not paths:
        logger.warning("No files matched glob '%s' in %s", glob_pattern, results_dir)
    return paths


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Merge benchmark throughput CSV files (newest wins on duplicates)."
    )
    p.add_argument(
        "--input-dir",
        type=Path,
        default=RESULTS_DIR,
        help="Directory containing input CSV files (default: %(default)s)",
    )
    p.add_argument(
        "--pattern",
        default=DEFAULT_GLOB,
        help="Glob pattern for input files (default: %(default)s)",
    )
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path for the merged output CSV (default: %(default)s)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing the output file.",
    )
    p.add_argument(
        "--sources",
        nargs="*",
        type=Path,
        help="Explicit list of source files (overrides --input-dir/--pattern discovery).",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = _build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stderr
    )

    # Determine source list
    if args.sources:
        sources = args.sources
        logger.info("Using %d explicitly provided source(s)", len(sources))
    else:
        sources = _discover_sources(args.pattern, args.input_dir)
        logger.info(
            "Auto-discovered %d source(s) matching '%s' in %s",
            len(sources),
            args.pattern,
            args.input_dir,
        )

    # Load frames
    frames: list[pd.DataFrame] = []
    for path in sources:
        if not path.exists():
            logger.warning("Missing %s, skipping", path)
            continue
        df = pd.read_csv(path)
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"Columns {missing} missing in {path}")
        frames.append(df)
        logger.info("Loaded %d rows from %s", len(df), path.name)

    if not frames:
        logger.error("No source files found – nothing to merge.")
        sys.exit(1)

    # Concatenate all; higher-priority (newer) frames come first.
    combined = pd.concat(frames, ignore_index=True)

    # Drop duplicates keeping first occurrence (highest priority source).
    merged = combined.drop_duplicates(subset=KEY_COLS, keep="first")
    merged = merged.sort_values(KEY_COLS).reset_index(drop=True)

    if args.dry_run:
        logger.info("Dry-run: would write %d rows to %s", len(merged), args.output)
    else:
        merged.to_csv(args.output, index=False)
        logger.info("Wrote %d rows to %s", len(merged), args.output)

    # Summary
    print("\nDatabases in merged CSV:")
    for db in sorted(merged["database"].unique()):
        rows = merged[merged["database"] == db]
        scenarios = sorted(rows["scenario"].unique())
        print(f"  {db}: {len(rows)} rows, scenarios: {', '.join(scenarios)}")


if __name__ == "__main__":
    main()