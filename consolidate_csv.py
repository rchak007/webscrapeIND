#!/usr/bin/env python3
"""
Consolidate all per_village CSVs into one combined CSV.
=========================================================
Scans data/per_village/*.csv, combines them, and writes data/all_villages.csv

Usage:
    python consolidate_csv.py
    python consolidate_csv.py --input-dir ./data/per_village --output ./data/all_villages.csv
"""

import os
import sys
import argparse
import time
from datetime import timedelta

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Consolidate per-village CSVs into one file")
    parser.add_argument("--input-dir", type=str, default="./data/per_village",
                        help="Directory containing per-village CSV files (default: ./data/per_village)")
    parser.add_argument("--output", type=str, default="./data/all_villages.csv",
                        help="Output CSV path (default: ./data/all_villages.csv)")
    args = parser.parse_args()

    input_dir = args.input_dir
    output_path = args.output

    if not os.path.exists(input_dir):
        print(f"ERROR: Directory not found: {input_dir}")
        sys.exit(1)

    # Find all CSV files
    csv_files = sorted([f for f in os.listdir(input_dir) if f.endswith(".csv")])
    print(f"Found {len(csv_files)} CSV files in {input_dir}")

    if not csv_files:
        print("No CSV files found. Nothing to do.")
        sys.exit(0)

    start = time.time()
    all_dfs = []
    skipped = 0
    error_files = []

    for i, filename in enumerate(csv_files, 1):
        filepath = os.path.join(input_dir, filename)
        try:
            df = pd.read_csv(filepath, encoding="utf-8-sig")
            if len(df) > 0:
                all_dfs.append(df)
            else:
                skipped += 1
        except Exception as e:
            error_files.append((filename, str(e)))
            print(f"  ⚠ Error reading {filename}: {e}")

        if i % 500 == 0:
            rows_so_far = sum(len(d) for d in all_dfs)
            print(f"  Processed {i}/{len(csv_files)} files ({rows_so_far} rows so far)...")

    if not all_dfs:
        print("No data found in any CSV files.")
        sys.exit(0)

    print(f"\nConcatenating {len(all_dfs)} dataframes...")
    combined = pd.concat(all_dfs, ignore_index=True)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    print(f"Writing {len(combined)} rows to {output_path}...")
    combined.to_csv(output_path, index=False, encoding="utf-8-sig")

    elapsed = time.time() - start
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)

    print(f"\n{'=' * 60}")
    print(f"  CONSOLIDATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Files processed:  {len(csv_files)}")
    print(f"  Files with data:  {len(all_dfs)}")
    print(f"  Files empty:      {skipped}")
    print(f"  Files with errors: {len(error_files)}")
    print(f"  Total rows:       {len(combined):,}")
    print(f"  Total columns:    {len(combined.columns)}")
    print(f"  Columns:          {list(combined.columns)}")
    print(f"  Output file:      {output_path}")
    print(f"  File size:        {file_size_mb:.1f} MB")
    print(f"  Time taken:       {timedelta(seconds=int(elapsed))}")
    print(f"{'=' * 60}")

    # Quick stats
    if "_district" in combined.columns:
        print(f"\n  Districts:  {combined['_district'].nunique()}")
    if "_mandal" in combined.columns:
        print(f"  Mandals:    {combined[['_district', '_mandal']].drop_duplicates().shape[0]}")
    if "_village" in combined.columns:
        print(f"  Villages:   {combined[['_district', '_mandal', '_village']].drop_duplicates().shape[0]}")
    print(f"  Total rows: {len(combined):,}")

    if error_files:
        print(f"\n  ⚠ Files with errors:")
        for fname, err in error_files[:10]:
            print(f"    {fname}: {err}")
        if len(error_files) > 10:
            print(f"    ... and {len(error_files) - 10} more")


if __name__ == "__main__":
    main()