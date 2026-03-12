# -*- coding: utf-8 -*-
"""
count_rows.py

Считает количество строк в каждом CSV-файле из data_raw/.
Создаёт таблицу row_counts.csv с результатами.

Формат вывода:
filename, rows
"""

import pandas as pd
from pathlib import Path


DATA_DIR = Path("data_raw")
OUTPUT_FILE = Path("row_counts.csv")


def main():
    if not DATA_DIR.exists():
        print(f"❌ Folder not found: {DATA_DIR}")
        return

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found in {DATA_DIR}")
        return

    results = []
    total_rows = 0

    print("Counting rows in all CSV files...\n")

    for path in csv_files:
        try:
            # Count rows efficiently without loading full dataframe into memory
            with path.open("r", encoding="utf-8") as f:
                row_count = sum(1 for _ in f) - 1  # minus header
        except Exception as e:
            print(f"⚠️ Error reading {path.name}: {e}")
            continue

        total_rows += max(row_count, 0)
        results.append({"filename": path.name, "rows": max(row_count, 0)})
        print(f"{path.name:45} → {row_count:,} rows")

    # Save result to CSV
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n====================================")
    print(f"Total rows across all files: {total_rows:,}")
    print(f"Results saved to: {OUTPUT_FILE}")
    print("====================================")


if __name__ == "__main__":
    main()
