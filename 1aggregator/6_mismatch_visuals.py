# -*- coding: utf-8 -*-
"""
6_mismatch_visuals.py

Builds PNG graphs from:
- mismatch_summary.md
- mismatch_by_file.csv
- mismatch_by_year.csv
- mismatch_top_all.csv

Outputs:
- mismatch_by_file.png
- mismatch_by_year.png
- mismatch_top20.png
- mismatch_summary.png
"""

from pathlib import Path
import csv
import re

import matplotlib.pyplot as plt


ROOT = Path("..").resolve()
BASE = ROOT / "1aggregator"

SUMMARY_MD = BASE / "mismatch_summary.md"
BY_FILE = BASE / "mismatch_by_file.csv"
BY_YEAR = BASE / "mismatch_by_year.csv"
TOP_ALL = BASE / "mismatch_top_all.csv"

OUT_BY_FILE = BASE / "mismatch_by_file.png"
OUT_BY_YEAR = BASE / "mismatch_by_year.png"
OUT_TOP20 = BASE / "mismatch_top20.png"
OUT_SUMMARY = BASE / "mismatch_summary.png"


def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def plot_by_file(rows):
    if not rows:
        return
    files = [r["file"] for r in rows]
    shares = [float(r["mismatch_share_pct"]) for r in rows]

    plt.figure(figsize=(10, 5))
    plt.bar(files, shares)
    plt.title("Mismatch share by file (%, higher is worse)")
    plt.xlabel("file")
    plt.ylabel("mismatch_share_pct")
    plt.xticks(rotation=30, ha="right", fontsize=8)
    plt.tight_layout()
    plt.savefig(OUT_BY_FILE, dpi=160)
    plt.close()


def plot_by_year(rows):
    if not rows:
        return
    years = [int(r["year"]) for r in rows]
    shares = [float(r["mismatch_share_pct"]) for r in rows]

    plt.figure(figsize=(8, 4))
    plt.plot(years, shares, marker="o")
    plt.title("Mismatch share by year (%)")
    plt.xlabel("year")
    plt.ylabel("mismatch_share_pct")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_BY_YEAR, dpi=160)
    plt.close()


def plot_top20(rows):
    if not rows:
        return
    top = rows[:20]
    codes = [r["code"] for r in top]
    counts = [int(r["count"]) for r in top]

    plt.figure(figsize=(10, 5))
    plt.bar(codes, counts)
    plt.title("Top-20 mismatch codes by frequency")
    plt.xlabel("code")
    plt.ylabel("count")
    plt.xticks(rotation=30, ha="right", fontsize=8)
    plt.tight_layout()
    plt.savefig(OUT_TOP20, dpi=160)
    plt.close()


def plot_summary(md_text: str):
    lines = [ln.strip() for ln in md_text.splitlines() if ln.strip().startswith("- ")]
    # turn "- Key: Value" into "Key: Value"
    items = [ln[2:] for ln in lines]

    plt.figure(figsize=(8, 4))
    plt.axis("off")
    text = "Mismatch Summary\n\n" + "\n".join(items)
    plt.text(0.01, 0.98, text, va="top", fontsize=11)
    plt.tight_layout()
    plt.savefig(OUT_SUMMARY, dpi=160)
    plt.close()


def main():
    if BY_FILE.exists():
        plot_by_file(read_csv_rows(BY_FILE))
    if BY_YEAR.exists():
        plot_by_year(read_csv_rows(BY_YEAR))
    if TOP_ALL.exists():
        plot_top20(read_csv_rows(TOP_ALL))
    if SUMMARY_MD.exists():
        plot_summary(SUMMARY_MD.read_text(encoding="utf-8"))

    print("✅ Written:")
    if OUT_BY_FILE.exists():
        print(f"- {OUT_BY_FILE}")
    if OUT_BY_YEAR.exists():
        print(f"- {OUT_BY_YEAR}")
    if OUT_TOP20.exists():
        print(f"- {OUT_TOP20}")
    if OUT_SUMMARY.exists():
        print(f"- {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
