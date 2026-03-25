# -*- coding: utf-8 -*-
"""
2_mirror_mismatch_visuals_all.py

Build all main graphs from:
  - mirror_mismatch_all.csv -> mirror_mismatch_graphs_all/
  - mirror_mismatch_all_zero_inclusive.csv -> mirror_mismatch_graphs_all_zero_inclusive/
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


BASE_DIR = Path(__file__).resolve().parent
INPUT_CSV = BASE_DIR / "mirror_mismatch_all.csv"
OUT_DIR = BASE_DIR / "mirror_mismatch_graphs_all"
INPUT_CSV_ZERO_INCLUSIVE = BASE_DIR / "mirror_mismatch_all_zero_inclusive.csv"
OUT_DIR_ZERO_INCLUSIVE = BASE_DIR / "mirror_mismatch_graphs_all_zero_inclusive"


def ensure_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def savefig(out_dir: Path, name: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_dir / name, dpi=170)
    plt.close()


def top_pairs_by_abs_gap(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby(["exporterISO", "importerISO"], as_index=False)["abs_gap"]
        .sum()
        .sort_values("abs_gap", ascending=False)
        .head(20)
    )
    labels = g["exporterISO"] + "->" + g["importerISO"]
    plt.figure(figsize=(11, 5))
    plt.bar(labels, g["abs_gap"])
    plt.title("Top-20 Country Pairs by Total Absolute Gap")
    plt.xlabel("Pair")
    plt.ylabel("abs_gap (USD)")
    plt.xticks(rotation=50, ha="right", fontsize=8)
    savefig(out_dir, "01_top_pairs_abs_gap.png")


def top_pairs_by_mismatch_pct(df: pd.DataFrame, out_dir: Path):
    work = df.copy()
    work["pair_flow"] = np.maximum(work["exp_A_to_B"], work["imp_B_from_A"])
    work = work[work["pair_flow"] >= 1_000_000]
    g = (
        work.groupby(["exporterISO", "importerISO"], as_index=False)
        .agg(mismatch_pct=("mismatch_pct", "mean"))
        .sort_values("mismatch_pct", ascending=False)
        .head(20)
    )
    labels = g["exporterISO"] + "->" + g["importerISO"]
    plt.figure(figsize=(11, 5))
    plt.bar(labels, g["mismatch_pct"])
    plt.title("Top-20 Country Pairs by Avg Mismatch % (Flow >= $1M)")
    plt.xlabel("Pair")
    plt.ylabel("avg mismatch_pct")
    plt.xticks(rotation=50, ha="right", fontsize=8)
    savefig(out_dir, "02_top_pairs_mismatch_pct.png")


def heatmap_exporter_importer(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby(["exporterISO", "importerISO"], as_index=False)["abs_gap"]
        .sum()
    )
    top_exp = g.groupby("exporterISO")["abs_gap"].sum().sort_values(ascending=False).head(25).index
    top_imp = g.groupby("importerISO")["abs_gap"].sum().sort_values(ascending=False).head(25).index
    sub = g[g["exporterISO"].isin(top_exp) & g["importerISO"].isin(top_imp)]
    pivot = sub.pivot(index="exporterISO", columns="importerISO", values="abs_gap").fillna(0.0)

    plt.figure(figsize=(11, 9))
    mat = np.log10(pivot.values + 1.0)
    plt.imshow(mat, aspect="auto")
    plt.colorbar(label="log10(abs_gap + 1)")
    plt.title("Exporter vs Importer Heatmap (Top 25 x 25 by abs_gap)")
    plt.xlabel("Importer")
    plt.ylabel("Exporter")
    plt.xticks(np.arange(len(pivot.columns)), pivot.columns, rotation=90, fontsize=7)
    plt.yticks(np.arange(len(pivot.index)), pivot.index, fontsize=7)
    savefig(out_dir, "03_heatmap_exporter_importer_abs_gap.png")


def year_trend_abs_gap(df: pd.DataFrame, out_dir: Path):
    g = df.groupby("year", as_index=False)["abs_gap"].sum().sort_values("year")
    plt.figure(figsize=(8, 4))
    plt.plot(g["year"], g["abs_gap"], marker="o")
    plt.title("Year Trend: Total abs_gap")
    plt.xlabel("year")
    plt.ylabel("total abs_gap (USD)")
    plt.grid(alpha=0.3)
    savefig(out_dir, "04_year_trend_abs_gap.png")


def year_trend_suspicious_count(df: pd.DataFrame, out_dir: Path):
    g = df.groupby("year", as_index=False).size().sort_values("year")
    plt.figure(figsize=(8, 4))
    plt.plot(g["year"], g["size"], marker="o")
    plt.title("Year Trend: Suspicious Record Count")
    plt.xlabel("year")
    plt.ylabel("count")
    plt.grid(alpha=0.3)
    savefig(out_dir, "05_year_trend_suspicious_count.png")


def commodity_abs_gap(df: pd.DataFrame, out_dir: Path):
    g = df.groupby("commodity", as_index=False)["abs_gap"].sum().sort_values("abs_gap", ascending=False)
    plt.figure(figsize=(10, 4))
    plt.bar(g["commodity"], g["abs_gap"])
    plt.title("Commodity Comparison: Total abs_gap")
    plt.xlabel("commodity")
    plt.ylabel("total abs_gap (USD)")
    plt.xticks(rotation=30, ha="right", fontsize=8)
    savefig(out_dir, "06_commodity_total_abs_gap.png")


def commodity_suspicious_count(df: pd.DataFrame, out_dir: Path):
    g = df.groupby("commodity", as_index=False).size().sort_values("size", ascending=False)
    plt.figure(figsize=(10, 4))
    plt.bar(g["commodity"], g["size"])
    plt.title("Commodity Comparison: Suspicious Count")
    plt.xlabel("commodity")
    plt.ylabel("count")
    plt.xticks(rotation=30, ha="right", fontsize=8)
    savefig(out_dir, "07_commodity_suspicious_count.png")


def severity_by_year_stacked(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby(["year", "severity"], as_index=False)
        .size()
        .pivot(index="year", columns="severity", values="size")
        .fillna(0)
        .sort_index()
    )
    for col in ["high", "medium"]:
        if col not in g.columns:
            g[col] = 0
    g = g[["medium", "high"]]
    plt.figure(figsize=(8, 4))
    plt.bar(g.index, g["medium"], label="medium")
    plt.bar(g.index, g["high"], bottom=g["medium"], label="high")
    plt.title("Severity Split by Year")
    plt.xlabel("year")
    plt.ylabel("count")
    plt.legend()
    savefig(out_dir, "08_severity_by_year_stacked.png")


def ratio_distribution(df: pd.DataFrame, out_dir: Path, *, zero_inclusive: bool):
    ratios = pd.to_numeric(df["exp_to_imp_ratio"], errors="coerce")
    ratios = ratios.replace([np.inf, -np.inf], np.nan).dropna()
    if zero_inclusive:
        ratios = ratios[ratios >= 0]
    else:
        ratios = ratios[ratios > 0]
    if len(ratios) == 0:
        return
    focus_max = 10.0
    focused = ratios[ratios <= focus_max]
    clip_q = 0.95
    clip_value = float(ratios.quantile(clip_q))
    clipped = ratios[ratios <= clip_value]
    if len(focused) > 0:
        plt.figure(figsize=(9, 4))
        plt.hist(focused, bins=80)
        plt.title(f"Distribution of exp_to_imp_ratio (0 to {int(focus_max)})")
        plt.xlabel("exp_to_imp_ratio")
        plt.ylabel("frequency")
        plt.axvline(1.0, color="tab:red", linestyle="--", linewidth=1, label="ratio = 1")
        plt.legend()
        savefig(out_dir, "09_ratio_distribution_focus_0_10.png")
    plt.figure(figsize=(9, 4))
    plt.hist(clipped, bins=80)
    plt.title(f"Distribution of exp_to_imp_ratio (<= p{int(clip_q * 100)})")
    plt.xlabel("exp_to_imp_ratio")
    plt.ylabel("frequency")
    plt.axvline(1.0, color="tab:red", linestyle="--", linewidth=1, label="ratio = 1")
    plt.legend()
    savefig(out_dir, "09_ratio_distribution.png")
    positive_ratios = ratios[ratios > 0]
    if len(positive_ratios) > 0:
        plt.figure(figsize=(9, 4))
        plt.hist(np.log10(positive_ratios), bins=80)
        plt.title("Distribution of exp_to_imp_ratio (log10 scale)")
        plt.xlabel("log10(exp_to_imp_ratio)")
        plt.ylabel("frequency")
        savefig(out_dir, "09_ratio_distribution_log10.png")


def scatter_exp_vs_imp(df: pd.DataFrame, out_dir: Path):
    work = df.copy()
    work["exp_A_to_B"] = np.maximum(work["exp_A_to_B"], 1.0)
    work["imp_B_from_A"] = np.maximum(work["imp_B_from_A"], 1.0)
    # sample to keep plot readable
    if len(work) > 50000:
        work = work.sample(50000, random_state=42)
    colors = work["severity"].map({"high": "tab:red", "medium": "tab:orange"}).fillna("tab:blue")
    plt.figure(figsize=(7, 7))
    plt.scatter(work["exp_A_to_B"], work["imp_B_from_A"], c=colors, s=5, alpha=0.35)
    line_max = max(work["exp_A_to_B"].max(), work["imp_B_from_A"].max())
    plt.plot([1, line_max], [1, line_max], linestyle="--", linewidth=1)
    plt.xscale("log")
    plt.yscale("log")
    plt.title("exp_A_to_B vs imp_B_from_A (log-log)")
    plt.xlabel("exp_A_to_B")
    plt.ylabel("imp_B_from_A")
    savefig(out_dir, "10_scatter_exp_vs_imp_loglog.png")


def bubble_pair_pct_vs_flow(df: pd.DataFrame, out_dir: Path):
    work = df.copy()
    work["pair"] = work["exporterISO"] + "->" + work["importerISO"]
    work["flow"] = np.maximum(work["exp_A_to_B"], work["imp_B_from_A"])
    g = (
        work.groupby("pair", as_index=False)
        .agg(
            avg_mismatch_pct=("mismatch_pct", "mean"),
            total_flow=("flow", "sum"),
            total_abs_gap=("abs_gap", "sum"),
        )
        .sort_values("total_abs_gap", ascending=False)
        .head(300)
    )
    if g.empty:
        return
    sizes = np.sqrt(g["total_abs_gap"].clip(lower=1.0)) / 50.0
    plt.figure(figsize=(10, 6))
    plt.scatter(
        np.maximum(g["total_flow"], 1.0),
        g["avg_mismatch_pct"],
        s=sizes,
        alpha=0.45,
    )
    plt.xscale("log")
    plt.title("Pair-Level Bubble: Avg Mismatch % vs Total Flow")
    plt.xlabel("total flow (log scale)")
    plt.ylabel("avg mismatch_pct")
    savefig(out_dir, "11_bubble_pair_mismatch_vs_flow.png")


def abs_vs_pct_scatter(
    data: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    out_dir: Path,
    filename: str,
    sample_limit: int | None = None,
):
    work = data[[x_col, y_col]].copy()
    work[x_col] = pd.to_numeric(work[x_col], errors="coerce")
    work[y_col] = pd.to_numeric(work[y_col], errors="coerce")
    work = work.dropna(subset=[x_col, y_col])
    if work.empty:
        return
    work[x_col] = work[x_col].clip(lower=1.0)
    work[y_col] = work[y_col].clip(lower=0.0)
    if sample_limit and len(work) > sample_limit:
        work = work.sample(sample_limit, random_state=42)
    y_max = max(100.0, float(work[y_col].quantile(0.99)))
    plt.figure(figsize=(9, 5.5))
    plt.scatter(work[x_col], work[y_col], s=12, alpha=0.35)
    plt.xscale("log")
    plt.ylim(0, y_max * 1.05)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(alpha=0.25)
    savefig(out_dir, filename)


def abs_vs_pct_scatter_row_level(df: pd.DataFrame, out_dir: Path):
    abs_vs_pct_scatter(
        df,
        x_col="abs_gap",
        y_col="mismatch_pct",
        title="Absolute Gap vs Mismatch % (Row Level)",
        xlabel="abs_gap (USD, log scale)",
        ylabel="mismatch_pct",
        out_dir=out_dir,
        filename="13_abs_vs_pct_scatter_row_level.png",
        sample_limit=50000,
    )


def abs_vs_pct_scatter_pair_level(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby(["exporterISO", "importerISO"], as_index=False)
        .agg(
            total_abs_gap=("abs_gap", "sum"),
            avg_mismatch_pct=("mismatch_pct", "mean"),
        )
    )
    abs_vs_pct_scatter(
        g,
        x_col="total_abs_gap",
        y_col="avg_mismatch_pct",
        title="Absolute Gap vs Mismatch % (Pair Level)",
        xlabel="total abs_gap (USD, log scale)",
        ylabel="avg mismatch_pct",
        out_dir=out_dir,
        filename="14_abs_vs_pct_scatter_pair_level.png",
    )


def abs_vs_pct_scatter_commodity_level(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby("commodity", as_index=False)
        .agg(
            total_abs_gap=("abs_gap", "sum"),
            avg_mismatch_pct=("mismatch_pct", "mean"),
        )
    )
    abs_vs_pct_scatter(
        g,
        x_col="total_abs_gap",
        y_col="avg_mismatch_pct",
        title="Absolute Gap vs Mismatch % (Commodity Level)",
        xlabel="total abs_gap (USD, log scale)",
        ylabel="avg mismatch_pct",
        out_dir=out_dir,
        filename="15_abs_vs_pct_scatter_commodity_level.png",
        )


def reverse_missing_share(df: pd.DataFrame, out_dir: Path):
    g = (
        df.groupby(["commodity", "year"], as_index=False)
        .agg(
            total=("reverse_row_present", "size"),
            missing=("reverse_row_present", lambda s: (~s.astype(bool)).sum()),
        )
    )
    g["missing_share_pct"] = np.where(g["total"] > 0, g["missing"] / g["total"] * 100.0, 0.0)

    commodities = sorted(g["commodity"].unique())
    plt.figure(figsize=(10, 5))
    for c in commodities:
        sub = g[g["commodity"] == c].sort_values("year")
        plt.plot(sub["year"], sub["missing_share_pct"], marker="o", label=c)
    plt.title("Missing Mirror Share by Commodity/Year")
    plt.xlabel("year")
    plt.ylabel("missing reverse row share (%)")
    plt.legend(fontsize=7)
    plt.grid(alpha=0.3)
    savefig(out_dir, "12_missing_reverse_share_by_commodity_year.png")


def run_variant(input_csv: Path, out_dir: Path, *, zero_inclusive: bool):
    if not input_csv.exists():
        print(f"Input file not found: {input_csv}")
        return

    df = pd.read_csv(input_csv)
    if df.empty:
        print(f"Input CSV is empty: {input_csv}")
        return

    for col in ["abs_gap", "mismatch_pct", "exp_A_to_B", "imp_B_from_A", "year"]:
        if col in df.columns:
            df[col] = ensure_numeric(df, col)
    if "reverse_row_present" in df.columns:
        df["reverse_row_present"] = df["reverse_row_present"].astype(str).str.lower().isin(["true", "1", "yes"])
    else:
        df["reverse_row_present"] = True

    top_pairs_by_abs_gap(df, out_dir)
    top_pairs_by_mismatch_pct(df, out_dir)
    heatmap_exporter_importer(df, out_dir)
    year_trend_abs_gap(df, out_dir)
    year_trend_suspicious_count(df, out_dir)
    commodity_abs_gap(df, out_dir)
    commodity_suspicious_count(df, out_dir)
    severity_by_year_stacked(df, out_dir)
    ratio_distribution(df, out_dir, zero_inclusive=zero_inclusive)
    scatter_exp_vs_imp(df, out_dir)
    bubble_pair_pct_vs_flow(df, out_dir)
    reverse_missing_share(df, out_dir)
    abs_vs_pct_scatter_row_level(df, out_dir)
    abs_vs_pct_scatter_pair_level(df, out_dir)
    abs_vs_pct_scatter_commodity_level(df, out_dir)

    print(f"Written graphs to: {out_dir}")
    print("Files:")
    for p in sorted(out_dir.glob("*.png")):
        print(f"- {p.name}")


def main():
    run_variant(INPUT_CSV, OUT_DIR, zero_inclusive=False)
    run_variant(INPUT_CSV_ZERO_INCLUSIVE, OUT_DIR_ZERO_INCLUSIVE, zero_inclusive=True)


if __name__ == "__main__":
    main()
