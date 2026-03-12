# -*- coding: utf-8 -*-
"""
4_mismatch_histogram.py

Строит гистограммы "классов несоответствий" на основе
identifiers_excluded_from_network.csv.

Выход:
- mismatch_histogram.png
- mismatch_histogram.xlsx (таблица + диаграмма)
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


DATA_FILE = Path("identifiers_excluded_from_network.csv")
OUT_PNG = Path("mismatch_histogram.png")
OUT_XLSX = Path("mismatch_histogram.xlsx")

# Настройки визуализации
TOP_N_REASONS = 10  # если причин много, остальные пойдут в "Other"


def main():
    if not DATA_FILE.exists():
        print(f"❌ Файл не найден: {DATA_FILE}")
        return

    df = pd.read_csv(DATA_FILE)
    needed = {"reason", "is_iso_like"}
    if not needed.issubset(df.columns):
        print(f"❌ В {DATA_FILE} нет колонок {needed}")
        return

    # --- сводки ---
    reason_counts = (
        df["reason"]
        .value_counts()
        .rename_axis("reason")
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    reason_iso = (
        df.groupby(["reason", "is_iso_like"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    # Пивот для удобного отображения
    reason_iso_pivot = reason_iso.pivot(
        index="reason", columns="is_iso_like", values="count"
    ).fillna(0).astype(int)
    reason_iso_pivot = reason_iso_pivot.sort_values(
        by=reason_iso_pivot.columns.tolist(), ascending=False
    )

    # Сжимаем причины в Top-N + Other, если нужно
    if len(reason_counts) > TOP_N_REASONS:
        top_reasons = reason_counts.head(TOP_N_REASONS)["reason"].tolist()

        other_count = reason_counts.loc[
            ~reason_counts["reason"].isin(top_reasons), "count"
        ].sum()
        reason_counts = reason_counts[reason_counts["reason"].isin(top_reasons)].copy()
        reason_counts = pd.concat(
            [reason_counts, pd.DataFrame([{"reason": "Other", "count": other_count}])],
            ignore_index=True,
        )

        other_rows = reason_iso_pivot.loc[~reason_iso_pivot.index.isin(top_reasons)]
        other_sum = other_rows.sum().to_frame().T
        other_sum.index = ["Other"]

        reason_iso_pivot = reason_iso_pivot.loc[reason_iso_pivot.index.isin(top_reasons)]
        reason_iso_pivot = pd.concat([reason_iso_pivot, other_sum], axis=0)

    # Доли (проценты)
    total = reason_counts["count"].sum()
    reason_counts["share_pct"] = (reason_counts["count"] / total * 100).round(2)

    # Доли внутри причины для is_iso_like
    reason_iso_share = reason_iso_pivot.div(
        reason_iso_pivot.sum(axis=1).replace(0, 1),
        axis=0,
    ) * 100

    # --- PNG ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].bar(reason_counts["reason"], reason_counts["share_pct"])
    axes[0].set_title("Несоответствия по причинам (доля, %)")
    axes[0].set_xlabel("reason")
    axes[0].set_ylabel("share, %")
    axes[0].tick_params(axis="x", rotation=30, labelsize=9)

    reason_iso_share.plot(
        kind="bar",
        stacked=True,
        ax=axes[1],
        title="Несоответствия: reason + is_iso_like (доля, %)",
    )
    axes[1].set_xlabel("reason")
    axes[1].set_ylabel("share, %")
    axes[1].tick_params(axis="x", rotation=30, labelsize=9)
    axes[1].legend(title="is_iso_like")

    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=160)
    plt.close(fig)

    # --- XLSX ---
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
        reason_counts.to_excel(writer, sheet_name="by_reason", index=False)
        reason_iso.to_excel(writer, sheet_name="by_reason_iso", index=False)
        reason_iso_pivot.reset_index().to_excel(
            writer, sheet_name="pivot", index=False
        )
        reason_iso_share.reset_index().to_excel(
            writer, sheet_name="pivot_share", index=False
        )

        workbook = writer.book
        worksheet = writer.sheets["by_reason"]

        # Диаграмма по причинам (проценты)
        chart = workbook.add_chart({"type": "column"})
        chart.add_series({
            "name": "share_pct",
            "categories": ["by_reason", 1, 0, len(reason_counts), 0],
            "values": ["by_reason", 1, 2, len(reason_counts), 2],
        })
        chart.set_title({"name": "Несоответствия по причинам (доля, %)"})
        chart.set_x_axis({"name": "reason"})
        chart.set_y_axis({"name": "share, %"})

        worksheet.insert_chart("D2", chart)

    print(f"✅ PNG:  {OUT_PNG}")
    print(f"✅ XLSX: {OUT_XLSX}")


if __name__ == "__main__":
    main()
