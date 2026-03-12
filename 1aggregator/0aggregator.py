# -*- coding: utf-8 -*-
"""
build_country_pairs_pivot.py

Строит сводные таблицы "пара стран -> объёмы экспорта/импорта по годам"
на основе уже скачанных CSV из data_raw/.

Для КАЖДОЙ отрасли (commodity) создаётся ОТДЕЛЬНЫЙ файл, где:

строки:   exporterISO, importerISO (нормализовано)
столбцы:  exp_2020, imp_2020, exp_2021, imp_2021, ... и т.д.

Источник данных: файлы вида
  {commodity_name}_{year}_{flow}.csv
например:
  machinery_electronics_HS84_85_2023_X.csv
"""

import pandas as pd
from pathlib import Path
import re

# --- пути ---
DATA_DIR = Path("data_raw")
OUT_DIR = Path("data_pairs")

# --- годы (на всякий случай, можно использовать и auto-detect по файлам) ---
YEARS = [2020, 2021, 2022, 2023, 2024]

# --- сопоставление символов потока ---
FLOW_LABEL = {
    "X": "exp",  # экспорт
    "M": "imp",  # импорт
}


def parse_filename(path: Path):
    """
    Разобрать имя файла вида:
        machinery_electronics_HS84_85_2023_X.csv
    в три части:
        commodity_name = 'machinery_electronics_HS84_85'
        year = 2023
        flow = 'X'
    Если формат другой — вернуть None.
    """
    stem = path.stem  # без .csv
    parts = stem.split("_")
    if len(parts) < 3:
        return None

    # последние два токена: year, flow (например, 2023, X)
    year_str = parts[-2]
    flow = parts[-1]

    # всё, что до этого — commodity_name
    commodity_name = "_".join(parts[:-2])

    # год должен быть числом
    if not year_str.isdigit():
        return None

    year = int(year_str)
    if flow not in ("X", "M"):
        return None

    return commodity_name, year, flow


def build_pivots_for_all_commodities():
    if not DATA_DIR.exists():
        print(f"❌ Папка с исходными CSV не найдена: {DATA_DIR}")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ В {DATA_DIR} нет CSV-файлов")
        return

    # сгруппируем файлы по commodity_name
    files_by_commodity = {}
    for path in csv_files:
        parsed = parse_filename(path)
        if parsed is None:
            print(f"⚠️ Пропускаем (непонятное имя файла): {path.name}")
            continue

        commodity_name, year, flow = parsed
        files_by_commodity.setdefault(commodity_name, []).append(
            (path, year, flow)
        )

    print(f"Найдено отраслей (commodity): {len(files_by_commodity)}")

    # обрабатываем каждую отрасль отдельно
    for commodity_name, file_infos in files_by_commodity.items():
        print(f"\n=== Обработка отрасли: {commodity_name} ===")
        summary_df = None

        for path, year, flow in sorted(file_infos, key=lambda x: (x[1], x[2])):
            if year not in YEARS:
                # если вдруг есть лишние годы — можно их отфильтровать
                print(f"  ⚠️ Пропускаем {path.name} (год {year} не в YEARS)")
                continue

            print(f"  → Читаем {path.name} (year={year}, flow={flow})")

            try:
                df = pd.read_csv(path)
            except Exception as e:
                print(f"    ⚠️ Ошибка чтения {path.name}: {e}")
                continue

            # Проверим необходимые колонки
            needed_cols = {"reporterISO", "partnerISO", "primaryValue"}
            if not needed_cols.issubset(df.columns):
                print(f"    ⚠️ В {path.name} нет нужных колонок {needed_cols}, пропускаем")
                continue

            # Нормализация направления:
            # хотим всегда exporterISO -> importerISO
            if flow == "M":
                # Для импорта reporter = importer, partner = exporter,
                # поэтому меняем местами, чтобы получить exporter -> importer.
                df = df.rename(
                    columns={
                        "reporterISO": "importerISO",
                        "partnerISO": "exporterISO",
                    }
                )
            else:
                df = df.rename(
                    columns={
                        "reporterISO": "exporterISO",
                        "partnerISO": "importerISO",
                    }
                )

            # агрегируем по нормализованным парам стран
            grouped = (
                df.groupby(["exporterISO", "importerISO"], as_index=False)["primaryValue"]
                .sum()
            )

            # имя столбца по году и типу потока
            flow_label = FLOW_LABEL.get(flow, flow.lower())
            col_name = f"{flow_label}_{year}"

            grouped = grouped.rename(columns={"primaryValue": col_name})

            # объединяем с уже накопленной сводкой
            if summary_df is None:
                summary_df = grouped
            else:
                summary_df = summary_df.merge(
                    grouped,
                    on=["exporterISO", "importerISO"],
                    how="outer",
                )

        if summary_df is None:
            print(f"⚠️ Для {commodity_name} не удалось построить сводку (нет корректных файлов)")
            continue

        # Заполним пропуски нулями (где нет данных по какому-то году/потоку)
        summary_df = summary_df.fillna(0)

        # немного упорядочим колонки:
        # сначала reporterISO, partnerISO, потом по годам в порядке
        other_cols = [c for c in summary_df.columns if c not in ("exporterISO", "importerISO")]
        # сортируем столбцы по году и типу (exp раньше imp, и по возрастанию года)
        def sort_key(col):
            m = re.match(r"(exp|imp)_(\d{4})", col)
            if m:
                kind, year_str = m.groups()
                kind_order = 0 if kind == "exp" else 1
                return (int(year_str), kind_order)
            return (9999, 9)

        other_cols_sorted = sorted(other_cols, key=sort_key)
        summary_df = summary_df[["exporterISO", "importerISO"] + other_cols_sorted]

        # сохраняем
        out_path = OUT_DIR / f"{commodity_name}_country_pairs_2020_2024.csv"
        summary_df.to_csv(out_path, index=False)
        print(f"✅ Сводная таблица сохранена: {out_path}")


def main():
    build_pivots_for_all_commodities()


if __name__ == "__main__":
    main()
