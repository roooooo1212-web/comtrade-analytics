# -*- coding: utf-8 -*-
"""
pairs_to_excel.py

Берёт сводные файлы из data_pairs_named/ и собирает их
в один Excel-файл с отдельной страницей под каждую отрасль.

Дополнительно:
  - создаёт скрытый лист UN_CODES с ISO3-кодами стран – членов ООН
  - условное форматирование:
      строки, где exporterISO или importerISO
      НЕ являются странами – членами ООН,
      подсвечиваются красным.

Результат:
    country_pairs_2020_2024.xlsx
"""

import pandas as pd
from pathlib import Path
import re

# Папка с обогащёнными сводками
DATA_DIR = Path("data_pairs_named")

# Имя выходного Excel-файла
OUTPUT_EXCEL = Path("country_pairs_2020_2024.xlsx")

# ----------------------------
#  UN MEMBER STATES — ISO3
# ----------------------------

UN_MEMBER_CODES = {
    "AFG","ALB","DZA","AND","AGO","ATG","ARG","ARM","AUS","AUT","AZE",
    "BHS","BHR","BGD","BRB","BLR","BEL","BLZ","BEN","BTN","BOL",
    "BIH","BWA","BRA","BRN","BGR","BFA","BDI","CPV","KHM","CMR",
    "CAN","CAF","TCD","CHL","CHN","COL","COM","COG","COD","CRI",
    "CIV","HRV","CUB","CYP","CZE","DNK","DJI","DMA","DOM","ECU",
    "EGY","SLV","GNQ","ERI","EST","SWZ","ETH","FJI","FIN","FRA",
    "GAB","GMB","GEO","DEU","GHA","GRC","GRD","GTM","GIN","GNB",
    "GUY","HTI","HND","HUN","ISL","IND","IDN","IRN","IRQ","IRL",
    "ISR","ITA","JAM","JPN","JOR","KAZ","KEN","KIR","PRK","KOR",
    "KWT","KGZ","LAO","LVA","LBN","LSO","LBR","LBY","LIE","LTU",
    "LUX","MDG","MWI","MYS","MDV","MLI","MLT","MHL","MRT","MUS",
    "MEX","FSM","MDA","MCO","MNG","MNE","MAR","MOZ","MMR","NAM",
    "NRU","NPL","NLD","NZL","NIC","NER","NGA","MKD","NOR","OMN",
    "PAK","PLW","PAN","PNG","PRY","PER","PHL","POL","PRT","QAT",
    "ROU","RUS","RWA","KNA","LCA","VCT","WSM","SMR","STP","SAU",
    "SEN","SRB","SYC","SLE","SGP","SVK","SVN","SLB","SOM","ZAF",
    "SSD","ESP","LKA","SDN","SUR","SWE","CHE","SYR","TJK","THA",
    "TLS","TGO","TON","TTO","TUN","TUR","TKM","TUV","UGA","UKR",
    "ARE","GBR","TZA","USA","URY","UZB","VUT","VEN","VNM","YEM",
    "ZMB","ZWE"
}


def guess_sheet_name(filename_stem: str) -> str:
    """
    Из имени файла вида:
        cereals_HS10_country_pairs_2020_2024
    сделать аккуратное имя листа:
        cereals_HS10

    Если формат другой, просто почистить и обрезать до 31 символа.
    """
    m = re.match(r"(.+?)_country_pairs_\d{4}_\d{4}$", filename_stem)
    if m:
        base = m.group(1)
    else:
        base = filename_stem

    # Удаляем запрещённые символы для Excel-листа
    name = re.sub(r'[\[\]\:\*\?\/\\]', "_", base)

    # Обрезаем до 31 символа (ограничение Excel)
    if len(name) > 31:
        name = name[:31]

    if not name:
        name = "Sheet"

    return name


def add_un_codes_sheet(writer) -> str:
    """
    Создаёт скрытый лист UN_CODES с кодами стран – членов ООН.
    Возвращает строку диапазона, например: 'UN_CODES!$A$1:$A$193'
    """
    workbook = writer.book
    sheet = workbook.add_worksheet("UN_CODES")

    # записываем UN-коды в первый столбец
    for i, code in enumerate(sorted(UN_MEMBER_CODES)):
        sheet.write(i, 0, code)

    # диапазон для MATCH
    end_row = len(UN_MEMBER_CODES)
    range_addr = f"UN_CODES!$A$1:$A${end_row}"

    # скрываем лист, чтобы не мешался пользователю
    sheet.hide()

    return range_addr


def main():
    if not DATA_DIR.exists():
        print(f"❌ Папка не найдена: {DATA_DIR}")
        return

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ В {DATA_DIR} нет CSV-файлов")
        return

    print(f"Найдено сводных CSV: {len(csv_files)}")
    print(f"Создаём Excel-файл: {OUTPUT_EXCEL}")

    used_sheet_names = set()

    def make_unique(name: str) -> str:
        """Гарантировать уникальность имени листа (Excel ограничивает 31 символ)."""
        base = name
        counter = 1
        while name in used_sheet_names:
            suffix = f"_{counter}"
            max_base_len = 31 - len(suffix)
            name = (base[:max_base_len] if len(base) > max_base_len else base) + suffix
            counter += 1
        used_sheet_names.add(name)
        return name

    with pd.ExcelWriter(OUTPUT_EXCEL, engine="xlsxwriter") as writer:
        workbook = writer.book

        # создаём скрытый лист с UN-кодами и получаем диапазон
        un_range = add_un_codes_sheet(writer)

        # формула: TRUE, если A2 или C2 НЕ найдены в un_range
        formula = (
            f"=OR("
            f"ISERROR(MATCH($A2,{un_range},0)),"
            f"ISERROR(MATCH($C2,{un_range},0))"
            f")"
        )

        # формат для "не-UN-страны" — красный фон
        red_fill = workbook.add_format({
            "bg_color": "#FFC7CE",  # светло-красный (как в Excel)
            "font_color": "#9C0006",
        })

        for csv_path in csv_files:
            print(f"→ Добавляем лист из файла: {csv_path.name}")
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                print(f"   ⚠️ Ошибка чтения {csv_path.name}: {e}")
                continue

            sheet_base = guess_sheet_name(csv_path.stem)
            sheet_name = make_unique(sheet_base)

            print(f"   Лист: {sheet_name}")
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # --- условное форматирование по "не-UN странам" ---
            worksheet = writer.sheets[sheet_name]

            n_rows, n_cols = df.shape
            if n_rows == 0:
                continue  # нечего форматировать

            # диапазон данных (без заголовка):
            # строки: 2..(n_rows+1) в Excel, в xlsxwriter это 1..n_rows
            first_row = 1
            last_row = n_rows      # включительно
            first_col = 0
            last_col = n_cols - 1  # столбцы 0..n_cols-1

            worksheet.conditional_format(
                first_row,
                first_col,
                last_row,
                last_col,
                {
                    "type": "formula",
                    "criteria": formula,
                    "format": red_fill,
                },
            )

    print(f"\n✅ Готово! Excel-файл сохранён как: {OUTPUT_EXCEL}")


if __name__ == "__main__":
    main()
