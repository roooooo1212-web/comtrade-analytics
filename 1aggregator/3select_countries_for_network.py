# -*- coding: utf-8 -*-
"""
select_countries_for_network.py

Из сводок в data_pairs_named/ определяет:

1) countries_included_for_network:
   страны – члены ООН (ISO3), участвующие хотя бы в одной связи
   "страна–страна", где обе стороны тоже являются членами ООН.
   Сохраняется в:
     - countries_included_for_network.csv
     - countries_included_for_network.xlsx

2) identifiers_excluded_from_network:
   все остальные коды (агрегаты, союзы, территории, _X, WLD и т.п.),
   а также UN-страны, которые НИ РАЗУ не встречаются в парах UN–UN.
   Сохраняется в:
     - identifiers_excluded_from_network.csv
     - identifiers_excluded_from_network.xlsx
"""

import pandas as pd
from pathlib import Path
import re

DATA_DIR = Path("data_pairs_named")

OUT_INCLUDED_CSV = Path("countries_included_for_network.csv")
OUT_INCLUDED_XLSX = Path("countries_included_for_network.xlsx")

OUT_EXCLUDED_CSV = Path("identifiers_excluded_from_network.csv")
OUT_EXCLUDED_XLSX = Path("identifiers_excluded_from_network.xlsx")

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


def is_iso_like(code: str) -> bool:
    """Помощник: просто проверяет, выглядит ли код как 3 заглавные буквы."""
    if not isinstance(code, str):
        return False
    code = code.strip()
    return bool(re.fullmatch(r"[A-Z]{3}", code))


def is_un_member(code: str) -> bool:
    """Является ли код страной – членом ООН."""
    if not isinstance(code, str):
        return False
    return code.strip() in UN_MEMBER_CODES


def main():
    if not DATA_DIR.exists():
        print(f"❌ Папка не найдена: {DATA_DIR}")
        return

    csv_files = sorted(DATA_DIR.glob("*_country_pairs_*.csv"))
    if not csv_files:
        print("⚠️ Нет сводок *_country_pairs_*.csv")
        return

    print(f"📂 Найдено файлов: {len(csv_files)}")

    # Словарь код → имя
    code_to_name: dict[str, str] = {}

    # Множество всех появившихся кодов (страны + агрегаты + территории)
    all_codes: set[str] = set()

    # UN-страны, участвующие в парах UN–UN
    countries_in_country_pairs: set[str] = set()

    # --- обрабатываем все таблицы ---
    for path in csv_files:
        print(f"→ Читаем {path.name}")

        df = pd.read_csv(path)

        needed = {"exporterISO", "exporterName", "importerISO", "importerName"}
        if not needed.issubset(df.columns):
            print("  ⚠️ Пропуск: нет нужных колонок")
            continue

        for r_code, r_name, p_code, p_name in df[
            ["exporterISO", "exporterName", "importerISO", "importerName"]
        ].itertuples(index=False):

            r_code = str(r_code).strip()
            p_code = str(p_code).strip()

            all_codes.add(r_code)
            all_codes.add(p_code)

            if r_code not in code_to_name:
                code_to_name[r_code] = str(r_name)
            if p_code not in code_to_name:
                code_to_name[p_code] = str(p_name)

            r_ok = is_un_member(r_code)
            p_ok = is_un_member(p_code)

            # Пара "UN-страна – UN-страна"
            if r_ok and p_ok:
                countries_in_country_pairs.add(r_code)
                countries_in_country_pairs.add(p_code)

    # --- формируем включённых (UN-страны, участвующие в UN–UN парах) ---
    included_rows = [{
        "code": code,
        "name": code_to_name.get(code, code),
        "role": "un_member_in_un_un_pairs"
    } for code in sorted(countries_in_country_pairs)]

    df_included = pd.DataFrame(included_rows)

    df_included.to_csv(OUT_INCLUDED_CSV, index=False)
    df_included.to_excel(OUT_INCLUDED_XLSX, index=False)

    print(f"✅ Страны включены (CSV):  {OUT_INCLUDED_CSV}")
    print(f"✅ Страны включены (XLSX): {OUT_INCLUDED_XLSX}")

    # --- формируем исключённых ---
    excluded_codes = all_codes - countries_in_country_pairs
    excluded_rows = []

    for code in sorted(excluded_codes):
        name = code_to_name.get(code, code)
        iso_like_flag = is_iso_like(code)
        if code in UN_MEMBER_CODES:
            reason = "un_member_without_any_un_un_pair"
        else:
            reason = "non_un_member"

        excluded_rows.append({
            "code": code,
            "name": name,
            "is_iso_like": iso_like_flag,
            "reason": reason
        })

    df_excluded = pd.DataFrame(excluded_rows)

    df_excluded.to_csv(OUT_EXCLUDED_CSV, index=False)
    df_excluded.to_excel(OUT_EXCLUDED_XLSX, index=False)

    print(f"✅ Идентификаторы исключены (CSV):  {OUT_EXCLUDED_CSV}")
    print(f"✅ Идентификаторы исключены (XLSX): {OUT_EXCLUDED_XLSX}")

    print("\nИТОГО:")
    print(f"  Всего кодов встречено:               {len(all_codes)}")
    print(f"  UN-стран, включённых как узлы сети:  {len(df_included)}")
    print(f"  Исключённых идентификаторов:        {len(df_excluded)}")


if __name__ == "__main__":
    main()
