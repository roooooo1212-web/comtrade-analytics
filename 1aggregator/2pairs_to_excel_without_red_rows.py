# -*- coding: utf-8 -*-
"""
2pairs_to_excel_without_red_rows.py

Builds a second workbook from data_pairs_named/ and removes any rows that would
have been highlighted red in 2pairs_to_excel_highlighted.py.

Rule:
  keep a row only if exporterISO and importerISO are both UN member ISO3 codes.

Result:
    country_pairs_2020_2024_without_red_rows.xlsx
"""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_pairs_named"
OUTPUT_EXCEL = BASE_DIR / "country_pairs_2020_2024_without_red_rows.xlsx"

UN_MEMBER_CODES = {
    "AFG", "ALB", "DZA", "AND", "AGO", "ATG", "ARG", "ARM", "AUS", "AUT", "AZE",
    "BHS", "BHR", "BGD", "BRB", "BLR", "BEL", "BLZ", "BEN", "BTN", "BOL",
    "BIH", "BWA", "BRA", "BRN", "BGR", "BFA", "BDI", "CPV", "KHM", "CMR",
    "CAN", "CAF", "TCD", "CHL", "CHN", "COL", "COM", "COG", "COD", "CRI",
    "CIV", "HRV", "CUB", "CYP", "CZE", "DNK", "DJI", "DMA", "DOM", "ECU",
    "EGY", "SLV", "GNQ", "ERI", "EST", "SWZ", "ETH", "FJI", "FIN", "FRA",
    "GAB", "GMB", "GEO", "DEU", "GHA", "GRC", "GRD", "GTM", "GIN", "GNB",
    "GUY", "HTI", "HND", "HUN", "ISL", "IND", "IDN", "IRN", "IRQ", "IRL",
    "ISR", "ITA", "JAM", "JPN", "JOR", "KAZ", "KEN", "KIR", "PRK", "KOR",
    "KWT", "KGZ", "LAO", "LVA", "LBN", "LSO", "LBR", "LBY", "LIE", "LTU",
    "LUX", "MDG", "MWI", "MYS", "MDV", "MLI", "MLT", "MHL", "MRT", "MUS",
    "MEX", "FSM", "MDA", "MCO", "MNG", "MNE", "MAR", "MOZ", "MMR", "NAM",
    "NRU", "NPL", "NLD", "NZL", "NIC", "NER", "NGA", "MKD", "NOR", "OMN",
    "PAK", "PLW", "PAN", "PNG", "PRY", "PER", "PHL", "POL", "PRT", "QAT",
    "ROU", "RUS", "RWA", "KNA", "LCA", "VCT", "WSM", "SMR", "STP", "SAU",
    "SEN", "SRB", "SYC", "SLE", "SGP", "SVK", "SVN", "SLB", "SOM", "ZAF",
    "SSD", "ESP", "LKA", "SDN", "SUR", "SWE", "CHE", "SYR", "TJK", "THA",
    "TLS", "TGO", "TON", "TTO", "TUN", "TUR", "TKM", "TUV", "UGA", "UKR",
    "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VUT", "VEN", "VNM", "YEM",
    "ZMB", "ZWE",
}


def guess_sheet_name(filename_stem: str) -> str:
    m = re.match(r"(.+?)_country_pairs_\d{4}_\d{4}$", filename_stem)
    if m:
        base = m.group(1)
    else:
        base = filename_stem

    name = re.sub(r"[\[\]\:\*\?\/\\]", "_", base)
    if len(name) > 31:
        name = name[:31]
    return name or "Sheet"


def filter_un_member_rows(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"exporterISO", "importerISO"}
    missing = required_columns.difference(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise KeyError(f"Missing required columns: {missing_list}")

    exporter = df["exporterISO"].astype(str).str.strip()
    importer = df["importerISO"].astype(str).str.strip()
    keep_mask = exporter.isin(UN_MEMBER_CODES) & importer.isin(UN_MEMBER_CODES)
    return df.loc[keep_mask].copy()


def main() -> None:
    if not DATA_DIR.exists():
        print(f"ERROR: folder not found: {DATA_DIR}")
        return

    csv_files = sorted(DATA_DIR.glob("*_country_pairs_*.csv"))
    if not csv_files:
        print(f"WARNING: no CSV files found in {DATA_DIR}")
        return

    print(f"Found CSV files: {len(csv_files)}")
    print(f"Writing filtered workbook: {OUTPUT_EXCEL}")

    used_sheet_names: set[str] = set()
    total_input_rows = 0
    total_kept_rows = 0

    def make_unique(name: str) -> str:
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
        for csv_path in csv_files:
            df = pd.read_csv(csv_path)
            filtered_df = filter_un_member_rows(df)

            total_input_rows += len(df)
            total_kept_rows += len(filtered_df)

            sheet_name = make_unique(guess_sheet_name(csv_path.stem))
            filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(
                f"{csv_path.name}: kept {len(filtered_df)} of {len(df)} rows "
                f"on sheet {sheet_name}"
            )

    print()
    print(f"Done: {OUTPUT_EXCEL}")
    print(f"Total kept rows: {total_kept_rows} of {total_input_rows}")


if __name__ == "__main__":
    main()
