# -*- coding: utf-8 -*-
"""
fetcher.py

Загрузка СЫРЫХ данных мировой торговли UN Comtrade за 2020–2024:
 - экспорт (X)
 - импорт (M)
 - без фильтрации
 - без очистки
 - без порогов
 - без ограничения строк (кроме серверных лимитов API)

Сохраняет ТОЛЬКО CSV (Parquet отключён).
"""

import comtradeapicall
import pandas as pd
import os
from pathlib import Path

# ============= НАСТРОЙКИ =============

ROOT_DIR = Path(__file__).resolve().parent.parent


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv_file(ROOT_DIR / ".env")
load_dotenv_file(Path(__file__).resolve().parent / ".env")

SUBSCRIPTION_KEY = os.environ.get("UN_COMTRADE_SUBSCRIPTION_KEY", "").strip()

YEARS = [2020, 2021, 2022, 2023, 2024]

COMMODITIES = {
    "energy_fuels_HS27": "27",
    "machinery_electronics_HS84_85": "84,85",
    "cereals_HS10": "10",
    "ores_HS26": "26"
}

# ДВА торговых потока
FLOWS = ["X", "M"]

# Папка для сохранения CSV
OUT_DIR = Path("data_raw")


# ============= RAW FETCH =============

def fetch_raw(hs_codes: str, year: int, flow_code: str) -> pd.DataFrame:
    """
    Скачивает СЫРЫЕ данные без фильтрации.
    """
    print(f"\n=== RAW fetch: HS={hs_codes}, year={year}, flow={flow_code} ===")

    df = comtradeapicall.getFinalData(
        SUBSCRIPTION_KEY,
        typeCode='C',
        freqCode='A',
        clCode='HS',
        period=str(year),
        reporterCode=None,
        cmdCode=hs_codes,
        flowCode=flow_code,
        partnerCode=None,
        partner2Code=None,
        customsCode=None,
        motCode=None,
        maxRecords=None,       # no manual limits
        format_output='JSON',
        aggregateBy=None,
        breakdownMode='classic',
        countOnly=None,
        includeDesc=True
    )

    print(f"Rows received: {len(df)}")
    return df


# ============= MAIN =============

def main():
    if not SUBSCRIPTION_KEY:
        raise RuntimeError(
            "Environment variable UN_COMTRADE_SUBSCRIPTION_KEY is not set."
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for year in YEARS:
        for flow in FLOWS:
            for name, hs in COMMODITIES.items():

                df = fetch_raw(hs_codes=hs, year=year, flow_code=flow)

                if df.empty:
                    print(f"⚠️ No data for {name} {year} {flow}")
                    continue

                base = f"{name}_{year}_{flow}"
                csv_path = OUT_DIR / f"{base}.csv"

                df.to_csv(csv_path, index=False)

                print(f"✅ Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
