# -*- coding: utf-8 -*-
"""
add_country_names.py

Обогащает сводные таблицы из data_pairs/ человекочитаемыми названиями стран.

1) Строит словарь code -> name по СЫРЫМ данным UN Comtrade
   из ../fetcher/data_raw/*.csv, используя колонки:
   - reporterISO, reporterDesc
   - partnerISO, partnerDesc

   Дополнительно:
   - Вшит полный словарь стран – членов ООН (UN member states),
     чтобы коды типа AFG, YEM, USA и др. всегда имели нормальное имя.

2) Добавляет в каждый файл из data_pairs/*.csv колонки:
   - exporterName
   - importerName

3) Сохраняет новые файлы в data_pairs_named/, не трогая оригиналы.

Если для какого-то кода нет названия, в качестве имени остаётся сам код.
"""

import pandas as pd
from pathlib import Path

# --- ПУТИ ---

# где лежат сводные файлы (как у тебя: aggregator/data_pairs)
DATA_PAIRS_DIR = Path("data_pairs")

# куда сохранять новые, обогащённые файлы
OUTPUT_DIR = Path("data_pairs_named")

# откуда брать исходные сырые файлы Comtrade (fetcher-скрипт)
RAW_DATA_DIR = Path("../fetcher/data_raw")


# --- спец-коды, для которых лучше задать название явно ---

SPECIAL_CODES = {
    "WLD": "World",
    "W00": "World (All partners)",
    "_X": "Areas not elsewhere specified (NES)",
    "EU27": "European Union (27 countries)",
    "EU28": "European Union (28 countries)",
    "EU25": "European Union (25 countries)",
    "EU15": "European Union (15 countries)",
    "EUU": "European Union",
    "XXA": "Special category A",
    "XXB": "Special category B",
    "XXC": "Special category C",
    "XXD": "Special category D",
    "XXE": "Special category E",
    "XXF": "Special category F",
}

# --- страны – члены ООН (UN Member States), ISO3 -> короткое имя ---

UN_MEMBER_NAMES = {
    "AFG": "Afghanistan",
    "ALB": "Albania",
    "DZA": "Algeria",
    "AND": "Andorra",
    "AGO": "Angola",
    "ATG": "Antigua and Barbuda",
    "ARG": "Argentina",
    "ARM": "Armenia",
    "AUS": "Australia",
    "AUT": "Austria",
    "AZE": "Azerbaijan",
    "BHS": "Bahamas",
    "BHR": "Bahrain",
    "BGD": "Bangladesh",
    "BRB": "Barbados",
    "BLR": "Belarus",
    "BEL": "Belgium",
    "BLZ": "Belize",
    "BEN": "Benin",
    "BTN": "Bhutan",
    "BOL": "Bolivia (Plurinational State of)",
    "BIH": "Bosnia and Herzegovina",
    "BWA": "Botswana",
    "BRA": "Brazil",
    "BRN": "Brunei Darussalam",
    "BGR": "Bulgaria",
    "BFA": "Burkina Faso",
    "BDI": "Burundi",
    "CPV": "Cabo Verde",
    "KHM": "Cambodia",
    "CMR": "Cameroon",
    "CAN": "Canada",
    "CAF": "Central African Republic",
    "TCD": "Chad",
    "CHL": "Chile",
    "CHN": "China",
    "COL": "Colombia",
    "COM": "Comoros",
    "COG": "Congo",
    "COD": "Congo, Democratic Republic of the",
    "CRI": "Costa Rica",
    "CIV": "Côte d'Ivoire",
    "HRV": "Croatia",
    "CUB": "Cuba",
    "CYP": "Cyprus",
    "CZE": "Czechia",
    "DNK": "Denmark",
    "DJI": "Djibouti",
    "DMA": "Dominica",
    "DOM": "Dominican Republic",
    "ECU": "Ecuador",
    "EGY": "Egypt",
    "SLV": "El Salvador",
    "GNQ": "Equatorial Guinea",
    "ERI": "Eritrea",
    "EST": "Estonia",
    "SWZ": "Eswatini",
    "ETH": "Ethiopia",
    "FJI": "Fiji",
    "FIN": "Finland",
    "FRA": "France",
    "GAB": "Gabon",
    "GMB": "Gambia",
    "GEO": "Georgia",
    "DEU": "Germany",
    "GHA": "Ghana",
    "GRC": "Greece",
    "GRD": "Grenada",
    "GTM": "Guatemala",
    "GIN": "Guinea",
    "GNB": "Guinea-Bissau",
    "GUY": "Guyana",
    "HTI": "Haiti",
    "HND": "Honduras",
    "HUN": "Hungary",
    "ISL": "Iceland",
    "IND": "India",
    "IDN": "Indonesia",
    "IRN": "Iran (Islamic Republic of)",
    "IRQ": "Iraq",
    "IRL": "Ireland",
    "ISR": "Israel",
    "ITA": "Italy",
    "JAM": "Jamaica",
    "JPN": "Japan",
    "JOR": "Jordan",
    "KAZ": "Kazakhstan",
    "KEN": "Kenya",
    "KIR": "Kiribati",
    "PRK": "Korea, Democratic People's Republic of",
    "KOR": "Korea, Republic of",
    "KWT": "Kuwait",
    "KGZ": "Kyrgyzstan",
    "LAO": "Lao People's Democratic Republic",
    "LVA": "Latvia",
    "LBN": "Lebanon",
    "LSO": "Lesotho",
    "LBR": "Liberia",
    "LBY": "Libya",
    "LIE": "Liechtenstein",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "MDG": "Madagascar",
    "MWI": "Malawi",
    "MYS": "Malaysia",
    "MDV": "Maldives",
    "MLI": "Mali",
    "MLT": "Malta",
    "MHL": "Marshall Islands",
    "MRT": "Mauritania",
    "MUS": "Mauritius",
    "MEX": "Mexico",
    "FSM": "Micronesia (Federated States of)",
    "MDA": "Republic of Moldova",
    "MCO": "Monaco",
    "MNG": "Mongolia",
    "MNE": "Montenegro",
    "MAR": "Morocco",
    "MOZ": "Mozambique",
    "MMR": "Myanmar",
    "NAM": "Namibia",
    "NRU": "Nauru",
    "NPL": "Nepal",
    "NLD": "Netherlands",
    "NZL": "New Zealand",
    "NIC": "Nicaragua",
    "NER": "Niger",
    "NGA": "Nigeria",
    "MKD": "North Macedonia",
    "NOR": "Norway",
    "OMN": "Oman",
    "PAK": "Pakistan",
    "PLW": "Palau",
    "PAN": "Panama",
    "PNG": "Papua New Guinea",
    "PRY": "Paraguay",
    "PER": "Peru",
    "PHL": "Philippines",
    "POL": "Poland",
    "PRT": "Portugal",
    "QAT": "Qatar",
    "ROU": "Romania",
    "RUS": "Russian Federation",
    "RWA": "Rwanda",
    "KNA": "Saint Kitts and Nevis",
    "LCA": "Saint Lucia",
    "VCT": "Saint Vincent and the Grenadines",
    "WSM": "Samoa",
    "SMR": "San Marino",
    "STP": "Sao Tome and Principe",
    "SAU": "Saudi Arabia",
    "SEN": "Senegal",
    "SRB": "Serbia",
    "SYC": "Seychelles",
    "SLE": "Sierra Leone",
    "SGP": "Singapore",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "SLB": "Solomon Islands",
    "SOM": "Somalia",
    "ZAF": "South Africa",
    "SSD": "South Sudan",
    "ESP": "Spain",
    "LKA": "Sri Lanka",
    "SDN": "Sudan",
    "SUR": "Suriname",
    "SWE": "Sweden",
    "CHE": "Switzerland",
    "SYR": "Syrian Arab Republic",
    "TJK": "Tajikistan",
    "THA": "Thailand",
    "TLS": "Timor-Leste",
    "TGO": "Togo",
    "TON": "Tonga",
    "TTO": "Trinidad and Tobago",
    "TUN": "Tunisia",
    "TUR": "Türkiye",
    "TKM": "Turkmenistan",
    "TUV": "Tuvalu",
    "UGA": "Uganda",
    "UKR": "Ukraine",
    "ARE": "United Arab Emirates",
    "GBR": "United Kingdom of Great Britain and Northern Ireland",
    "TZA": "United Republic of Tanzania",
    "USA": "United States of America",
    "URY": "Uruguay",
    "UZB": "Uzbekistan",
    "VUT": "Vanuatu",
    "VEN": "Venezuela (Bolivarian Republic of)",
    "VNM": "Viet Nam",
    "YEM": "Yemen",
    "ZMB": "Zambia",
    "ZWE": "Zimbabwe",
}


def build_country_dict() -> dict:
    """
    Пройти по ВСЕМ сырым CSV в RAW_DATA_DIR и собрать словарь:
        code (reporterISO/partnerISO) -> name (reporterDesc/partnerDesc)

    Приоритет:
    1) SPECIAL_CODES
    2) UN_MEMBER_NAMES (страны – члены ООН)
    3) reporterDesc / partnerDesc из данных
    4) если нет имени, остаётся сам код
    """
    country_dict: dict[str, str] = {}

    # 1) спец-коды (World, EU, _X, ...)
    for code, name in SPECIAL_CODES.items():
        country_dict[code] = name

    # 2) страны – члены ООН
    for code, name in UN_MEMBER_NAMES.items():
        country_dict.setdefault(code, name)

    if not RAW_DATA_DIR.exists():
        print(f"⚠️ Папка с сырыми данными не найдена: {RAW_DATA_DIR}")
        print("   Используем только SPECIAL_CODES и UN_MEMBER_NAMES.")
        return country_dict

    raw_files = sorted(RAW_DATA_DIR.glob("*.csv"))
    if not raw_files:
        print(f"⚠️ В {RAW_DATA_DIR} нет CSV-файлов, словарь будет только из SPECIAL_CODES и UN_MEMBER_NAMES.")
        return country_dict

    print(f"🔎 Строим словарь стран по сырым данным из: {RAW_DATA_DIR}")
    print(f"   Найдено файлов: {len(raw_files)}")

    # будем пробовать разные наборы, т.к. в некоторых файлах могут отсутствовать desc-колонки
    possible_cols = [
        ["reporterISO", "reporterDesc"],
        ["partnerISO", "partnerDesc"],
        ["reporterISO", "partnerISO", "reporterDesc", "partnerDesc"],
    ]

    for path in raw_files:
        print(f"   → {path.name}")
        df = None
        for cols in possible_cols:
            try:
                df = pd.read_csv(path, usecols=cols)
                break
            except Exception:
                df = None
                continue

        if df is None:
            print(f"     ⚠️ Не удалось прочитать нужные колонки из {path.name}, пропускаем.")
            continue

        # reporter
        if "reporterISO" in df.columns and "reporterDesc" in df.columns:
            sub = df[["reporterISO", "reporterDesc"]].dropna()
            for code, name in sub.itertuples(index=False):
                if not isinstance(code, str):
                    continue
                code_clean = code.strip()
                if not code_clean:
                    continue
                # reporterDesc для стран-членов ООН не должен ломать наши названия,
                # поэтому используем setdefault (не перезаписываем уже заданные).
                country_dict.setdefault(code_clean, str(name).strip())

        # partner
        if "partnerISO" in df.columns and "partnerDesc" in df.columns:
            sub = df[["partnerISO", "partnerDesc"]].dropna()
            for code, name in sub.itertuples(index=False):
                if not isinstance(code, str):
                    continue
                code_clean = code.strip()
                if not code_clean:
                    continue
                country_dict.setdefault(code_clean, str(name).strip())

    print(f"✅ Словарь стран построен, всего кодов: {len(country_dict)}")
    return country_dict


def enrich_pairs_files(country_dict: dict):
    """
    Пройти по всем *_country_pairs_*.csv в DATA_PAIRS_DIR,
    добавить exporterName / importerName и сохранить в OUTPUT_DIR.
    """
    if not DATA_PAIRS_DIR.exists():
        print(f"❌ Папка data_pairs не найдена: {DATA_PAIRS_DIR}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pair_files = sorted(DATA_PAIRS_DIR.glob("*_country_pairs_*.csv"))
    if not pair_files:
        print(f"⚠️ В {DATA_PAIRS_DIR} нет файлов *_country_pairs_*.csv")
        return

    print(f"📂 Найдено сводных файлов: {len(pair_files)}")
    print(f"   Обогащённые файлы будут сохранены в: {OUTPUT_DIR}")

    def decode(code):
        if isinstance(code, str):
            code_clean = code.strip()
        else:
            code_clean = str(code)
        return country_dict.get(code_clean, code_clean)

    for path in pair_files:
        print(f"\n→ Обрабатываем {path.name}")
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"   ⚠️ Ошибка чтения {path.name}: {e}")
            continue

        if "exporterISO" not in df.columns or "importerISO" not in df.columns:
            print(f"   ⚠️ В {path.name} нет колонок exporterISO/importerISO, пропускаем.")
            continue

        # добавляем названия
        df["exporterName"] = df["exporterISO"].apply(decode)
        df["importerName"] = df["importerISO"].apply(decode)

        # ставим их рядом с кодами
        cols = df.columns.tolist()
        new_order = (
            ["exporterISO", "exporterName", "importerISO", "importerName"]
            + [c for c in cols if c not in ("exporterISO", "exporterName",
                                            "importerISO", "importerName")]
        )
        df = df[new_order]

        out_path = OUTPUT_DIR / path.name
        df.to_csv(out_path, index=False)
        print(f"   ✅ Сохранено: {out_path}")


def main():
    country_dict = build_country_dict()
    enrich_pairs_files(country_dict)


if __name__ == "__main__":
    main()
