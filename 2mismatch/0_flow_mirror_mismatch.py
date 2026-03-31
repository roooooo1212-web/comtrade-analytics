# -*- coding: utf-8 -*-
"""
0_flow_mirror_mismatch.py

Checks mirrored trade consistency:
  A -> B export (exp_YYYY from row A,B)
vs
  B <- A import (imp_YYYY from row B,A)

Reads all *_country_pairs_*.csv from data_pairs_named/ (fallback: data_pairs/),
builds mismatch rows, flags suspicious records, and writes:
  - mirror_mismatch_all.csv
  - mirror_mismatch_suspicious.csv
  - mirror_mismatch_suspicious.xlsx (highlighted)
  - zero-inclusive ratio variants with `_zero_inclusive` suffix
"""

from __future__ import annotations

from pathlib import Path
import argparse
import re

import pandas as pd
import xlsxwriter


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR_NAMED = BASE_DIR / "data_pairs_named"
DATA_DIR_RAW = BASE_DIR / "data_pairs"
EXCLUDED_CODES_XLSX = BASE_DIR / "identifiers_excluded_from_network.xlsx"
EXCLUDED_CODES_CSV = BASE_DIR / "identifiers_excluded_from_network.csv"
EXCLUDED_CODES_CSV_FALLBACK = BASE_DIR.parent / "1aggregator" / "identifiers_excluded_from_network.csv"

OUT_ALL = BASE_DIR / "mirror_mismatch_all.csv"
OUT_SUSPICIOUS = BASE_DIR / "mirror_mismatch_suspicious.csv"
OUT_XLSX = BASE_DIR / "mirror_mismatch_suspicious.xlsx"
OUT_BY_SOURCE_DIR = BASE_DIR / "mirror_mismatch_by_source"
OUT_ALL_ZERO_INCLUSIVE = BASE_DIR / "mirror_mismatch_all_zero_inclusive.csv"
OUT_SUSPICIOUS_ZERO_INCLUSIVE = BASE_DIR / "mirror_mismatch_suspicious_zero_inclusive.csv"
OUT_XLSX_ZERO_INCLUSIVE = BASE_DIR / "mirror_mismatch_suspicious_zero_inclusive.xlsx"
OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR = BASE_DIR / "mirror_mismatch_by_source_zero_inclusive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare A->B exports with mirrored B->A imports and flag suspicious mismatches."
    )
    parser.add_argument(
        "--min-flow",
        type=float,
        default=50_000.0,
        help="Minimum of max(export, mirrored_import) to consider row for suspicious checks.",
    )
    parser.add_argument(
        "--abs-gap-threshold",
        type=float,
        default=1_000_000.0,
        help="Absolute gap threshold (USD) for suspicious flag.",
    )
    parser.add_argument(
        "--pct-gap-threshold",
        type=float,
        default=50.0,
        help="Percent gap threshold for suspicious flag.",
    )
    parser.add_argument(
        "--zero-pair-threshold",
        type=float,
        default=200_000.0,
        help="If one side is zero and other >= this value, mark suspicious.",
    )
    parser.add_argument(
        "--keep-excluded",
        action="store_true",
        help="Keep special/non-country codes (W00, _X, etc.) if present.",
    )
    parser.add_argument(
        "--keep-self-trade",
        action="store_true",
        help="Keep rows where exporterISO == importerISO.",
    )
    parser.add_argument(
        "--no-xlsx",
        action="store_true",
        help="Skip Excel output and write CSV files only.",
    )
    parser.add_argument(
        "--xlsx-max-rows",
        type=int,
        default=0,
        help="Max rows to write into highlighted Excel (0 = no limit).",
    )
    return parser.parse_args()


def detect_data_dir() -> Path:
    if DATA_DIR_NAMED.exists() and any(DATA_DIR_NAMED.glob("*_country_pairs_*.csv")):
        return DATA_DIR_NAMED
    if DATA_DIR_RAW.exists() and any(DATA_DIR_RAW.glob("*_country_pairs_*.csv")):
        return DATA_DIR_RAW
    raise FileNotFoundError("No *_country_pairs_*.csv files found in data_pairs_named/ or data_pairs/")


def parse_years(columns: list[str]) -> list[int]:
    years = set()
    for col in columns:
        m = re.match(r"(exp|imp)_(\d{4})$", col)
        if m:
            years.add(int(m.group(2)))
    return sorted(years)


def load_excluded_codes() -> set[str]:
    if EXCLUDED_CODES_CSV.exists():
        df = pd.read_csv(EXCLUDED_CODES_CSV)
    elif EXCLUDED_CODES_CSV_FALLBACK.exists():
        df = pd.read_csv(EXCLUDED_CODES_CSV_FALLBACK)
    elif EXCLUDED_CODES_XLSX.exists():
        try:
            df = pd.read_excel(EXCLUDED_CODES_XLSX)
        except Exception:
            return set()
    else:
        return set()

    if "code" not in df.columns:
        return set()

    return {
        str(x).strip()
        for x in df["code"].dropna().tolist()
        if str(x).strip()
    }


def get_name_maps(df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    exporter_name_col = "exporterName" if "exporterName" in df.columns else None
    importer_name_col = "importerName" if "importerName" in df.columns else None

    exp_map = {}
    imp_map = {}
    if exporter_name_col:
        exp_map = (
            df[["exporterISO", exporter_name_col]]
            .dropna()
            .drop_duplicates(subset=["exporterISO"])
            .set_index("exporterISO")[exporter_name_col]
            .astype(str)
            .to_dict()
        )
    if importer_name_col:
        imp_map = (
            df[["importerISO", importer_name_col]]
            .dropna()
            .drop_duplicates(subset=["importerISO"])
            .set_index("importerISO")[importer_name_col]
            .astype(str)
            .to_dict()
        )
    return exp_map, imp_map


def get_country_name(iso: str, exp_map: dict[str, str], imp_map: dict[str, str]) -> str:
    return exp_map.get(iso) or imp_map.get(iso) or iso


def compute_ratio_metrics(export_value: float, mirrored_import_value: float) -> tuple[float | None, float | None, str]:
    if mirrored_import_value > 0:
        ratio = export_value / mirrored_import_value
        return ratio, ratio, "finite"
    if export_value == 0:
        return None, 0.0, "zero_over_zero_or_missing"
    return None, float("inf"), "positive_over_zero_or_missing"


def compute_abs_vs_relative_point(export_value: float, mirrored_import_value: float, abs_gap: float) -> tuple[float, float | None]:
    x_value = abs_gap
    max_side = max(export_value, mirrored_import_value)
    min_side = min(export_value, mirrored_import_value)
    if min_side > 0:
        return x_value, max_side / min_side
    if max_side > 0:
        return x_value, 1.0
    return x_value, None


def classify_row(
    export_value: float,
    mirrored_import_value: float,
    abs_gap: float,
    pct_gap: float,
    *,
    min_flow: float,
    abs_gap_threshold: float,
    pct_gap_threshold: float,
    zero_pair_threshold: float,
) -> tuple[bool, str, str]:
    max_flow = max(export_value, mirrored_import_value)
    min_flow_side = min(export_value, mirrored_import_value)

    if max_flow < min_flow:
        return False, "below_min_flow", "none"

    reasons = []
    if abs_gap >= abs_gap_threshold:
        reasons.append("high_abs_gap")
    if pct_gap >= pct_gap_threshold:
        reasons.append("high_pct_gap")
    if min_flow_side == 0 and max_flow >= zero_pair_threshold:
        reasons.append("one_side_zero")

    if not reasons:
        return False, "ok", "none"

    if (
        abs_gap >= abs_gap_threshold * 2
        or pct_gap >= max(85.0, pct_gap_threshold + 25.0)
        or (min_flow_side == 0 and max_flow >= zero_pair_threshold * 5)
    ):
        severity = "high"
    else:
        severity = "medium"

    return True, "|".join(reasons), severity


def compare_file(path: Path, args: argparse.Namespace, excluded_codes: set[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["exporterISO"] = df["exporterISO"].astype(str).str.strip()
    df["importerISO"] = df["importerISO"].astype(str).str.strip()

    if excluded_codes:
        df = df[
            ~df["exporterISO"].isin(excluded_codes)
            & ~df["importerISO"].isin(excluded_codes)
        ].copy()
    if not args.keep_self_trade:
        df = df[df["exporterISO"] != df["importerISO"]].copy()

    years = parse_years(list(df.columns))
    if not years:
        return pd.DataFrame()

    exp_name_map, imp_name_map = get_name_maps(df)

    # Reverse lookup for mirrored import:
    # key (A,B) in this dict means flow A->B from import file perspective on row (B,A)
    reverse_imp_lookup = (
        df.set_index(["importerISO", "exporterISO"])[[f"imp_{y}" for y in years]]
        .to_dict(orient="index")
    )

    commodity = path.name.split("_country_pairs_")[0]
    out_rows = []

    for row in df.itertuples(index=False):
        exporter = str(getattr(row, "exporterISO"))
        importer = str(getattr(row, "importerISO"))

        exporter_name = get_country_name(exporter, exp_name_map, imp_name_map)
        importer_name = get_country_name(importer, exp_name_map, imp_name_map)

        mirror_key = (exporter, importer)
        mirror_values = reverse_imp_lookup.get(mirror_key)
        reverse_present = mirror_values is not None

        for year in years:
            exp_col = f"exp_{year}"
            imp_col = f"imp_{year}"

            exp_value = float(getattr(row, exp_col, 0.0) or 0.0)
            mirrored_imp_value = float((mirror_values or {}).get(imp_col, 0.0) or 0.0)

            abs_gap = abs(exp_value - mirrored_imp_value)
            denom = max(exp_value, mirrored_imp_value, 1.0)
            pct_gap = abs_gap / denom * 100.0
            ratio, ratio_zero_inclusive, ratio_case = compute_ratio_metrics(
                exp_value,
                mirrored_imp_value,
            )
            abs_vs_relative_x, abs_vs_relative_y = compute_abs_vs_relative_point(
                exp_value,
                mirrored_imp_value,
                abs_gap,
            )

            is_suspicious, reason, severity = classify_row(
                exp_value,
                mirrored_imp_value,
                abs_gap,
                pct_gap,
                min_flow=args.min_flow,
                abs_gap_threshold=args.abs_gap_threshold,
                pct_gap_threshold=args.pct_gap_threshold,
                zero_pair_threshold=args.zero_pair_threshold,
            )

            out_rows.append(
                {
                    "commodity": commodity,
                    "year": year,
                    "exporterISO": exporter,
                    "exporterName": exporter_name,
                    "importerISO": importer,
                    "importerName": importer_name,
                    "exp_A_to_B": exp_value,
                    "imp_B_from_A": mirrored_imp_value,
                    "abs_gap": abs_gap,
                    "abs_vs_relative_x": abs_vs_relative_x,
                    "abs_vs_relative_y": abs_vs_relative_y,
                    "mismatch_pct": pct_gap,
                    "exp_to_imp_ratio": ratio,
                    "exp_to_imp_ratio_zero_inclusive": ratio_zero_inclusive,
                    "exp_to_imp_ratio_case": ratio_case,
                    "reverse_row_present": reverse_present,
                    "is_suspicious": is_suspicious,
                    "reason": reason,
                    "severity": severity,
                    "source_file": path.name,
                }
            )

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    return result.sort_values(
        by=["is_suspicious", "abs_gap", "mismatch_pct"],
        ascending=[False, False, False],
    )


def write_highlighted_excel(df_suspicious: pd.DataFrame, out_path: Path):
    workbook = xlsxwriter.Workbook(
        str(out_path),
        {
            "constant_memory": True,
            "nan_inf_to_errors": True,
        },
    )
    worksheet = workbook.add_worksheet("suspicious")

    high_fmt = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
    med_fmt = workbook.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500"})

    rows, cols = df_suspicious.shape
    headers = list(df_suspicious.columns)
    worksheet.write_row(0, 0, headers)

    for r_idx, row in enumerate(df_suspicious.itertuples(index=False), start=1):
        values = []
        for value in row:
            if pd.isna(value):
                values.append(None)
            else:
                values.append(value)
        worksheet.write_row(r_idx, 0, values)

    if rows > 0:
        severity_col_idx = df_suspicious.columns.get_loc("severity")
        first_row = 1
        last_row = rows
        first_col = 0
        last_col = cols - 1

        formula_high = f'=INDEX($1:$1048576,ROW(),{severity_col_idx + 1})="high"'
        formula_med = f'=INDEX($1:$1048576,ROW(),{severity_col_idx + 1})="medium"'

        worksheet.conditional_format(
            first_row, first_col, last_row, last_col,
            {"type": "formula", "criteria": formula_high, "format": high_fmt}
        )
        worksheet.conditional_format(
            first_row, first_col, last_row, last_col,
            {"type": "formula", "criteria": formula_med, "format": med_fmt}
        )

    worksheet.freeze_panes(1, 0)
    workbook.close()


def safe_stem(filename: str) -> str:
    stem = Path(filename).stem
    return re.sub(r"[^A-Za-z0-9._-]+", "_", stem)


def build_zero_inclusive_variant(df: pd.DataFrame) -> pd.DataFrame:
    variant = df.copy()
    if "exp_to_imp_ratio_zero_inclusive" in variant.columns:
        variant["exp_to_imp_ratio"] = variant["exp_to_imp_ratio_zero_inclusive"]
    return variant


def main():
    args = parse_args()
    data_dir = detect_data_dir()
    files = sorted(data_dir.glob("*_country_pairs_*.csv"))
    excluded_codes = set() if args.keep_excluded else load_excluded_codes()

    all_parts = []
    for path in files:
        part = compare_file(path, args, excluded_codes)
        if not part.empty:
            all_parts.append(part)

    if not all_parts:
        print("No comparable data found.")
        return

    all_df = pd.concat(all_parts, ignore_index=True)
    all_df = all_df.sort_values(
        by=["is_suspicious", "severity", "abs_gap", "mismatch_pct"],
        ascending=[False, True, False, False],
    )
    all_df_zero_inclusive = build_zero_inclusive_variant(all_df)

    suspicious_df = all_df[all_df["is_suspicious"]].copy()
    suspicious_df_zero_inclusive = all_df_zero_inclusive[all_df_zero_inclusive["is_suspicious"]].copy()

    all_df.to_csv(OUT_ALL, index=False)
    suspicious_df.to_csv(OUT_SUSPICIOUS, index=False)
    all_df_zero_inclusive.to_csv(OUT_ALL_ZERO_INCLUSIVE, index=False)
    suspicious_df_zero_inclusive.to_csv(OUT_SUSPICIOUS_ZERO_INCLUSIVE, index=False)
    xlsx_df = suspicious_df.sort_values(by=["abs_gap", "mismatch_pct"], ascending=[False, False]).copy()
    xlsx_df_zero_inclusive = suspicious_df_zero_inclusive.sort_values(
        by=["abs_gap", "mismatch_pct"],
        ascending=[False, False],
    ).copy()
    xlsx_limited = False
    if args.xlsx_max_rows > 0 and len(xlsx_df) > args.xlsx_max_rows:
        xlsx_df = xlsx_df.head(args.xlsx_max_rows).copy()
        xlsx_df_zero_inclusive = xlsx_df_zero_inclusive.head(args.xlsx_max_rows).copy()
        xlsx_limited = True

    if not args.no_xlsx:
        write_highlighted_excel(xlsx_df, OUT_XLSX)
        write_highlighted_excel(xlsx_df_zero_inclusive, OUT_XLSX_ZERO_INCLUSIVE)

    OUT_BY_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR.mkdir(parents=True, exist_ok=True)
    written_source_files = 0
    for source_file, src_df in all_df.groupby("source_file", sort=True):
        src_all = src_df.sort_values(
            by=["is_suspicious", "severity", "abs_gap", "mismatch_pct"],
            ascending=[False, True, False, False],
        ).copy()
        src_all_zero_inclusive = build_zero_inclusive_variant(src_all)
        src_suspicious = src_all[src_all["is_suspicious"]].copy()
        src_suspicious_zero_inclusive = src_all_zero_inclusive[src_all_zero_inclusive["is_suspicious"]].copy()
        src_key = safe_stem(source_file)

        src_all_path = OUT_BY_SOURCE_DIR / f"{src_key}_mirror_mismatch_all.csv"
        src_suspicious_path = OUT_BY_SOURCE_DIR / f"{src_key}_mirror_mismatch_suspicious.csv"
        src_all_zero_inclusive_path = (
            OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR / f"{src_key}_mirror_mismatch_all_zero_inclusive.csv"
        )
        src_suspicious_zero_inclusive_path = (
            OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR / f"{src_key}_mirror_mismatch_suspicious_zero_inclusive.csv"
        )
        src_all.to_csv(src_all_path, index=False)
        src_suspicious.to_csv(src_suspicious_path, index=False)
        src_all_zero_inclusive.to_csv(src_all_zero_inclusive_path, index=False)
        src_suspicious_zero_inclusive.to_csv(src_suspicious_zero_inclusive_path, index=False)

        if not args.no_xlsx:
            src_xlsx = src_suspicious.sort_values(
                by=["abs_gap", "mismatch_pct"], ascending=[False, False]
            ).copy()
            src_xlsx_zero_inclusive = src_suspicious_zero_inclusive.sort_values(
                by=["abs_gap", "mismatch_pct"], ascending=[False, False]
            ).copy()
            if args.xlsx_max_rows > 0 and len(src_xlsx) > args.xlsx_max_rows:
                src_xlsx = src_xlsx.head(args.xlsx_max_rows).copy()
                src_xlsx_zero_inclusive = src_xlsx_zero_inclusive.head(args.xlsx_max_rows).copy()
            src_xlsx_path = OUT_BY_SOURCE_DIR / f"{src_key}_mirror_mismatch_suspicious.xlsx"
            src_xlsx_zero_inclusive_path = (
                OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR / f"{src_key}_mirror_mismatch_suspicious_zero_inclusive.xlsx"
            )
            write_highlighted_excel(src_xlsx, src_xlsx_path)
            write_highlighted_excel(src_xlsx_zero_inclusive, src_xlsx_zero_inclusive_path)
        written_source_files += 1

    print("Written files:")
    print(f"- {OUT_ALL}")
    print(f"- {OUT_SUSPICIOUS}")
    print(f"- {OUT_ALL_ZERO_INCLUSIVE}")
    print(f"- {OUT_SUSPICIOUS_ZERO_INCLUSIVE}")
    if args.no_xlsx:
        print("- Excel output skipped (--no-xlsx)")
    else:
        print(f"- {OUT_XLSX}")
        print(f"- {OUT_XLSX_ZERO_INCLUSIVE}")
    print(f"Total rows checked: {len(all_df)}")
    print(f"Suspicious rows: {len(suspicious_df)}")
    print(f"Per-source outputs folder: {OUT_BY_SOURCE_DIR} ({written_source_files} sources)")
    print(
        f"Per-source zero-inclusive outputs folder: "
        f"{OUT_BY_SOURCE_ZERO_INCLUSIVE_DIR} ({written_source_files} sources)"
    )
    if xlsx_limited and not args.no_xlsx:
        print(f"Excel rows written: {len(xlsx_df)} (limited by --xlsx-max-rows)")
    if excluded_codes:
        print(f"Excluded identifier codes: {len(excluded_codes)}")


if __name__ == "__main__":
    main()
