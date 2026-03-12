# -*- coding: utf-8 -*-
"""
5_mismatch_analysis.py

Полный (unlimited) список mismatch-кодов + сводки:
- mismatch_top_all.csv (все коды, отсортировано по частоте)
- mismatch_by_file.csv (доля mismatch по товарным файлам)
- mismatch_by_year.csv (доля mismatch по годам)
- mismatch_summary.md (готовый текст с ключевыми цифрами)
"""

from pathlib import Path
from collections import Counter, defaultdict
import csv
import re
import ast


ROOT = Path("..").resolve()
DATA_DIR = ROOT / "1aggregator" / "data_pairs_named"
EXCLUDED_FILE = ROOT / "1aggregator" / "identifiers_excluded_from_network.csv"
INCLUDED_FILE = ROOT / "1aggregator" / "countries_included_for_network.csv"
UN_SOURCE = ROOT / "1aggregator" / "3select_countries_for_network.py"

OUT_TOP_ALL = ROOT / "1aggregator" / "mismatch_top_all.csv"
OUT_BY_FILE = ROOT / "1aggregator" / "mismatch_by_file.csv"
OUT_BY_YEAR = ROOT / "1aggregator" / "mismatch_by_year.csv"
OUT_SUMMARY = ROOT / "1aggregator" / "mismatch_summary.md"


def parse_un_members(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    idx = text.find("UN_MEMBER_CODES")
    if idx == -1:
        return set()
    start = text.find("{", idx)
    if start == -1:
        return set()
    level = 0
    end = None
    for i in range(start, len(text)):
        if text[i] == "{":
            level += 1
        elif text[i] == "}":
            level -= 1
            if level == 0:
                end = i
                break
    if end is None:
        return set()
    return ast.literal_eval(text[start : end + 1])


def load_excluded(path: Path) -> dict[str, dict]:
    out = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            if not code:
                continue
            out[code] = {
                "name": row.get("name", ""),
                "is_iso_like": row.get("is_iso_like", ""),
                "reason": row.get("reason", ""),
            }
    return out


def load_included(path: Path) -> set[str]:
    if not path.exists():
        return set()
    codes = set()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            if code:
                codes.add(code)
    return codes


def parse_year_columns(headers: list[str]) -> list[int]:
    years = set()
    for h in headers:
        m = re.match(r"(exp|imp)_(\d{4})$", h)
        if m:
            years.add(int(m.group(2)))
    return sorted(years)


def to_float(val: str) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def main():
    if not DATA_DIR.exists():
        print(f"❌ Нет папки: {DATA_DIR}")
        return

    un_members = parse_un_members(UN_SOURCE)
    excluded_map = load_excluded(EXCLUDED_FILE)
    included_codes = load_included(INCLUDED_FILE)

    # глобальные счётчики
    code_counts = Counter()
    file_stats = []
    year_totals = Counter()      # год -> всего строк с ненулевыми значениями
    year_mismatch = Counter()    # год -> mismatch строк

    files = sorted(DATA_DIR.glob("*.csv"))
    if not files:
        print(f"❌ Нет файлов в {DATA_DIR}")
        return

    for path in files:
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            years = parse_year_columns(headers)

            total_rows = 0
            mismatch_rows = 0

            for row in reader:
                total_rows += 1
                exp = (row.get("exporterISO") or "").strip()
                imp = (row.get("importerISO") or "").strip()

                exp_bad = exp not in un_members
                imp_bad = imp not in un_members
                row_mismatch = exp_bad or imp_bad

                if row_mismatch:
                    mismatch_rows += 1
                    if exp:
                        code_counts[exp] += 1
                    if imp:
                        code_counts[imp] += 1

                # разрез по годам: строка учитывается в году,
                # если есть ненулевое значение exp/imp за этот год
                for y in years:
                    v_exp = to_float(row.get(f"exp_{y}", "0"))
                    v_imp = to_float(row.get(f"imp_{y}", "0"))
                    if v_exp != 0 or v_imp != 0:
                        year_totals[y] += 1
                        if row_mismatch:
                            year_mismatch[y] += 1

            share = (mismatch_rows / total_rows * 100) if total_rows else 0
            file_stats.append(
                {
                    "file": path.name,
                    "rows": total_rows,
                    "mismatch_rows": mismatch_rows,
                    "mismatch_share_pct": round(share, 2),
                }
            )

    # --- записываем TOP (unlimited) ---
    with OUT_TOP_ALL.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "count", "is_iso_like", "reason", "name"])
        for code, cnt in code_counts.most_common():
            meta = excluded_map.get(code, {})
            writer.writerow([
                code,
                cnt,
                meta.get("is_iso_like", ""),
                meta.get("reason", ""),
                meta.get("name", ""),
            ])

    # --- mismatch by file ---
    file_stats_sorted = sorted(file_stats, key=lambda x: x["mismatch_share_pct"], reverse=True)
    with OUT_BY_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["file", "rows", "mismatch_rows", "mismatch_share_pct"],
        )
        writer.writeheader()
        for row in file_stats_sorted:
            writer.writerow(row)

    # --- mismatch by year ---
    with OUT_BY_YEAR.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["year", "rows", "mismatch_rows", "mismatch_share_pct"])
        for y in sorted(year_totals.keys()):
            total = year_totals[y]
            mism = year_mismatch[y]
            share = (mism / total * 100) if total else 0
            writer.writerow([y, total, mism, round(share, 2)])

    # --- summary markdown ---
    n_excluded = len(excluded_map)
    n_included = len(included_codes)
    n_all = len(set(excluded_map.keys()) | included_codes)
    iso_true = sum(1 for v in excluded_map.values() if str(v.get("is_iso_like")) == "True")
    iso_false = sum(1 for v in excluded_map.values() if str(v.get("is_iso_like")) == "False")
    share_true = (iso_true / n_excluded * 100) if n_excluded else 0
    share_false = (iso_false / n_excluded * 100) if n_excluded else 0

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        f.write("# Mismatch Summary\n\n")
        f.write(f"- Unique codes total: {n_all}\n")
        f.write(f"- Included UN codes: {n_included}\n")
        f.write(f"- Mismatch codes: {n_excluded}\n")
        f.write(f"- ISO-like mismatch: {iso_true} ({share_true:.2f}%)\n")
        f.write(f"- Non-ISO-like mismatch: {iso_false} ({share_false:.2f}%)\n\n")

        f.write("## Top mismatch codes (unlimited list in mismatch_top_all.csv)\n")
        top_preview = code_counts.most_common(10)
        for code, cnt in top_preview:
            f.write(f"- {code}: {cnt}\n")

    print("✅ Written:")
    print(f"- {OUT_TOP_ALL}")
    print(f"- {OUT_BY_FILE}")
    print(f"- {OUT_BY_YEAR}")
    print(f"- {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
