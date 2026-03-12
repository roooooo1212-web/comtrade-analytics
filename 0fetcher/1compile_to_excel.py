# -*- coding: utf-8 -*-
"""
compile_to_excel.py

Берёт все CSV из папки data_raw/ и собирает их в один Excel-файл,
где каждый CSV -> отдельный лист (sheet / page).

Имена листов берутся из имён файлов (без .csv), но:
 - обрезаются до 31 символа (ограничение Excel)
 - чистятся от запрещённых символов: []:*?/\\
"""

import pandas as pd
from pathlib import Path
import re


# ===== НАСТРОЙКИ =====

# Папка, где лежат .csv, созданные fetcher.py
DATA_DIR = Path("data_raw")

# Имя выходного Excel-файла
OUTPUT_EXCEL = Path("compiled_trade_data.xlsx")


def make_sheet_name(filename_stem: str, existing_names: set) -> str:
    """
    Делает валидное имя листа Excel:
      - убирает запрещённые символы
      - обрезает до 31 символа
      - гарантирует уникальность (добавляет суффикс _1, _2, ...)
    """
    # Удаляем запрещённые символы
    name = re.sub(r'[\[\]\:\*\?\/\\]', "_", filename_stem)

    # Обрезаем до 31 символа
    name = name[:31] if len(name) > 31 else name

    # Если получилось пусто (на всякий случай)
    if not name:
        name = "Sheet"

    base = name
    counter = 1
    # Гарантия уникальности
    while name in existing_names:
        # Добавляем суффикс, при этом следим за длиной 31
        suffix = f"_{counter}"
        max_base_len = 31 - len(suffix)
        name = (base[:max_base_len] if len(base) > max_base_len else base) + suffix
        counter += 1

    existing_names.add(name)
    return name


def main():
    if not DATA_DIR.exists():
        print(f"❌ Папка {DATA_DIR} не найдена")
        return

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ В папке {DATA_DIR} нет CSV-файлов")
        return

    print(f"Найдено CSV-файлов: {len(csv_files)}")
    print(f"Создаём Excel-файл: {OUTPUT_EXCEL}")

    used_sheet_names = set()

    # Создаём ExcelWriter
    with pd.ExcelWriter(OUTPUT_EXCEL, engine="xlsxwriter") as writer:
        for csv_path in csv_files:
            print(f"→ Обрабатываем {csv_path.name} ...")
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                print(f"   ⚠️ Ошибка чтения {csv_path.name}: {e}")
                continue

            sheet_name = make_sheet_name(csv_path.stem, used_sheet_names)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ Готово! Excel-файл сохранён как: {OUTPUT_EXCEL}")


if __name__ == "__main__":
    main()
