from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"


def md_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.14",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def script_runner_code() -> str:
    return """from __future__ import annotations

from pathlib import Path
import os
import runpy
import sys


ROOT = Path.cwd().resolve().parent if Path.cwd().name == "notebooks" else Path.cwd().resolve()


def run_project_script(relative_path: str, argv: list[str] | None = None) -> None:
    script_path = (ROOT / relative_path).resolve()
    script_dir = script_path.parent
    old_cwd = Path.cwd()
    old_argv = sys.argv[:]
    try:
        os.chdir(script_dir)
        sys.argv = [str(script_path), *(argv or [])]
        runpy.run_path(str(script_path), run_name="__main__")
    finally:
        os.chdir(old_cwd)
        sys.argv = old_argv


def show_stage_files(relative_dir: str, pattern: str = "*") -> list[Path]:
    base = (ROOT / relative_dir).resolve()
    files = sorted(base.glob(pattern))
    for path in files:
        print(path.relative_to(ROOT))
    return files
"""


def stage_notebook(
    title: str,
    description: str,
    stage_dir: str,
    scripts: list[dict],
    extra_notes: list[str] | None = None,
) -> dict:
    cells = [
        md_cell(
            f"# {title}\n\n"
            f"{description}\n\n"
            "This notebook preserves the original `.py` scripts and runs them in place.\n"
        ),
        md_cell(
            "## How To Use\n\n"
            "1. Open this notebook from the repo root or from the `notebooks/` folder.\n"
            "2. Run the setup cell once.\n"
            "3. Run the script cells you need. They execute the existing source files with the correct working directory.\n"
        ),
        code_cell(script_runner_code()),
        code_cell(
            f'print("Stage directory: {stage_dir}")\n'
            f'show_stage_files("{stage_dir}", "*.py")\n'
        ),
    ]

    if extra_notes:
        cells.append(md_cell("## Notes\n\n" + "\n".join(f"- {line}" for line in extra_notes)))

    for item in scripts:
        args = item.get("argv", [])
        arg_literal = json.dumps(args)
        cells.append(
            md_cell(
                f"## `{item['path']}`\n\n"
                f"{item['description']}\n"
            )
        )
        cells.append(
            code_cell(
                f'run_project_script("{item["path"]}", argv={arg_literal})\n'
            )
        )
    return notebook(cells)


def overview_notebook() -> dict:
    cells = [
        md_cell(
            "# Project Notebook Index\n\n"
            "This repo now has notebook entry points for the main pipeline stages while keeping the original scripts unchanged.\n"
        ),
        md_cell(
            "## Stages\n\n"
            "- `00_fetcher_pipeline.ipynb`: raw data download, Excel compilation, row counts.\n"
            "- `01_aggregator_pipeline.ipynb`: country-pair aggregation, enrichment, exports, mismatch summaries.\n"
            "- `02_mirror_mismatch_pipeline.ipynb`: mirrored-flow mismatch detection and visualization.\n"
        ),
        code_cell(
            "from pathlib import Path\n\n"
            "root = Path.cwd().resolve().parent if Path.cwd().name == 'notebooks' else Path.cwd().resolve()\n"
            "for path in sorted((root / 'notebooks').glob('*.ipynb')):\n"
            "    print(path.relative_to(root))\n"
        ),
    ]
    return notebook(cells)


def write_notebook(path: Path, content: dict) -> None:
    path.write_text(json.dumps(content, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)

    write_notebook(NOTEBOOKS_DIR / "00_project_index.ipynb", overview_notebook())

    write_notebook(
        NOTEBOOKS_DIR / "00_fetcher_pipeline.ipynb",
        stage_notebook(
            title="Fetcher Pipeline",
            description="Notebook wrapper around the raw UN Comtrade extraction stage.",
            stage_dir="0fetcher",
            extra_notes=[
                "The fetch step calls the live UN Comtrade API and writes CSV files into `0fetcher/data_raw`.",
                "The fetcher script currently contains a hard-coded subscription key. The notebook does not modify that behavior.",
            ],
            scripts=[
                {
                    "path": "0fetcher/0fetcher.py",
                    "description": "Download raw export and import CSV files for the configured commodities and years.",
                },
                {
                    "path": "0fetcher/1compile_to_excel.py",
                    "description": "Combine raw CSV files into a single multi-sheet Excel workbook.",
                },
                {
                    "path": "0fetcher/2counter.py",
                    "description": "Count rows in each raw CSV file and write `row_counts.csv`.",
                },
            ],
        ),
    )

    write_notebook(
        NOTEBOOKS_DIR / "01_aggregator_pipeline.ipynb",
        stage_notebook(
            title="Aggregator Pipeline",
            description="Notebook wrapper around the country-pair aggregation and mismatch-summary stage.",
            stage_dir="1aggregator",
            extra_notes=[
                "Some scripts depend on relative paths and are executed from `1aggregator/` automatically.",
                "Run the cells in order if you want to rebuild the full stage outputs from raw inputs.",
            ],
            scripts=[
                {
                    "path": "1aggregator/0aggregator.py",
                    "description": "Normalize trade direction and build country-pair pivot tables by commodity.",
                },
                {
                    "path": "1aggregator/1add_country_names.py",
                    "description": "Add `exporterName` and `importerName` columns to the pair tables.",
                },
                {
                    "path": "1aggregator/2pairs_to_excel_highlighted.py",
                    "description": "Export the named pair tables to a highlighted Excel workbook.",
                },
                {
                    "path": "1aggregator/3select_countries_for_network.py",
                    "description": "Build the included and excluded country/code lists for network analysis.",
                },
                {
                    "path": "1aggregator/4_mismatch_histogram.py",
                    "description": "Generate the mismatch histogram PNG and Excel workbook.",
                },
                {
                    "path": "1aggregator/5_mismatch_analysis.py",
                    "description": "Write mismatch summaries by file, by year, and by code frequency.",
                },
                {
                    "path": "1aggregator/6_mismatch_visuals.py",
                    "description": "Render summary PNG charts from the mismatch summary CSV and Markdown outputs.",
                },
            ],
        ),
    )

    write_notebook(
        NOTEBOOKS_DIR / "02_mirror_mismatch_pipeline.ipynb",
        stage_notebook(
            title="Mirror Mismatch Pipeline",
            description="Notebook wrapper around mirrored export/import mismatch detection and graph generation.",
            stage_dir="2mismatch",
            extra_notes=[
                "The first script supports CLI arguments. The default cell below uses its default thresholds.",
                "Edit the argument list in the code cell if you want different thresholds without touching the source script.",
            ],
            scripts=[
                {
                    "path": "2mismatch/0_flow_mirror_mismatch.py",
                    "description": "Compare A->B exports against mirrored B<-A imports and write suspicious mismatch tables.",
                    "argv": [],
                },
                {
                    "path": "2mismatch/1_mirror_mismatch_visuals.py",
                    "description": "Generate graphs from `mirror_mismatch_suspicious.csv`.",
                },
                {
                    "path": "2mismatch/2_mirror_mismatch_visuals_all.py",
                    "description": "Generate graphs from `mirror_mismatch_all.csv`.",
                },
            ],
        ),
    )


if __name__ == "__main__":
    main()
