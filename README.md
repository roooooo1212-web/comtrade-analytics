UN Comtrade key: https://comtradedeveloper.un.org/profile
Set the key either in a shell env var:
`$env:UN_COMTRADE_SUBSCRIPTION_KEY="your-key"`
or in a repo-level `.env` file:
`UN_COMTRADE_SUBSCRIPTION_KEY=your-key`

Pipeline

1. 0fetcher/0fetcher.py -> raw CSV in 0fetcher/data_raw (flows X/M).
2. Copy the raw CSV files from `0fetcher/data_raw/` into `1aggregator/data_raw/`.
3. 1aggregator/0aggregator.py -> country pairs in 1aggregator/data_pairs.
   Direction is normalized: exporterISO -> importerISO for both X and M.
4. 1aggregator/1add_country_names.py -> adds exporterName/importerName into data_pairs_named.
5. 1aggregator/2pairs_to_excel_highlighted.py -> Excel with UN member highlight.
6. 1aggregator/3select_countries_for_network.py -> included/excluded country lists.

Notes

- After normalization, exp_YYYY and imp_YYYY are comparable in the same direction (exporter -> importer).
- `1aggregator/0aggregator.py` reads from `1aggregator/data_raw/`, so populate that folder before running the aggregator stage.
- Notebook wrappers are available in `notebooks/`:
  - `00_project_index.ipynb`
  - `00_fetcher_pipeline.ipynb`
  - `01_aggregator_pipeline.ipynb`
  - `02_mirror_mismatch_pipeline.ipynb`
- The notebooks run the existing source scripts in place. The `.py` files remain the source of truth.
