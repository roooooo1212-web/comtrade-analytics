# Notebooks

These notebooks wrap the existing project scripts without replacing them.

Files:
- `00_project_index.ipynb` - quick entry point listing the notebook set.
- `00_fetcher_pipeline.ipynb` - raw data fetch, Excel compilation, row counts.
- `01_aggregator_pipeline.ipynb` - country-pair aggregation, enrichment, exports, mismatch summaries.
- `02_mirror_mismatch_pipeline.ipynb` - mirrored-flow mismatch detection and graph generation.

Stage handoff:
- `1aggregator` expects raw CSV inputs in `1aggregator/data_raw/`.
- If you generated files with `0fetcher`, copy them from `0fetcher/data_raw/` into `1aggregator/data_raw/` before running the aggregator notebook or scripts.

Design:
- The original scripts in `0fetcher/`, `1aggregator/`, and `2mismatch/` are unchanged.
- Each notebook uses `runpy` and temporarily switches into the script directory so relative paths still work.
- If you need different CLI arguments for `2mismatch/0_flow_mirror_mismatch.py`, edit the corresponding notebook code cell.
- Before running `0fetcher/0fetcher.py`, set `UN_COMTRADE_SUBSCRIPTION_KEY` in your shell or place it in `.env` at the repo root.
