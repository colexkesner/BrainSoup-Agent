# INFORMS ALICE Analytics Agent

## Quickstart
1. `python -m pip install -r requirements.txt`
2. `python -m src.run_pipeline --config config/config.yaml`
3. Outputs appear in `outputs/powerbi`, `outputs/reports`, and `outputs/logs`.

## HITL Modes
- `interactive`: prompt for approvals in terminal.
- `auto_reject`: auto-mark recommendations as rejected/pending.
- `noninteractive_ui`: do not prompt; persist recommendations as pending for Streamlit decisions.

Override at runtime:
- `python -m src.run_pipeline --config config/config.yaml --hitl-mode interactive`

## External dataset ingestion
- Upload approved files to `data/raw/approved/`.
- Map each recommendation to `local_file` in `config/approvals.yaml` (or via Streamlit).
- Only records with `status: approved` are ingested.
- Join supports `fips+year` (preferred) or `county_name_norm+state+year`.
- Low match-rate joins are blocked by `external_join_min_match_rate` unless `allow_low_match_override: true`.
- Enriched data is exported to both `fact_county_year.csv` and `fact_enriched.csv`.

## Streamlit UI
- `streamlit run streamlit_app.py`
- Features:
  - Run pipeline in `noninteractive_ui` mode.
  - Render latest recommendations from `outputs/logs/research_recommendations.json`.
  - Approve/reject each dataset and persist to `config/approvals.yaml`.
  - Upload external files and map uploaded filenames to approved datasets.
  - Download `outputs/powerbi/*.csv`.

## Tests
- `pytest -q`
