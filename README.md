# INFORMS ALICE Analytics Agent

## Quickstart
1. `python -m pip install -r requirements.txt`
2. `python -m src.run_pipeline --config config/config.yaml --hitl-mode auto_reject`
3. Outputs appear in `outputs/powerbi`, `outputs/reports`, and `outputs/logs`.

## HITL Modes
- `interactive`: prompt for approvals in terminal.
- `auto_reject`: auto-mark recommendations as rejected/pending.
- `noninteractive_ui`: never prompt; keep recommendations pending until Streamlit/UI writes approvals.

## How to approve datasets
1. Run pipeline once to generate recommendations:
   - `python -m src.run_pipeline --config config/config.yaml --hitl-mode noninteractive_ui`
2. Open UI:
   - `streamlit run streamlit_app.py`
3. In UI, approve/reject each dataset, map a local filename under `data/raw/approved/`, and save approvals.
4. Re-run pipeline (any mode) to ingest approved datasets and produce `fact_enriched.csv` + `join_audit.csv`.

## External dataset ingestion and join safety
- Only `status: approved` entries from `config/approvals.yaml` are ingested.
- Supported files: `csv`, `xlsx`, `zip` (zip must include csv/xlsx).
- Preferred join keys: `fips+year`; optional `county_name_norm+state+year` when enabled.
- Match-rate gate: below `external_join_min_match_rate` is blocked unless `allow_low_match_override: true`.
- Audit files:
  - `outputs/powerbi/join_audit.csv`
  - `outputs/powerbi/provenance.csv`

## Driver analysis (config-gated)
- Controlled by `use_external_features_in_driver_analysis` (default `false`).
- Uses only ingested external features and optional `external_feature_allowlist`.
- Outputs (when enabled and features available):
  - `outputs/powerbi/drivers_2023.csv`
  - `outputs/powerbi/drivers_5yr.csv`
- Important: these are predictive/associative signals, not causal conclusions.

## Streamlit dependency smoke check
- Streamlit is included in `requirements.txt`.
- Quick smoke check:
  - `python -c "import streamlit; print(streamlit.__version__)"`

## How to verify locally
- `pytest`
- `python -m src.run_pipeline --config config/config.yaml --hitl-mode auto_reject`
- `python -m compileall src tests streamlit_app.py`
