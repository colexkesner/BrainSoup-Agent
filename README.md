# INFORMS ALICE Analytics Agent

## Quickstart
1. `python -m pip install -r requirements.txt`
2. `python -m src.run_pipeline --config config/config.yaml`
3. Outputs appear in `outputs/powerbi`, `outputs/reports`, and `outputs/logs`.

## HITL Modes
- `interactive`: prompt for approval in terminal.
- `auto_reject`: auto-mark recommendations as rejected/pending.
- `noninteractive_prompt`: write pending recommendations without approving.

Override at runtime:
- `python -m src.run_pipeline --config config/config.yaml --hitl-mode interactive`

## External dataset ingestion
- Upload approved files to `data/raw/approved/`.
- Map each recommendation to `local_file` in `config/approvals.yaml` (or via Streamlit).
- Join supports `fips+year` or `county_name_norm+state+year`.
- Join is blocked if match rate is below `external_join_min_match_rate` unless `allow_low_match_override: true`.

## Streamlit UI
- `streamlit run streamlit_app.py`
- Features: run pipeline, approve/reject recommendations, map approved dataset file paths, upload files, and download `outputs/powerbi/*.csv`.

## Tests
- `pytest -q`
