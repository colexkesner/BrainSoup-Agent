# INFORMS ALICE Analytics Agent

## Quickstart
1. `python -m pip install -r requirements.txt`
2. `python -m src.run_pipeline --config config/config.yaml`
3. Outputs appear in `outputs/powerbi`, `outputs/reports`, and `outputs/logs`.

## Human-in-the-loop workflow
- Web research recommends external datasets but does **not** ingest automatically.
- Dataset decisions are persisted to `config/approvals.yaml`.
- Provide Bulloch neighbors in `config/neighbors.yaml` or approve an adjacency dataset.

## Tests
- `pytest -q`
