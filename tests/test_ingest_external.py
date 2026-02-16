from pathlib import Path

import pytest
pd = pytest.importorskip("pandas")

from src.ingest_external import ingest_approved_datasets


def _base_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fips": ["13001", "13003", "13005"],
            "year": [2023, 2023, 2023],
            "county_name_norm": ["county1", "county2", "county3"],
            "state": ["Georgia", "Georgia", "Georgia"],
            "ALICE_pct": [0.3, 0.4, 0.2],
        }
    )


def test_join_audit_created_and_join_success(tmp_path: Path):
    approved_dir = tmp_path / "approved"
    approved_dir.mkdir()
    ext = pd.DataFrame({"fips": ["13001", "13003"], "year": [2023, 2023], "feature_x": [10, 20]})
    file_path = approved_dir / "housing_cost.csv"
    ext.to_csv(file_path, index=False)

    approvals = {
        "approved_datasets": [
            {
                "name": "Housing Cost",
                "status": "approved",
                "join_keys": ["fips", "year"],
                "local_file": str(file_path),
            }
        ]
    }
    cfg = {"approved_data_dir": str(approved_dir), "external_join_min_match_rate": 0.5}

    enriched, join_audit_records, join_audit_df = ingest_approved_datasets(_base_df(), approvals, cfg, str(tmp_path / "run_log.jsonl"))
    assert "ext_housing_cost__feature_x" in enriched.columns
    assert join_audit_records[0]["join_outcome"] == "used"
    assert set(["dataset_name", "match_rate", "blocked_reason"]).issubset(set(join_audit_df.columns))


def test_match_rate_threshold_block(tmp_path: Path):
    approved_dir = tmp_path / "approved"
    approved_dir.mkdir()
    ext = pd.DataFrame({"fips": ["99999"], "year": [2023], "feature_x": [999]})
    file_path = approved_dir / "transport.csv"
    ext.to_csv(file_path, index=False)

    approvals = {
        "approved_datasets": [
            {
                "name": "Transport",
                "status": "approved",
                "join_keys": ["fips", "year"],
                "local_file": str(file_path),
                "allow_low_match_override": False,
            }
        ]
    }
    cfg = {"approved_data_dir": str(approved_dir), "external_join_min_match_rate": 0.8}

    enriched, join_audit_records, _ = ingest_approved_datasets(_base_df(), approvals, cfg, str(tmp_path / "run_log.jsonl"))
    assert "ext_transport__feature_x" not in enriched.columns
    assert "match_rate=" in join_audit_records[0]["blocked_reason"]
