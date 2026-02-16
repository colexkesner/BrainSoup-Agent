from pathlib import Path

import yaml

from src.approval import approval_gate


def test_approval_gate_noninteractive_ui_writes_pending(tmp_path: Path):
    approvals_path = tmp_path / "approvals.yaml"
    approvals_path.write_text("approved_datasets: []\n", encoding="utf-8")
    research = {
        "recommended_datasets": [
            {
                "name": "Test Dataset",
                "purpose": "Testing",
                "suggested_sources": ["https://example.com"],
                "join_keys": ["fips", "year"],
            }
        ]
    }
    out = approval_gate(research, str(approvals_path), hitl_mode="noninteractive_ui")
    assert out["approved_datasets"][0]["status"] == "pending_user_input"

    raw = yaml.safe_load(approvals_path.read_text(encoding="utf-8"))
    assert raw["approved_datasets"][0]["name"] == "Test Dataset"
