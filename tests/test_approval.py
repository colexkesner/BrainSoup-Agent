from pathlib import Path

from src.approval import approval_gate
from src.utils import load_yaml


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

    raw = load_yaml(approvals_path)
    assert raw["approved_datasets"][0]["name"] == "Test Dataset"


def test_approval_gate_noninteractive_prompt_alias(tmp_path: Path):
    approvals_path = tmp_path / "approvals.yaml"
    approvals_path.write_text("approved_datasets: []\n", encoding="utf-8")
    out = approval_gate({"recommended_datasets": [{"name": "X", "join_keys": []}]}, str(approvals_path), hitl_mode="noninteractive_prompt")
    assert out["approved_datasets"][0]["status"] == "pending_user_input"
