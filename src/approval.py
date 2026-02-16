from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console

from src.utils import load_yaml, save_yaml

console = Console()


def approval_gate(research: dict[str, Any], approvals_path: str, non_interactive_default_reject: bool = True) -> dict[str, Any]:
    approvals = load_yaml(approvals_path)
    approvals.setdefault("approved_datasets", [])
    approved_names = {x.get("name") for x in approvals["approved_datasets"]}

    for item in research.get("recommended_datasets", []):
        if item.get("name") in approved_names:
            continue
        approve = False
        if non_interactive_default_reject:
            approve = False
        else:
            console.print(f"Dataset recommendation: [bold]{item.get('name')}[/bold]")
            console.print(f"Purpose: {item.get('purpose')}")
            ans = input("Approve dataset for ingestion? [y/N]: ").strip().lower()
            approve = ans == "y"
        if approve:
            approvals["approved_datasets"].append(
                {
                    "name": item.get("name"),
                    "source_url": (item.get("suggested_sources") or [None])[0],
                    "join_keys": item.get("join_keys", []),
                    "approved_at": datetime.now(timezone.utc).isoformat(),
                    "status": "approved",
                }
            )
        else:
            approvals["approved_datasets"].append(
                {
                    "name": item.get("name"),
                    "source_url": (item.get("suggested_sources") or [None])[0],
                    "join_keys": item.get("join_keys", []),
                    "approved_at": None,
                    "status": "rejected_or_pending",
                }
            )
    save_yaml(approvals_path, approvals)
    return approvals


def load_neighbors(path: str) -> list[str]:
    if not Path(path).exists():
        return []
    data = load_yaml(path)
    return data.get("bulloch_neighbors", []) or []
