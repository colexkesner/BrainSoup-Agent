from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console

from src.utils import load_yaml, save_yaml

console = Console()

VALID_HITL_MODES = {
    "interactive",
    "auto_reject",
    "noninteractive_ui",
    "noninteractive_prompt",
}


def _normalize_approval_item(item: dict[str, Any], status: str) -> dict[str, Any]:
    approved_at = datetime.now(timezone.utc).isoformat() if status == "approved" else None
    return {
        "name": item.get("name"),
        "source_url": (item.get("suggested_sources") or [None])[0],
        "join_keys": item.get("join_keys", []),
        "approved_at": approved_at,
        "status": status,
        "local_file": item.get("local_file"),
        "allow_low_match_override": bool(item.get("allow_low_match_override", False)),
    }


def approval_gate(research: dict[str, Any], approvals_path: str, hitl_mode: str = "auto_reject") -> dict[str, Any]:
    if hitl_mode not in VALID_HITL_MODES:
        raise ValueError(f"Unsupported hitl_mode={hitl_mode}; expected one of {sorted(VALID_HITL_MODES)}")

    approvals = load_yaml(approvals_path)
    approvals.setdefault("approved_datasets", [])
    existing = {x.get("name") for x in approvals["approved_datasets"]}
    is_tty = sys.stdin.isatty()

    for item in research.get("recommended_datasets", []):
        if item.get("name") in existing:
            continue

        if hitl_mode == "auto_reject":
            status = "rejected_or_pending"
        elif hitl_mode in {"noninteractive_ui", "noninteractive_prompt"}:
            status = "pending_user_input"
            console.print(f"[yellow]Pending UI approval:[/yellow] {item.get('name')} ({item.get('purpose')})")
        else:
            if not is_tty:
                status = "pending_user_input"
                console.print(f"[yellow]TTY unavailable; marked pending:[/yellow] {item.get('name')}")
            else:
                console.print(f"Dataset recommendation: [bold]{item.get('name')}[/bold]")
                console.print(f"Purpose: {item.get('purpose')}")
                console.print(f"Sources: {', '.join(item.get('suggested_sources', []))}")
                ans = input("Approve dataset for ingestion? [y/N]: ").strip().lower()
                status = "approved" if ans == "y" else "rejected_or_pending"

        approvals["approved_datasets"].append(_normalize_approval_item(item, status=status))

    save_yaml(approvals_path, approvals)
    return approvals


def load_neighbors(path: str) -> list[str]:
    if not Path(path).exists():
        return []
    data = load_yaml(path)
    return data.get("bulloch_neighbors", []) or []
