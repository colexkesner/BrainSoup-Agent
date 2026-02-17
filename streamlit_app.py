from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import streamlit as st
import yaml

st.title("INFORMS ALICE Analytics Agent")

BASE_DIR = Path(__file__).resolve().parent
config_path = BASE_DIR / "config" / "config.yaml"
approvals_path = BASE_DIR / "config" / "approvals.yaml"
recommendations_path = BASE_DIR / "outputs" / "logs" / "research_recommendations.json"
approved_data_dir = BASE_DIR / "data" / "raw" / "approved"


def _load_yaml(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    if path.exists():
        return yaml.safe_load(path.read_text(encoding="utf-8")) or fallback
    return fallback


def _load_recommendations(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"recommended_datasets": []}
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return yaml.safe_load(text) or {"recommended_datasets": []}


def _save_approvals(data: dict[str, Any]) -> None:
    approvals_path.parent.mkdir(parents=True, exist_ok=True)
    approvals_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _persist_state() -> None:
    _save_approvals(st.session_state.approvals_data)


def _upsert_recommended(recommendations: dict[str, Any]) -> None:
    approvals_data = st.session_state.approvals_data
    approvals_data.setdefault("approved_datasets", [])
    existing = {d.get("name"): d for d in approvals_data.get("approved_datasets", []) if d.get("name")}

    for ds in recommendations.get("recommended_datasets", []):
        name = ds.get("name")
        if not name:
            continue

        current = existing.get(name, {})
        current.setdefault("name", name)
        current["source_url"] = current.get("source_url") or (ds.get("suggested_sources") or [None])[0]
        current["join_keys"] = current.get("join_keys") or ds.get("join_keys", [])
        current.setdefault("status", "pending_user_input")
        current.setdefault("approved_at", None)
        current.setdefault("local_file", None)
        current.setdefault("allow_low_match_override", False)
        existing[name] = current

    approvals_data["approved_datasets"] = list(existing.values())


def _set_status(dataset_name: str, status: str) -> None:
    for item in st.session_state.approvals_data.get("approved_datasets", []):
        if item.get("name") != dataset_name:
            continue

        prior_status = item.get("status")
        item["status"] = status
        if status == "approved" and not item.get("approved_at"):
            item["approved_at"] = datetime.now(timezone.utc).isoformat()
        elif status != "approved" and prior_status != status:
            item["approved_at"] = None
        break
    _persist_state()


def _set_local_file(dataset_name: str) -> None:
    local_value = st.session_state.get(f"local_{dataset_name}", "")
    for item in st.session_state.approvals_data.get("approved_datasets", []):
        if item.get("name") == dataset_name:
            item["local_file"] = local_value or None
            break
    _persist_state()


def _set_allow_override(dataset_name: str) -> None:
    override_value = bool(st.session_state.get(f"override_{dataset_name}", False))
    for item in st.session_state.approvals_data.get("approved_datasets", []):
        if item.get("name") == dataset_name:
            item["allow_low_match_override"] = override_value
            break
    _persist_state()


def _save_selected_mapping() -> None:
    target = st.session_state.get("map_target")
    mapped_file = st.session_state.get("mapped_file", "").strip()
    if not target or not mapped_file:
        return

    for item in st.session_state.approvals_data.get("approved_datasets", []):
        if item.get("name") == target:
            item["local_file"] = mapped_file
            break

    _persist_state()


if "approvals_data" not in st.session_state:
    st.session_state.approvals_data = _load_yaml(approvals_path, {"approved_datasets": []})

recommendations = _load_recommendations(recommendations_path)
_upsert_recommended(recommendations)

if st.button("Run Pipeline"):
    subprocess.run(
        [sys.executable, "-m", "src.run_pipeline", "--config", str(config_path), "--hitl-mode", "noninteractive_ui"],
        cwd=BASE_DIR,
        check=False,
    )
    st.success("Pipeline command executed.")

report_path = BASE_DIR / "outputs" / "reports" / "report.md"
if report_path.exists():
    st.subheader("Report")
    st.markdown(report_path.read_text(encoding="utf-8"))

st.subheader("Dataset Recommendations & Approvals")
existing = {d.get("name"): d for d in st.session_state.approvals_data.get("approved_datasets", []) if d.get("name")}

for ds in recommendations.get("recommended_datasets", []):
    name = ds.get("name")
    if not name:
        continue

    item = existing.get(name, {})
    status = item.get("status", "pending_user_input")

    with st.container(border=True):
        st.markdown(f"**{name}**")
        st.write(ds.get("purpose"))
        st.write("Sources:", ds.get("suggested_sources", []))
        st.write("Join keys:", ds.get("join_keys", []))

        col1, col2, col3 = st.columns([1, 1, 2])
        col1.button(
            f"Approve {name}",
            key=f"approve_{name}",
            on_click=_set_status,
            args=(name, "approved"),
        )
        col2.button(
            f"Reject {name}",
            key=f"reject_{name}",
            on_click=_set_status,
            args=(name, "rejected_or_pending"),
        )

        col3.text_input(
            f"Local file mapping for {name}",
            value=item.get("local_file") or "",
            key=f"local_{name}",
            help="Absolute path or filename under data/raw/approved/",
            on_change=_set_local_file,
            args=(name,),
        )

        st.checkbox(
            f"Allow low match-rate override for {name}",
            value=bool(item.get("allow_low_match_override", False)),
            key=f"override_{name}",
            on_change=_set_allow_override,
            args=(name,),
        )

        st.caption(f"Current status: {status}")

if st.button("Save approvals"):
    _persist_state()
    st.success("Approvals saved to config/approvals.yaml")

st.subheader("Upload approved external dataset file")
uploaded = st.file_uploader("Upload file to data/raw/approved")
if uploaded:
    out = approved_data_dir / uploaded.name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(uploaded.read())
    st.success(f"Saved to {out}")

approved_names = [x.get("name") for x in st.session_state.approvals_data.get("approved_datasets", []) if x.get("status") == "approved"]
if approved_names:
    st.selectbox("Map uploaded filename to approved dataset", approved_names, key="map_target")
    st.text_input("Filename in data/raw/approved", value="", key="mapped_file")
    if st.button("Save dataset file mapping", on_click=_save_selected_mapping):
        st.success(f"Mapped {st.session_state.get('map_target')} -> {st.session_state.get('mapped_file')}")

st.caption(f"Approvals file: {approvals_path}")
if approvals_path.exists():
    modified_at = datetime.fromtimestamp(approvals_path.stat().st_mtime, tz=timezone.utc).isoformat()
    st.caption(f"Approvals last modified (UTC): {modified_at}")
else:
    st.caption("Approvals file does not exist yet.")

st.subheader("Power BI Exports")
for p in sorted((BASE_DIR / "outputs" / "powerbi").glob("*.csv")):
    st.download_button(f"Download {p.name}", p.read_bytes(), file_name=p.name)
