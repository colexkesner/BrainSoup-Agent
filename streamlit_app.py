from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import yaml

st.title("INFORMS ALICE Analytics Agent")

config_path = Path("config/config.yaml")
approvals_path = Path("config/approvals.yaml")
recommendations_path = Path("outputs/logs/research_recommendations.json")


def _load_yaml(path: Path, fallback: dict) -> dict:
    if path.exists():
        return yaml.safe_load(path.read_text(encoding="utf-8")) or fallback
    return fallback


def _load_recommendations(path: Path) -> dict:
    if not path.exists():
        return {"recommended_datasets": []}
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return yaml.safe_load(text) or {"recommended_datasets": []}


def _save_approvals(data: dict) -> None:
    approvals_path.parent.mkdir(parents=True, exist_ok=True)
    approvals_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


if st.button("Run Pipeline"):
    subprocess.run(["python", "-m", "src.run_pipeline", "--config", str(config_path), "--hitl-mode", "noninteractive_ui"], check=False)
    st.success("Pipeline command executed.")

report_path = Path("outputs/reports/report.md")
if report_path.exists():
    st.subheader("Report")
    st.markdown(report_path.read_text(encoding="utf-8"))

st.subheader("Dataset Recommendations & Approvals")
recommendations = _load_recommendations(recommendations_path)
approvals = _load_yaml(approvals_path, {"approved_datasets": []})
existing = {d.get("name"): d for d in approvals.get("approved_datasets", [])}

for ds in recommendations.get("recommended_datasets", []):
    name = ds.get("name")
    if not name:
        continue

    with st.container(border=True):
        st.markdown(f"**{name}**")
        st.write(ds.get("purpose"))
        st.write("Sources:", ds.get("suggested_sources", []))
        st.write("Join keys:", ds.get("join_keys", []))

        status = existing.get(name, {}).get("status", "pending_user_input")
        col1, col2, col3 = st.columns([1, 1, 2])
        if col1.button(f"Approve {name}", key=f"approve_{name}"):
            status = "approved"
        if col2.button(f"Reject {name}", key=f"reject_{name}"):
            status = "rejected_or_pending"

        local_file = col3.text_input(
            f"Local file mapping for {name}",
            value=existing.get(name, {}).get("local_file") or "",
            key=f"local_{name}",
            help="Absolute path or filename under data/raw/approved/",
        )
        allow_override = st.checkbox(
            f"Allow low match-rate override for {name}",
            value=bool(existing.get(name, {}).get("allow_low_match_override", False)),
            key=f"override_{name}",
        )

        existing[name] = {
            "name": name,
            "source_url": (ds.get("suggested_sources") or [None])[0],
            "join_keys": ds.get("join_keys", []),
            "status": status,
            "approved_at": datetime.now(timezone.utc).isoformat() if status == "approved" else None,
            "local_file": local_file or None,
            "allow_low_match_override": allow_override,
        }
        st.caption(f"Current status: {status}")

if st.button("Save approvals"):
    approvals["approved_datasets"] = list(existing.values())
    _save_approvals(approvals)
    st.success("Approvals saved to config/approvals.yaml")

st.subheader("Upload approved external dataset file")
uploaded = st.file_uploader("Upload file to data/raw/approved")
if uploaded:
    out = Path("data/raw/approved") / uploaded.name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(uploaded.read())
    st.success(f"Saved to {out}")

approved_names = [x.get("name") for x in existing.values() if x.get("status") == "approved"]
if approved_names:
    target = st.selectbox("Map uploaded filename to approved dataset", approved_names)
    mapped_file = st.text_input("Filename in data/raw/approved", value="")
    if st.button("Save dataset file mapping") and mapped_file:
        entry = existing[target]
        entry["local_file"] = mapped_file
        approvals["approved_datasets"] = list(existing.values())
        _save_approvals(approvals)
        st.success(f"Mapped {target} -> {mapped_file}")

st.subheader("Power BI Exports")
for p in sorted(Path("outputs/powerbi").glob("*.csv")):
    st.download_button(f"Download {p.name}", p.read_bytes(), file_name=p.name)
