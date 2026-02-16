from __future__ import annotations

from pathlib import Path
import subprocess

import streamlit as st
import yaml

st.title("INFORMS ALICE Analytics Agent")

config_path = Path("config/config.yaml")
approvals_path = Path("config/approvals.yaml")
recommendations_path = Path("outputs/logs/research_recommendations.json")

if st.button("Run Pipeline"):
    subprocess.run(["python", "-m", "src.run_pipeline", "--config", str(config_path)], check=False)
    st.success("Pipeline command executed.")

report_path = Path("outputs/reports/report.md")
if report_path.exists():
    st.subheader("Report")
    st.markdown(report_path.read_text(encoding="utf-8"))

st.subheader("Dataset Recommendations & Approvals")
recommendations = {"recommended_datasets": []}
if recommendations_path.exists():
    recommendations = yaml.safe_load(recommendations_path.read_text(encoding="utf-8")) or recommendations

approvals = {"approved_datasets": []}
if approvals_path.exists():
    approvals = yaml.safe_load(approvals_path.read_text(encoding="utf-8")) or approvals

existing = {d.get("name"): d for d in approvals.get("approved_datasets", [])}
new_approval_rows = []

for ds in recommendations.get("recommended_datasets", []):
    name = ds.get("name")
    with st.expander(name or "Unnamed dataset", expanded=False):
        st.write(ds.get("purpose"))
        st.write("Sources:", ds.get("suggested_sources", []))
        current = existing.get(name, {})
        status_default = current.get("status", "pending_user_input")
        status = st.selectbox(
            f"Approval status for {name}",
            options=["approved", "rejected_or_pending", "pending_user_input"],
            index=["approved", "rejected_or_pending", "pending_user_input"].index(status_default) if status_default in ["approved", "rejected_or_pending", "pending_user_input"] else 2,
            key=f"status_{name}",
        )
        local_file = st.text_input(
            f"Mapped local file path (relative to data/raw/approved or absolute) for {name}",
            value=current.get("local_file") or "",
            key=f"file_{name}",
        )
        allow_override = st.checkbox(
            f"Allow low match-rate override for {name}",
            value=bool(current.get("allow_low_match_override", False)),
            key=f"override_{name}",
        )
        new_approval_rows.append(
            {
                "name": name,
                "source_url": (ds.get("suggested_sources") or [None])[0],
                "join_keys": ds.get("join_keys", []),
                "status": status,
                "approved_at": current.get("approved_at"),
                "local_file": local_file or None,
                "allow_low_match_override": allow_override,
            }
        )

if st.button("Save approvals"):
    approvals["approved_datasets"] = new_approval_rows
    approvals_path.parent.mkdir(parents=True, exist_ok=True)
    approvals_path.write_text(yaml.safe_dump(approvals, sort_keys=False), encoding="utf-8")
    st.success("Approvals updated.")

st.subheader("Upload approved external dataset file")
uploaded = st.file_uploader("Upload file to data/raw/approved")
if uploaded:
    out = Path("data/raw/approved") / uploaded.name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(uploaded.read())
    st.success(f"Saved to {out}")

st.subheader("Power BI Exports")
for p in sorted(Path("outputs/powerbi").glob("*.csv")):
    st.download_button(f"Download {p.name}", p.read_bytes(), file_name=p.name)
