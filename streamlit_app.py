from pathlib import Path
import subprocess

import streamlit as st
import yaml

st.title("INFORMS ALICE Analytics Agent")

if st.button("Run Pipeline"):
    subprocess.run(["python", "-m", "src.run_pipeline", "--config", "config/config.yaml"], check=False)
    st.success("Pipeline command executed.")

report_path = Path("outputs/reports/report.md")
if report_path.exists():
    st.subheader("Report")
    st.markdown(report_path.read_text(encoding="utf-8"))

st.subheader("Dataset Recommendations Approval")
approvals_path = Path("config/approvals.yaml")
if approvals_path.exists():
    approvals = yaml.safe_load(approvals_path.read_text(encoding="utf-8")) or {}
    st.write(approvals)

uploaded = st.file_uploader("Upload approved external dataset")
if uploaded:
    out = Path("data/raw/approved") / uploaded.name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(uploaded.read())
    st.success(f"Saved to {out}")

st.subheader("Power BI Exports")
for p in sorted(Path("outputs/powerbi").glob("*.csv")):
    st.download_button(f"Download {p.name}", p.read_bytes(), file_name=p.name)
