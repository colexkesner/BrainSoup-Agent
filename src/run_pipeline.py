from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from src.analysis import build_peers, build_rank_changes, compute_trends, find_anomalies, pca_and_cluster, snapshot_2023_rankings
from src.approval import approval_gate, load_neighbors
from src.extract import extract_checkpoints, extract_pdf_text, load_excel_dataset
from src.reporting import write_reports
from src.research_agent import run_research_agent
from src.transforms import add_derived_metrics, filter_five_year_window, integrity_checks, normalize_county_keys
from src.utils import PipelineConfig, append_jsonl, load_yaml, now_iso, save_yaml


def _ensure_dirs(cfg: dict) -> None:
    for p in [cfg["output_dirs"]["powerbi"], cfg["output_dirs"]["reports"], cfg["output_dirs"]["logs"], cfg["cache_dir"]]:
        Path(p).mkdir(parents=True, exist_ok=True)


def _build_provenance(approvals: dict, excel_path: str, pdf_path: str) -> pd.DataFrame:
    rows = [
        {"name": "INFORMS Excel", "source": excel_path, "approval_status": "provided", "join_keys": "fips/year", "date_added": now_iso()},
        {"name": "INFORMS PDF", "source": pdf_path, "approval_status": "provided", "join_keys": "n/a", "date_added": now_iso()},
    ]
    for x in approvals.get("approved_datasets", []):
        rows.append(
            {
                "name": x.get("name"),
                "source": x.get("source_url"),
                "approval_status": x.get("status"),
                "join_keys": ",".join(x.get("join_keys", [])),
                "date_added": x.get("approved_at"),
            }
        )
    return pd.DataFrame(rows)


def _neighbors_comparison(df_5yr: pd.DataFrame, neighbors: list[str]) -> pd.DataFrame:
    if not neighbors:
        return pd.DataFrame()
    counties = ["Bulloch", *neighbors]
    return df_5yr[df_5yr["County"].isin(counties)].copy()


def run_pipeline(config_path: str) -> None:
    cfg_raw = load_yaml(config_path)
    cfg = PipelineConfig(cfg_raw).raw
    np.random.seed(int(cfg.get("seed", 42)))
    _ensure_dirs(cfg)
    run_log = Path(cfg["output_dirs"]["logs"]) / "run_log.jsonl"

    append_jsonl(run_log, {"ts": now_iso(), "event": "pipeline_start"})
    excel_path = cfg["input_paths"]["excel"]
    pdf_path = cfg["input_paths"]["pdf"]

    meta_df, county_df = load_excel_dataset(excel_path)
    pdf_text = extract_pdf_text(pdf_path)
    if len(pdf_text) < int(cfg.get("pdf_text_min_chars", 800)):
        append_jsonl(run_log, {"ts": now_iso(), "event": "pdf_low_text_warning", "chars": len(pdf_text), "action": "request OCR approval"})

    df = normalize_county_keys(county_df)
    df = add_derived_metrics(df, rounded_decimals=int(cfg.get("percent_round_decimals", 4)))
    quality_warnings = integrity_checks(df, tolerance=float(cfg.get("relative_error_tolerance", 0.01)))
    for w in quality_warnings:
        append_jsonl(run_log, {"ts": now_iso(), "event": "integrity_warning", "message": w})

    snapshot_2023 = snapshot_2023_rankings(df, cfg.get("weights", {}), top_n=int(cfg.get("top_n", 10)))
    trends_window = filter_five_year_window(df)
    trends = compute_trends(trends_window)
    rank_changes = build_rank_changes(trends_window)
    clusters_2023, pca_loadings, cluster_meta = pca_and_cluster(snapshot_2023, seed=int(cfg["seed"]), pca_components=int(cfg.get("pca_components", 3)))
    anomalies = find_anomalies(df, seed=int(cfg["seed"]))
    peers = build_peers(snapshot_2023.reset_index(drop=True), clusters_2023.reset_index(drop=True), target_county="Bulloch", n_neighbors=10)

    neighbors = load_neighbors(cfg["input_paths"].get("neighbors_yaml", "config/neighbors.yaml"))
    bulloch_vs_neighbors = _neighbors_comparison(trends_window, neighbors)

    checkpoints = extract_checkpoints(pdf_text)

    summary_payload = {
        "top_alice": snapshot_2023.nlargest(5, "ALICE_pct")[["County", "ALICE_pct"]].to_dict("records"),
        "top_poverty": snapshot_2023.nlargest(5, "Poverty_pct")[["County", "Poverty_pct"]].to_dict("records"),
        "bulloch_trend": trends[trends["County"].str.lower() == "bulloch"].to_dict("records"),
        "cluster_meta": cluster_meta,
        "anomalies": anomalies.head(10).to_dict("records"),
    }
    research = run_research_agent(cfg, pdf_text, summary_payload, str(run_log))

    approvals = approval_gate(research, "config/approvals.yaml", non_interactive_default_reject=True)

    provenance = _build_provenance(approvals, excel_path, pdf_path)

    out_powerbi = Path(cfg["output_dirs"]["powerbi"])
    df.to_csv(out_powerbi / "fact_county_year.csv", index=False)
    snapshot_2023.to_csv(out_powerbi / "snapshot_2023.csv", index=False)
    trends_window.to_csv(out_powerbi / "trends_5yr.csv", index=False)
    if not bulloch_vs_neighbors.empty:
        bulloch_vs_neighbors.to_csv(out_powerbi / "bulloch_vs_neighbors.csv", index=False)
    peers.to_csv(out_powerbi / "bulloch_vs_peers.csv", index=False)
    clusters_2023.to_csv(out_powerbi / "clusters_2023.csv", index=False)
    anomalies.to_csv(out_powerbi / "anomalies.csv", index=False)
    rank_changes.to_csv(out_powerbi / "rank_changes.csv", index=False)
    checkpoints.to_csv(out_powerbi / "checkpoints_from_pdf.csv", index=False)
    provenance.to_csv(out_powerbi / "provenance.csv", index=False)
    pca_loadings.to_csv(out_powerbi / "pca_loadings.csv", index=False)

    write_reports(
        report_dir=cfg["output_dirs"]["reports"],
        snapshot_2023=snapshot_2023,
        trends=trends,
        bulloch_peers=peers,
        bulloch_neighbors_df=bulloch_vs_neighbors if not bulloch_vs_neighbors.empty else None,
        anomalies=anomalies,
        checkpoints_df=checkpoints,
        quality_warnings=quality_warnings,
        research=research,
        cluster_meta=cluster_meta,
        top_n=int(cfg.get("top_n", 10)),
    )

    if cfg.get("enable_hosted_container_tools"):
        append_jsonl(run_log, {"ts": now_iso(), "event": "hosted_container_tools_enabled_warning", "message": "Sessions may expire; log IDs externally."})

    append_jsonl(run_log, {"ts": now_iso(), "event": "pipeline_complete"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run_pipeline(args.config)
