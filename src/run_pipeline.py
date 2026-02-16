from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.analysis import build_peers, build_rank_changes, compute_trends, find_anomalies, pca_and_cluster, snapshot_2023_rankings
from src.approval import VALID_HITL_MODES, approval_gate, load_neighbors
from src.driver_analysis import run_driver_analysis
from src.extract import extract_checkpoints, extract_pdf_text_with_pages, load_excel_dataset
from src.ingest_external import ingest_approved_datasets
from src.reporting import write_reports
from src.research_agent import run_research_agent
from src.transforms import add_derived_metrics, filter_five_year_window, integrity_checks, normalize_county_keys
from src.utils import PipelineConfig, append_jsonl, load_yaml, now_iso


def _ensure_dirs(cfg: dict) -> None:
    for p in [cfg["output_dirs"]["powerbi"], cfg["output_dirs"]["reports"], cfg["output_dirs"]["logs"], cfg["cache_dir"], cfg.get("approved_data_dir", "data/raw/approved")]:
        Path(p).mkdir(parents=True, exist_ok=True)


def _build_base_provenance(excel_path: str, pdf_path: str) -> list[dict[str, str]]:
    return [
        {"name": "INFORMS Excel", "source": excel_path, "approval_status": "provided", "join_keys": "fips/year", "date_added": now_iso(), "join_outcome": "provided"},
        {"name": "INFORMS PDF", "source": pdf_path, "approval_status": "provided", "join_keys": "n/a", "date_added": now_iso(), "join_outcome": "provided"},
    ]


def _neighbors_comparison(df_5yr: pd.DataFrame, neighbors: list[str]) -> pd.DataFrame:
    if not neighbors:
        return pd.DataFrame()
    counties = ["Bulloch", *neighbors]
    return df_5yr[df_5yr["County"].isin(counties)].copy()


def _hitl_behavior(hitl_mode: str) -> tuple[bool, bool]:
    if hitl_mode == "interactive":
        return False, True
    if hitl_mode == "auto_reject":
        return True, False
    if hitl_mode == "noninteractive_ui":
        return False, False
    raise ValueError(f"Unsupported hitl_mode={hitl_mode}")


def run_pipeline(config_path: str, hitl_mode_override: str | None = None) -> None:
    cfg_raw = load_yaml(config_path)
    cfg = PipelineConfig(cfg_raw).raw
    np.random.seed(int(cfg.get("seed", 42)))
    _ensure_dirs(cfg)
    run_log = Path(cfg["output_dirs"]["logs"]) / "run_log.jsonl"

    append_jsonl(run_log, {"ts": now_iso(), "event": "pipeline_start"})
    excel_path = cfg["input_paths"]["excel"]
    pdf_path = cfg["input_paths"]["pdf"]

    _, county_df, excel_warnings = load_excel_dataset(excel_path)
    for w in excel_warnings:
        append_jsonl(run_log, {"ts": now_iso(), "event": "excel_warning", "message": w})

    pdf_text, pdf_pages = extract_pdf_text_with_pages(pdf_path)
    pdf_limitations: list[str] = []
    if len(pdf_text) < int(cfg.get("pdf_text_min_chars", 800)):
        msg = "PDF extraction limited; consider OCR (approval required)."
        pdf_limitations.append(msg)
        append_jsonl(
            run_log,
            {
                "ts": now_iso(),
                "event": "pdf_low_text_warning",
                "chars": len(pdf_text),
                "action": "request OCR approval",
                "message": msg,
            },
        )

    df = normalize_county_keys(county_df)
    df = add_derived_metrics(df, rounded_decimals=int(cfg.get("percent_round_decimals", 4)))
    quality_warnings = integrity_checks(df, tolerance=float(cfg.get("relative_error_tolerance", 0.01))) + excel_warnings + pdf_limitations
    for w in quality_warnings:
        append_jsonl(run_log, {"ts": now_iso(), "event": "integrity_warning", "message": w})

    summary_payload = {
        "top_alice": df[df["year"] == 2023].nlargest(5, "ALICE_pct")[ ["County", "ALICE_pct"] ].to_dict("records"),
        "top_poverty": df[df["year"] == 2023].nlargest(5, "Poverty_pct")[ ["County", "Poverty_pct"] ].to_dict("records"),
    }
    research = run_research_agent(cfg, pdf_text, summary_payload, str(run_log))
    (Path(cfg["output_dirs"]["logs"]) / "research_recommendations.json").write_text(json.dumps(research, indent=2), encoding="utf-8")

    hitl_mode = hitl_mode_override or cfg.get("hitl_mode", "auto_reject")
    if hitl_mode not in VALID_HITL_MODES:
        raise ValueError(f"Unsupported hitl_mode={hitl_mode}; expected one of {sorted(VALID_HITL_MODES)}")

    non_interactive_default_reject, terminal_prompt_enabled = _hitl_behavior(hitl_mode)
    append_jsonl(
        run_log,
        {
            "ts": now_iso(),
            "event": "hitl_mode_selected",
            "hitl_mode": hitl_mode,
            "non_interactive_default_reject": non_interactive_default_reject,
            "terminal_prompt_enabled": terminal_prompt_enabled,
        },
    )

    approvals_path = cfg.get("approvals_path", "config/approvals.yaml")
    approvals = approval_gate(research, approvals_path, hitl_mode=hitl_mode)

    enriched_df, join_audit_records, join_audit_df = ingest_approved_datasets(df, approvals, cfg, str(run_log))
    ingestion_warnings = [
        f"External dataset '{r.get('dataset_name')}' excluded: {r.get('blocked_reason')}"
        for r in join_audit_records
        if r.get("blocked_reason")
    ]
    quality_warnings.extend(ingestion_warnings)

    ext_cols = [c for c in enriched_df.columns if c.startswith("ext_")]
    append_jsonl(run_log, {"ts": now_iso(), "event": "enriched_features_detected", "count": len(ext_cols), "columns": ext_cols[:20]})

    snapshot_2023 = snapshot_2023_rankings(enriched_df, cfg.get("weights", {}), top_n=int(cfg.get("top_n", 10)))
    trends_window = filter_five_year_window(enriched_df)
    trends = compute_trends(trends_window)
    rank_changes = build_rank_changes(trends_window)
    clusters_2023, pca_loadings, cluster_meta = pca_and_cluster(snapshot_2023, seed=int(cfg["seed"]), pca_components=int(cfg.get("pca_components", 3)))
    anomalies = find_anomalies(enriched_df, seed=int(cfg["seed"]))
    peers = build_peers(snapshot_2023.reset_index(drop=True), clusters_2023.reset_index(drop=True), target_county="Bulloch", n_neighbors=10)

    drivers_2023, drivers_5yr, driver_warnings = run_driver_analysis(enriched_df, cfg)
    quality_warnings.extend(driver_warnings)

    neighbors = load_neighbors(cfg["input_paths"].get("neighbors_yaml", "config/neighbors.yaml"))
    bulloch_vs_neighbors = _neighbors_comparison(trends_window, neighbors)
    checkpoints = extract_checkpoints(pdf_pages)

    base_prov = pd.DataFrame(_build_base_provenance(excel_path, pdf_path))
    approvals_prov = pd.DataFrame(approvals.get("approved_datasets", []))
    if not approvals_prov.empty:
        approvals_prov = approvals_prov.rename(columns={"source_url": "source", "approved_at": "date_added", "status": "approval_status"})
    provenance = pd.concat([base_prov, approvals_prov, join_audit_df], ignore_index=True, sort=False)

    out_powerbi = Path(cfg["output_dirs"]["powerbi"])
    enriched_df.to_csv(out_powerbi / "fact_county_year.csv", index=False)
    enriched_df.to_csv(out_powerbi / "fact_enriched.csv", index=False)
    snapshot_2023.to_csv(out_powerbi / "snapshot_2023.csv", index=False)
    trends_window.to_csv(out_powerbi / "trends_5yr.csv", index=False)
    if not bulloch_vs_neighbors.empty:
        bulloch_vs_neighbors.to_csv(out_powerbi / "bulloch_vs_neighbors.csv", index=False)
    peers.to_csv(out_powerbi / "bulloch_vs_peers.csv", index=False)
    clusters_2023.to_csv(out_powerbi / "clusters_2023.csv", index=False)
    anomalies.to_csv(out_powerbi / "anomalies.csv", index=False)
    rank_changes.to_csv(out_powerbi / "rank_changes.csv", index=False)
    checkpoints.to_csv(out_powerbi / "checkpoints_from_pdf.csv", index=False)
    join_audit_df.to_csv(out_powerbi / "join_audit.csv", index=False)
    provenance.to_csv(out_powerbi / "provenance.csv", index=False)
    pca_loadings.to_csv(out_powerbi / "pca_loadings.csv", index=False)
    if not drivers_2023.empty:
        drivers_2023.to_csv(out_powerbi / "drivers_2023.csv", index=False)
    if not drivers_5yr.empty:
        drivers_5yr.to_csv(out_powerbi / "drivers_5yr.csv", index=False)

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
    parser.add_argument("--hitl-mode", choices=["interactive", "auto_reject", "noninteractive_ui"], default=None)
    args = parser.parse_args()
    run_pipeline(args.config, hitl_mode_override=args.hitl_mode)
