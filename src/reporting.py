from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _top_bottom_text(snapshot: pd.DataFrame, metric: str, n: int) -> str:
    top = snapshot.nlargest(n, metric)[["County", metric]]
    bottom = snapshot.nsmallest(n, metric)[["County", metric]]
    return (
        f"Top {n} by {metric}: "
        + ", ".join([f"{r.County} ({r[metric]:.2%})" for _, r in top.iterrows()])
        + "\n"
        + f"Bottom {n} by {metric}: "
        + ", ".join([f"{r.County} ({r[metric]:.2%})" for _, r in bottom.iterrows()])
    )


def write_reports(
    report_dir: str,
    snapshot_2023: pd.DataFrame,
    trends: pd.DataFrame,
    bulloch_peers: pd.DataFrame,
    bulloch_neighbors_df: pd.DataFrame | None,
    anomalies: pd.DataFrame,
    checkpoints_df: pd.DataFrame,
    quality_warnings: list[str],
    research: dict[str, Any],
    cluster_meta: dict[str, Any],
    top_n: int,
) -> None:
    Path(report_dir).mkdir(parents=True, exist_ok=True)

    bulloch = snapshot_2023[snapshot_2023["County"].str.lower() == "bulloch"]
    bulloch_rank_alice = int(bulloch["rank_ALICE_pct"].iloc[0]) if not bulloch.empty else None
    bulloch_rank_pov = int(bulloch["rank_Poverty_pct"].iloc[0]) if not bulloch.empty else None

    lines = [
        "# INFORMS ALICE Analytics Agent Report",
        "",
        "## 1) 2023 Snapshot",
        _top_bottom_text(snapshot_2023, "ALICE_pct", top_n),
        "",
        _top_bottom_text(snapshot_2023, "Poverty_pct", top_n),
        f"Bulloch rank by ALICE_pct: {bulloch_rank_alice}; by Poverty_pct: {bulloch_rank_pov}.",
        "Prioritization columns are in `snapshot_2023.csv` (z-score, weighted, Pareto frontier).",
        "",
        "## 2) 5-Year Perspective (2018, 2019, 2021-2023)",
        "County trend metrics are in `trends_5yr.csv` and rank movement in `rank_changes.csv`.",
        "Bulloch peers comparison is in `bulloch_vs_peers.csv`.",
    ]

    if bulloch_neighbors_df is not None and not bulloch_neighbors_df.empty:
        lines.append("Bulloch neighbors comparison available in `bulloch_vs_neighbors.csv`.")
    else:
        lines.append("Neighbor adjacency not provided/approved; using peers instead of geographic neighbors.")

    lines.extend(
        [
            "",
            "## 3) Clusters / Segments",
            f"Chosen clustering: {cluster_meta.get('algo')} with k={cluster_meta.get('k')} "
            f"(silhouette={cluster_meta.get('silhouette'):.3f}, DB={cluster_meta.get('db'):.3f}).",
            "Cluster assignments and PCA coordinates are in `clusters_2023.csv`.",
            "",
            "## 4) Anomalies",
            f"Flagged anomaly count: {len(anomalies)} (see `anomalies.csv`).",
            "",
            "## 5) Data Lineage & Join Quality",
            "See `join_audit.csv` for dataset-by-dataset join coverage, match rates, and blocked reasons.",
            "See `provenance.csv` for approval status and join outcomes used in this run.",
            "",
            "## 6) Recommended Additional Datasets (HITL Approval Required)",
        ]
    )

    if research.get("recommended_datasets"):
        for ds in research["recommended_datasets"]:
            lines.append(f"- **{ds.get('name')}**: {ds.get('purpose')} | join keys: {', '.join(ds.get('join_keys', []))}")
            lines.append(f"  Sources: {', '.join(ds.get('suggested_sources', []))}")
    else:
        lines.append("No researched datasets returned. Configure OPENAI_API_KEY and rerun to enable agentic web research.")

    lines.extend(["", "## 7) Rules / Checkpoints Extracted from PDF", "Parsed checkpoint rows are in `checkpoints_from_pdf.csv`."])
    for _, row in checkpoints_df.head(12).iterrows():
        lines.append(f"- {row.get('date')}: {row.get('checkpoint_name')} (p.{row.get('page_number')}) — {row.get('description')}")

    lines.extend(
        [
            "",
            "## Data Quality",
            *([f"- {w}" for w in quality_warnings] if quality_warnings else ["- No critical integrity issues detected under configured tolerance."]),
            "- Meta sheet note: percentage calculations may differ by +/-1% due to rounding.",
            "",
            "## Limitations",
            "- External drivers are not merged unless explicitly approved and ingested.",
            "- Geographic neighbors require user-provided neighbor file or approved adjacency dataset.",
            "- Driver analysis outputs (if present) are associative/predictive only and not causal evidence.",
        ]
    )

    (Path(report_dir) / "report.md").write_text("\n".join(lines), encoding="utf-8")

    exec_summary = [
        "# Executive Summary",
        "- 2023 county ranking/prioritization outputs are Power BI ready (`snapshot_2023.csv`).",
        "- Bulloch five-year trend metrics and rank movement are generated for 2018, 2019, 2021-2023.",
        "- Peers are provided when adjacency is unavailable (`bulloch_vs_peers.csv`).",
        "- Clustering + PCA and anomaly tables provide segmentation and outlier triage for investigation.",
        "- Any external dataset use is blocked behind explicit approval in `config/approvals.yaml`.",
    ]
    (Path(report_dir) / "executive_summary.md").write_text("\n".join(exec_summary), encoding="utf-8")

    methods = [
        "# Methods Appendix",
        "- Smoothing: rolling mean window=3 and LOWESS for irregular year spacing.",
        "- Trend stats: level, delta from previous available observation, slope, volatility.",
        "- PCA on standardized 2023 feature matrix; clustering model selection via silhouette (tie-break Davies-Bouldin).",
        f"- Selected model: {cluster_meta}.",
        "- Anomaly detection: IsolationForest over deltas + gap with deterministic seed.",
        "- Integrity checks: non-negativity, composition tolerance, bounded percentages, expected years, FIPS coercion.",
    ]
    (Path(report_dir) / "methods_appendix.md").write_text("\n".join(methods), encoding="utf-8")
