from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler


def _select_external_features(df: pd.DataFrame, allowlist: list[str]) -> list[str]:
    ext_cols = [c for c in df.columns if c.startswith("ext_")]
    if not allowlist:
        return ext_cols
    selected: list[str] = []
    for c in ext_cols:
        if any(pat in c for pat in allowlist):
            selected.append(c)
    return selected


def _fit_models(X: pd.DataFrame, y: pd.Series, seed: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    x = X.fillna(X.median(numeric_only=True)).fillna(0.0)
    scaler = StandardScaler()
    z = scaler.fit_transform(x)

    ridge = Ridge(alpha=1.0, random_state=seed)
    ridge.fit(z, y)
    for feature, coef in zip(X.columns, ridge.coef_):
        rows.append(
            {
                "feature": feature,
                "method": "ridge",
                "coefficient_or_importance": float(abs(coef)),
                "direction": "positive" if coef > 0 else "negative" if coef < 0 else "neutral",
                "notes": "standardized ridge coefficient magnitude",
            }
        )

    rf = RandomForestRegressor(n_estimators=300, random_state=seed)
    rf.fit(x, y)
    perm = permutation_importance(rf, x, y, n_repeats=8, random_state=seed)
    for feature, imp in zip(X.columns, perm.importances_mean):
        rows.append(
            {
                "feature": feature,
                "method": "rf_permutation",
                "coefficient_or_importance": float(max(0.0, imp)),
                "direction": "positive_signal",
                "notes": "permutation importance (non-causal predictive signal)",
            }
        )
    return rows


def run_driver_analysis(enriched_df: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    warnings: list[str] = []
    if not bool(cfg.get("use_external_features_in_driver_analysis", False)):
        warnings.append("Driver analysis skipped: use_external_features_in_driver_analysis=false")
        return pd.DataFrame(), pd.DataFrame(), warnings

    allowlist = cfg.get("external_feature_allowlist", []) or []
    ext_features = _select_external_features(enriched_df, allowlist)
    if not ext_features:
        warnings.append("Driver analysis skipped: no allowed external enriched columns found")
        return pd.DataFrame(), pd.DataFrame(), warnings

    d2023 = enriched_df[enriched_df["year"] == 2023].copy()
    if d2023.empty:
        warnings.append("Driver analysis skipped: no 2023 records")
        return pd.DataFrame(), pd.DataFrame(), warnings

    rows_2023: list[dict[str, Any]] = []
    for target in ["ALICE_pct", "Poverty_pct"]:
        model_rows = _fit_models(d2023[ext_features], d2023[target].fillna(0.0), seed=int(cfg.get("seed", 42)))
        for r in model_rows:
            rows_2023.append({"target": target, **r})

    df_sorted = enriched_df.sort_values(["fips", "year"]).copy()
    df_sorted["delta_ALICE_pct"] = df_sorted.groupby("fips")["ALICE_pct"].diff()
    df_sorted["delta_Poverty_pct"] = df_sorted.groupby("fips")["Poverty_pct"].diff()
    d5 = df_sorted.dropna(subset=["delta_ALICE_pct", "delta_Poverty_pct"]).copy()

    rows_5yr: list[dict[str, Any]] = []
    if d5.empty:
        warnings.append("Driver analysis 5-year deltas skipped: insufficient sequential records")
    else:
        for target in ["delta_ALICE_pct", "delta_Poverty_pct"]:
            model_rows = _fit_models(d5[ext_features], d5[target].fillna(0.0), seed=int(cfg.get("seed", 42)))
            for r in model_rows:
                rows_5yr.append({"target": target, **r})

    out_2023 = pd.DataFrame(rows_2023).sort_values(["target", "method", "coefficient_or_importance"], ascending=[True, True, False])
    out_5yr = pd.DataFrame(rows_5yr).sort_values(["target", "method", "coefficient_or_importance"], ascending=[True, True, False]) if rows_5yr else pd.DataFrame()
    return out_2023, out_5yr, warnings
