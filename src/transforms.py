from __future__ import annotations

import numpy as np
import pandas as pd

from src.utils import EXPECTED_YEARS, pad_fips, safe_divide


def normalize_county_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["fips"] = out["GEO id2"].apply(pad_fips)
    out["county_name_norm"] = out["County"].astype(str).str.lower().str.replace(" county", "", regex=False).str.strip()
    out["year"] = out["Year"].astype(int)
    return out


def add_derived_metrics(df: pd.DataFrame, rounded_decimals: int = 4) -> pd.DataFrame:
    out = df.copy()
    hh = out["Households"].replace({0: np.nan})
    out["ALICE_pct"] = out["ALICE Households"] / hh
    out["Poverty_pct"] = out["Poverty Households"] / hh
    out["Above_ALICE_pct"] = out["Above ALICE Households"] / hh
    out["Gap_pct"] = out["ALICE_pct"] - out["Poverty_pct"]
    out["ALICE_to_Poverty_ratio"] = [safe_divide(a, p) for a, p in zip(out["ALICE Households"], out["Poverty Households"]) ]
    for col in ["ALICE_pct", "Poverty_pct", "Above_ALICE_pct", "Gap_pct"]:
        out[f"{col}_rounded"] = out[col].round(rounded_decimals)
    return out


def filter_five_year_window(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["year"].isin([2018, 2019, 2021, 2022, 2023])].copy()


def integrity_checks(df: pd.DataFrame, tolerance: float = 0.01) -> list[str]:
    warnings: list[str] = []
    if (df[["Households", "Poverty Households", "ALICE Households", "Above ALICE Households"]] < 0).any().any():
        warnings.append("Negative household counts detected")
    rel_err = (df["Households"] - (df["Poverty Households"] + df["ALICE Households"] + df["Above ALICE Households"])) / df["Households"].replace(0, np.nan)
    if rel_err.abs().fillna(0).gt(tolerance).any():
        warnings.append(f"Household composition check failed with tolerance {tolerance}")
    for col in ["ALICE_pct", "Poverty_pct", "Above_ALICE_pct"]:
        if df[col].dropna().lt(-tolerance).any() or df[col].dropna().gt(1 + tolerance).any():
            warnings.append(f"Percent bound check failed for {col}")
    years = set(df["year"].unique().tolist())
    if not years.issubset(EXPECTED_YEARS):
        warnings.append("Unexpected years outside expected set")
    if not pd.api.types.is_integer_dtype(df["GEO id2"]):
        warnings.append("GEO id2 is not integer dtype")
    return warnings
