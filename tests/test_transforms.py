import pytest
pd = pytest.importorskip("pandas")

from src.transforms import add_derived_metrics, filter_five_year_window, integrity_checks, normalize_county_keys
from src.utils import pad_fips, safe_divide


def test_safe_divide_and_percents():
    df = pd.DataFrame(
        {
            "State": ["Georgia"],
            "Year": [2023],
            "GEO id2": [13031],
            "GEO display_label": ["Bulloch County"],
            "County": ["Bulloch"],
            "State Abbr": ["GA"],
            "Households": [100],
            "Poverty Households": [20],
            "ALICE Households": [30],
            "Above ALICE Households": [50],
            "ALICE Threshold - HH under 65": [30000],
            "ALICE Threshold - HH 65 years and over": [25000],
            "Source: American Community Survey": ["ACS"],
        }
    )
    out = add_derived_metrics(normalize_county_keys(df))
    assert out.loc[0, "ALICE_pct"] == 0.3
    assert out.loc[0, "Poverty_pct"] == 0.2
    assert safe_divide(1, 0) == 0.0


def test_pad_fips():
    assert pad_fips(1) == "00001"
    assert pad_fips(13031) == "13031"


def test_filter_five_year_window():
    df = pd.DataFrame({"year": [2010, 2018, 2019, 2020, 2021, 2022, 2023]})
    out = filter_five_year_window(df)
    assert out["year"].tolist() == [2018, 2019, 2021, 2022, 2023]


def test_integrity_tolerance_behavior():
    df = pd.DataFrame(
        {
            "Households": [100],
            "Poverty Households": [34],
            "ALICE Households": [34],
            "Above ALICE Households": [34],
            "ALICE_pct": [0.34],
            "Poverty_pct": [0.34],
            "Above_ALICE_pct": [0.34],
            "year": [2023],
            "GEO id2": pd.Series([13031], dtype="int64"),
        }
    )
    # 2% mismatch should fail at 1% tolerance
    warnings = integrity_checks(df, tolerance=0.01)
    assert any("composition" in w.lower() for w in warnings)
