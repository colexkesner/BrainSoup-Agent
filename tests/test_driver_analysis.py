import pytest
pd = pytest.importorskip("pandas")

from src.driver_analysis import run_driver_analysis


def test_driver_analysis_runs_only_when_toggled_and_external_cols_exist():
    df = pd.DataFrame(
        {
            "fips": ["13001", "13003", "13001", "13003"],
            "year": [2022, 2022, 2023, 2023],
            "ALICE_pct": [0.30, 0.40, 0.31, 0.42],
            "Poverty_pct": [0.20, 0.30, 0.21, 0.29],
            "ext_ds__feature_a": [1.0, 2.0, 1.5, 2.5],
        }
    )

    off_2023, off_5yr, off_warnings = run_driver_analysis(df, {"use_external_features_in_driver_analysis": False})
    assert off_2023.empty and off_5yr.empty
    assert any("skipped" in w.lower() for w in off_warnings)

    on_2023, on_5yr, _ = run_driver_analysis(
        df,
        {
            "use_external_features_in_driver_analysis": True,
            "external_feature_allowlist": ["feature_a"],
            "seed": 42,
        },
    )
    assert not on_2023.empty
    assert set(["target", "feature", "coefficient_or_importance", "direction", "notes"]).issubset(set(on_2023.columns))
