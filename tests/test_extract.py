from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("openpyxl")

from src.extract import load_excel_dataset
from src.utils import COUNTY_COLUMNS


def _make_excel(tmp_path: Path, years: list[int]) -> Path:
    meta = pd.DataFrame({"note": ["meta"]})
    rows = []
    for y in years:
        row = {c: 0 for c in COUNTY_COLUMNS}
        row.update(
            {
                "State": "Georgia",
                "Year": y,
                "GEO id2": 13001,
                "GEO display_label": "Bulloch",
                "County": "Bulloch",
                "State Abbr": "GA",
                "Households": 100,
                "Poverty Households": 20,
                "ALICE Households": 30,
                "Above ALICE Households": 50,
                "ALICE Threshold - HH under 65": 30000,
                "ALICE Threshold - HH 65 years and over": 25000,
                "Source: American Community Survey": "ACS",
            }
        )
        rows.append(row)

    county = pd.DataFrame(rows)
    path = tmp_path / "in.xlsx"
    with pd.ExcelWriter(path) as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        county.to_excel(writer, sheet_name="County", index=False)
    return path


def test_year_check_allows_extra_years(tmp_path: Path):
    years = [2010, 2012, 2014, 2016, 2018, 2019, 2021, 2022, 2023, 2024]
    path = _make_excel(tmp_path, years)
    _, _, warnings = load_excel_dataset(path)
    assert any("Extra years detected" in w for w in warnings)


def test_year_check_fails_when_required_missing(tmp_path: Path):
    years = [2010, 2012, 2014, 2016, 2018, 2019, 2021, 2022]
    path = _make_excel(tmp_path, years)
    with pytest.raises(ValueError):
        load_excel_dataset(path)
