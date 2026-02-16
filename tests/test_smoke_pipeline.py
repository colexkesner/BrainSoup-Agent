from pathlib import Path

import pytest
pd = pytest.importorskip("pandas")
pytest.importorskip("openpyxl")
from pypdf import PdfWriter

from src.run_pipeline import run_pipeline
from src.utils import save_yaml


def _make_input_files(tmp_path: Path) -> tuple[Path, Path]:
    meta = pd.DataFrame({"note": ["Rounding may differ by +/- 1%; use GEO id2 with matching-year shapefiles."]})
    rows = []
    years = [2010, 2012, 2014, 2016, 2018, 2019, 2021, 2022, 2023]
    for i in range(1, 160):
        for y in years:
            rows.append(
                {
                    "State": "Georgia",
                    "Year": y,
                    "GEO id2": 13000 + i,
                    "GEO display_label": f"County {i}",
                    "County": "Bulloch" if i == 1 else f"County {i}",
                    "State Abbr": "GA",
                    "Households": 1000 + i,
                    "Poverty Households": 200 + (i % 30),
                    "ALICE Households": 300 + (i % 40),
                    "Above ALICE Households": 500 + (i % 50),
                    "ALICE Threshold - HH under 65": 31000,
                    "ALICE Threshold - HH 65 years and over": 26000,
                    "Source: American Community Survey": "ACS",
                }
            )
    county = pd.DataFrame(rows)
    excel_path = tmp_path / "input.xlsx"
    with pd.ExcelWriter(excel_path) as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        county.to_excel(writer, sheet_name="County", index=False)

    pdf_path = tmp_path / "instructions.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    with open(pdf_path, "wb") as f:
        writer.write(f)
    return excel_path, pdf_path


def test_smoke_pipeline(tmp_path: Path):
    excel, pdf = _make_input_files(tmp_path)
    cfg = {
        "seed": 42,
        "provider": "openai",
        "web_search_tool_type": "web_search",
        "model": "gpt-4.1-mini",
        "max_retries": 1,
        "cache_dir": str(tmp_path / "logs" / "cache"),
        "weights": {"alice_pct": 0.35, "poverty_pct": 0.35, "log_alice_households": 0.15, "log_poverty_households": 0.15},
        "output_dirs": {"powerbi": str(tmp_path / "powerbi"), "reports": str(tmp_path / "reports"), "logs": str(tmp_path / "logs")},
        "input_paths": {"excel": str(excel), "pdf": str(pdf), "neighbors_yaml": str(tmp_path / "neighbors.yaml")},
        "hitl_mode": "auto_reject",
        "approvals_path": str(tmp_path / "approvals.yaml"),
    }
    (tmp_path / "neighbors.yaml").write_text("bulloch_neighbors: []\n", encoding="utf-8")
    (tmp_path / "approvals.yaml").write_text("approved_datasets: []\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    save_yaml(config_path, cfg)

    run_pipeline(str(config_path))

    assert (tmp_path / "powerbi" / "fact_county_year.csv").exists()
    assert (tmp_path / "powerbi" / "snapshot_2023.csv").exists()
    assert (tmp_path / "powerbi" / "fact_enriched.csv").exists()
    assert (tmp_path / "reports" / "report.md").exists()
