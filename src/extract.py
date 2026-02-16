from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from pypdf import PdfReader

from src.utils import COUNTY_COLUMNS, EXPECTED_YEARS


DATE_PATTERN = re.compile(
    r"\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)


def load_excel_dataset(path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    xls = pd.ExcelFile(path)
    meta = pd.read_excel(xls, sheet_name="Meta")
    county = pd.read_excel(xls, sheet_name="County")
    missing = [c for c in COUNTY_COLUMNS if c not in county.columns]
    if missing:
        raise ValueError(f"County sheet missing expected columns: {missing}")
    years = set(pd.Series(county["Year"]).dropna().astype(int).unique().tolist())
    if years != EXPECTED_YEARS:
        raise ValueError(f"Unexpected year set. got={sorted(years)} expected={sorted(EXPECTED_YEARS)}")
    return meta, county


def extract_pdf_text(path: str | Path) -> str:
    reader = PdfReader(str(path))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    return "\n".join(chunks).strip()


def extract_checkpoints(text: str, context_window: int = 1) -> pd.DataFrame:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    rows: list[dict[str, str]] = []
    for i, line in enumerate(lines):
        dates = DATE_PATTERN.findall(line)
        if not dates:
            continue
        start = max(0, i - context_window)
        end = min(len(lines), i + context_window + 1)
        block = " ".join(lines[start:end])
        rows.append(
            {
                "line_index": i,
                "dates_found": " | ".join(dates),
                "raw_line": line,
                "context_block": block,
            }
        )
    return pd.DataFrame(rows)
