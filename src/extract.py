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
def load_excel_dataset(path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    xls = pd.ExcelFile(path)
    meta = pd.read_excel(xls, sheet_name="Meta")
    county = pd.read_excel(xls, sheet_name="County")
    missing = [c for c in COUNTY_COLUMNS if c not in county.columns]
    if missing:
        raise ValueError(f"County sheet missing expected columns: {missing}")

    years = set(pd.Series(county["Year"]).dropna().astype(int).unique().tolist())
    missing_required = sorted(EXPECTED_YEARS - years)
    if missing_required:
        raise ValueError(
            f"Missing required analysis years: {missing_required}. "
            f"Required subset is {sorted(EXPECTED_YEARS)}; available years={sorted(years)}"
        )

    warnings: list[str] = []
    extra_years = sorted(years - EXPECTED_YEARS)
    if extra_years:
        warnings.append(f"Data Coverage: extra years detected beyond required set: {extra_years}")

    return meta, county, warnings


def extract_pdf_text_with_pages(path: str | Path) -> tuple[str, list[dict[str, str | int]]]:
    reader = PdfReader(str(path))
    page_chunks: list[dict[str, str | int]] = []
    for idx, page in enumerate(reader.pages, start=1):
        page_text = (page.extract_text() or "").strip()
        page_chunks.append({"page_number": idx, "text": page_text})
    full_text = "\n".join([str(x["text"]) for x in page_chunks if x["text"]]).strip()
    return full_text, page_chunks


def extract_pdf_text(path: str | Path) -> str:
    text, _ = extract_pdf_text_with_pages(path)
    return text


def extract_checkpoints(page_chunks: list[dict[str, str | int]], context_window: int = 1) -> pd.DataFrame:
    rows: list[dict[str, str | int | None]] = []
    for chunk in page_chunks:
        page_number = int(chunk["page_number"])
        lines = [ln.strip() for ln in str(chunk["text"]).splitlines() if ln.strip()]
        for i, line in enumerate(lines):
            dates = DATE_PATTERN.findall(line)
            if not dates:
                continue
            start = max(0, i - context_window)
            end = min(len(lines), i + context_window + 1)
            excerpt = " ".join(lines[start:end])
            name = line.split(":")[0][:120] if ":" in line else line[:120]
            description = line.split(":", 1)[1].strip() if ":" in line else line
            for d in dates:
                rows.append(
                    {
                        "date": d,
                        "checkpoint_name": name,
                        "description": description,
                        "source_excerpt": excerpt,
                        "page_number": page_number,
                    }
                )
    return pd.DataFrame(rows)
