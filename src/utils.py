from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator


EXPECTED_YEARS = {2010, 2012, 2014, 2016, 2018, 2019, 2021, 2022, 2023}
COUNTY_COLUMNS = [
    "State",
    "Year",
    "GEO id2",
    "GEO display_label",
    "County",
    "State Abbr",
    "Households",
    "Poverty Households",
    "ALICE Households",
    "Above ALICE Households",
    "ALICE Threshold - HH under 65",
    "ALICE Threshold - HH 65 years and over",
    "Source: American Community Survey",
]


@dataclass
class PipelineConfig:
    raw: dict[str, Any]

    @property
    def seed(self) -> int:
        return int(self.raw.get("seed", 42))

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: str | Path, data: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_payload(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def cached_json_response(cache_dir: str | Path, key: str) -> dict[str, Any] | None:
    p = Path(cache_dir) / f"{key}.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def store_cached_json_response(cache_dir: str | Path, key: str, value: dict[str, Any]) -> None:
    d = Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{key}.json").write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")


def validate_json_schema(obj: dict[str, Any], schema: dict[str, Any]) -> tuple[bool, list[str]]:
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(obj), key=lambda e: e.path)
    msgs = [f"{'/'.join([str(x) for x in err.path])}: {err.message}" for err in errors]
    return len(errors) == 0, msgs


def pad_fips(value: Any) -> str:
    return f"{int(value):05d}"


def safe_divide(num: float, denom: float) -> float:
    if denom in (0, 0.0) or denom is None:
        return 0.0
    return float(num) / float(denom)
