from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    yaml = None


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


def _parse_scalar(text: str) -> Any:
    s = text.strip()
    if s in {"true", "True"}:
        return True
    if s in {"false", "False"}:
        return False
    if s in {"null", "None", "~"}:
        return None
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(x) for x in inner.split(",")]
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s.strip('"').strip("'")


def _minimal_yaml_load(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(0, root)]

    for raw in text.splitlines():
        if not raw.strip() or raw.strip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        while stack and indent < stack[-1][0]:
            stack.pop()
        current = stack[-1][1]

        if value == "":
            child: dict[str, Any] = {}
            current[key] = child
            stack.append((indent + 2, child))
        else:
            current[key] = _parse_scalar(value)

    return root


def _minimal_yaml_dump(data: Any, indent: int = 0) -> list[str]:
    pad = " " * indent
    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(f"{pad}{key}:")
                lines.extend(_minimal_yaml_dump(value, indent + 2))
            elif isinstance(value, list):
                if not value:
                    lines.append(f"{pad}{key}: []")
                else:
                    lines.append(f"{pad}{key}:")
                    for item in value:
                        if isinstance(item, dict):
                            lines.append(f"{pad}  -")
                            lines.extend(_minimal_yaml_dump(item, indent + 4))
                        else:
                            lines.append(f"{pad}  - {item}")
            else:
                lines.append(f"{pad}{key}: {value}")
        return lines
    return [f"{pad}{data}"]


def load_yaml(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(text) or {}
    return _minimal_yaml_load(text)


def save_yaml(path: str | Path, data: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if yaml is not None:
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return
    Path(path).write_text("\n".join(_minimal_yaml_dump(data)) + "\n", encoding="utf-8")


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
