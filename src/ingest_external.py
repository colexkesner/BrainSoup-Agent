from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import append_jsonl, now_iso

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".zip"}


def _normalize_county(value: Any) -> str:
    return str(value).lower().replace(" county", "").strip()


def _slug(name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")


def _find_local_file(dataset_name: str, approved_dir: Path, dataset_meta: dict[str, Any]) -> Path | None:
    explicit = dataset_meta.get("local_file")
    if explicit:
        p = Path(explicit)
        if p.exists():
            return p
        p2 = approved_dir / explicit
        if p2.exists():
            return p2
    slug = _slug(dataset_name)
    for f in sorted(approved_dir.glob("*")):
        if slug in _slug(f.stem):
            return f
    files = sorted(approved_dir.glob("*"))
    return files[0] if len(files) == 1 else None


def _load_external_frame(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".xlsx":
        return pd.read_excel(path)
    if ext == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            candidates = [n for n in zf.namelist() if n.lower().endswith(".csv") or n.lower().endswith(".xlsx")]
            if not candidates:
                raise ValueError(f"Zip file {path} contains no csv/xlsx files")
            name = candidates[0]
            with zf.open(name) as f:
                payload = f.read()
            if name.lower().endswith(".csv"):
                return pd.read_csv(io.BytesIO(payload))
            return pd.read_excel(io.BytesIO(payload))
    raise ValueError(f"Unsupported file type: {ext}")


def _prepare_joinable_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = {c.lower(): c for c in out.columns}
    if "fips" not in out.columns:
        if "geo id2" in cols:
            out["fips"] = out[cols["geo id2"]].apply(lambda x: f"{int(x):05d}")
        elif "fips_code" in cols:
            out["fips"] = out[cols["fips_code"]].apply(lambda x: f"{int(x):05d}")
    if "year" not in out.columns and "year" in cols:
        out["year"] = out[cols["year"]].astype(int)
    if "county_name_norm" not in out.columns:
        county_col = cols.get("county") or cols.get("county_name")
        if county_col:
            out["county_name_norm"] = out[county_col].map(_normalize_county)
    if "state" not in out.columns:
        state_col = cols.get("state") or cols.get("state abbr")
        if state_col:
            out["state"] = out[state_col].astype(str)
    return out


def _validate_join_keys(join_keys: list[str], allow_county_state_year: bool) -> tuple[bool, str]:
    jk = {k.strip().lower() for k in join_keys}
    if {"fips", "year"}.issubset(jk):
        return True, "ok"
    if allow_county_state_year and {"county_name_norm", "state", "year"}.issubset(jk):
        return True, "ok"
    if allow_county_state_year:
        return False, "join keys must include either [fips, year] or [county_name_norm, state, year]"
    return False, "join keys must include [fips, year] (county/state/year disabled by config)"


def ingest_approved_datasets(
    base_df: pd.DataFrame,
    approvals: dict[str, Any],
    cfg: dict[str, Any],
    run_log_path: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]], pd.DataFrame]:
    approved_dir = Path(cfg.get("approved_data_dir", "data/raw/approved"))
    approved_dir.mkdir(parents=True, exist_ok=True)
    min_match_rate = float(cfg.get("external_join_min_match_rate", 0.5))
    allow_county_state_year = bool(cfg.get("allow_county_state_year_join", True))

    enriched = base_df.copy()
    join_audit: list[dict[str, Any]] = []

    for ds in approvals.get("approved_datasets", []):
        if ds.get("status") != "approved":
            continue
        name = ds.get("name", "unnamed_dataset")
        join_keys = ds.get("join_keys", [])
        allow_low_match = bool(ds.get("allow_low_match_override", False))
        audit = {
            "dataset_name": name,
            "local_file": None,
            "join_keys": ",".join(join_keys),
            "rows_external": 0,
            "rows_fact": int(len(base_df)),
            "matched_rows": 0,
            "match_rate": 0.0,
            "blocked_reason": "",
            "approval_status": ds.get("status"),
            "join_outcome": "blocked",
        }

        valid_keys, msg = _validate_join_keys(join_keys, allow_county_state_year=allow_county_state_year)
        if not valid_keys:
            audit["blocked_reason"] = msg
            join_audit.append(audit)
            append_jsonl(run_log_path, {"ts": now_iso(), "event": "external_ingest_blocked", "dataset": name, "reason": msg})
            continue

        local_file = _find_local_file(name, approved_dir, ds)
        if local_file is None:
            audit["blocked_reason"] = "No mapped local file in data/raw/approved"
            join_audit.append(audit)
            append_jsonl(run_log_path, {"ts": now_iso(), "event": "external_ingest_blocked", "dataset": name, "reason": "missing local file"})
            continue

        audit["local_file"] = str(local_file)
        if local_file.suffix.lower() not in ALLOWED_EXTENSIONS:
            audit["blocked_reason"] = f"unsupported extension {local_file.suffix.lower()}"
            join_audit.append(audit)
            continue

        external = _prepare_joinable_frame(_load_external_frame(local_file))
        keys = ["fips", "year"] if {"fips", "year"}.issubset({k.lower() for k in join_keys}) else ["county_name_norm", "state", "year"]
        audit["join_keys"] = ",".join(keys)

        missing_required = [k for k in keys if k not in external.columns]
        if missing_required:
            audit["blocked_reason"] = f"missing join columns: {','.join(missing_required)}"
            join_audit.append(audit)
            append_jsonl(run_log_path, {"ts": now_iso(), "event": "external_ingest_blocked", "dataset": name, "reason": audit["blocked_reason"]})
            continue

        dedup = external.drop_duplicates(subset=keys).copy()
        base_keys = enriched[keys].drop_duplicates()
        joined_keys = base_keys.merge(dedup[keys], on=keys, how="left", indicator=True)
        matched_rows = int((joined_keys["_merge"] == "both").sum())
        match_rate = float((joined_keys["_merge"] == "both").mean()) if len(joined_keys) else 0.0

        audit["rows_external"] = int(len(dedup))
        audit["matched_rows"] = matched_rows
        audit["match_rate"] = match_rate

        if match_rate < min_match_rate and not allow_low_match:
            audit["blocked_reason"] = f"match_rate={match_rate:.3f} below threshold={min_match_rate:.3f}"
            join_audit.append(audit)
            append_jsonl(run_log_path, {"ts": now_iso(), "event": "external_ingest_blocked", "dataset": name, "reason": "low_match", "match_rate": match_rate})
            continue

        cols_to_add = [c for c in dedup.columns if c not in keys]
        renamed = dedup[keys + cols_to_add].rename(columns={c: f"ext_{_slug(name)}__{c}" for c in cols_to_add})
        enriched = enriched.merge(renamed, on=keys, how="left")

        audit["join_outcome"] = "used"
        join_audit.append(audit)
        append_jsonl(run_log_path, {"ts": now_iso(), "event": "external_ingest_used", "dataset": name, "local_file": str(local_file), "match_rate": match_rate})

    join_audit_df = pd.DataFrame(join_audit)
    return enriched, join_audit, join_audit_df
