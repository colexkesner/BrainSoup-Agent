from __future__ import annotations

import json
import os
from typing import Any

from openai import OpenAI, AzureOpenAI

from src.utils import (
    append_jsonl,
    cached_json_response,
    hash_payload,
    now_iso,
    store_cached_json_response,
    validate_json_schema,
)

RESEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["recommended_datasets", "recommended_methods", "mapping_guidance", "questions_for_user"],
    "properties": {
        "recommended_datasets": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name", "purpose", "suggested_sources", "join_keys", "granularity", "priority", "risks_or_limitations", "citations"],
                "properties": {
                    "name": {"type": "string"},
                    "purpose": {"type": "string"},
                    "suggested_sources": {"type": "array", "items": {"type": "string"}},
                    "join_keys": {"type": "array", "items": {"type": "string"}},
                    "granularity": {"type": "string"},
                    "priority": {"type": "string"},
                    "risks_or_limitations": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
        "recommended_methods": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["method", "why", "how_to_apply", "citations"],
                "properties": {
                    "method": {"type": "string"},
                    "why": {"type": "string"},
                    "how_to_apply": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
        "mapping_guidance": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["approach", "steps", "citations"],
                "properties": {
                    "approach": {"type": "string"},
                    "steps": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
        "questions_for_user": {"type": "array", "items": {"type": "string"}},
    },
}


def _client(cfg: dict[str, Any]):
    provider = cfg.get("provider", "openai")
    if provider == "azure":
        return AzureOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            api_version=cfg.get("azure_api_version", "2024-10-01-preview"),
            azure_endpoint=cfg.get("azure_endpoint"),
        )
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _tools(cfg: dict[str, Any]) -> list[dict[str, str]]:
    tool_type = cfg.get("web_search_tool_type") or ("web_search_preview" if cfg.get("provider") == "azure" else "web_search")
    return [{"type": tool_type}]


def run_research_agent(cfg: dict[str, Any], pdf_text: str, compact_summary: dict[str, Any], run_log_path: str) -> dict[str, Any]:
    payload = {
        "pdf_excerpt": pdf_text[:5000],
        "summary": compact_summary,
        "schema": RESEARCH_SCHEMA,
        "tool_type": cfg.get("web_search_tool_type"),
        "provider": cfg.get("provider"),
        "model": cfg.get("model"),
    }
    cache_key = hash_payload(payload)
    cached = cached_json_response(cfg["cache_dir"], cache_key)
    if cached:
        return cached

    if not os.getenv("OPENAI_API_KEY"):
        fallback = {
            "recommended_datasets": [],
            "recommended_methods": [],
            "mapping_guidance": [],
            "questions_for_user": ["Set OPENAI_API_KEY to enable web research recommendations."],
        }
        store_cached_json_response(cfg["cache_dir"], cache_key, fallback)
        return fallback

    prompt = (
        "You are a research agent. Use web search tool and return ONLY valid JSON matching schema. "
        "Do not include markdown. Do not invent numeric facts. Include URL citations in arrays.\n"
        f"Schema: {json.dumps(RESEARCH_SCHEMA)}\n"
        f"PDF text excerpt: {pdf_text[:5000]}\n"
        f"Summary: {json.dumps(compact_summary)}"
    )

    client = _client(cfg)
    attempts = 0
    max_retries = int(cfg.get("max_retries", 2))
    last_raw = ""
    while attempts <= max_retries:
        attempts += 1
        resp = client.responses.create(
            model=cfg.get("model", "gpt-4.1-mini"),
            input=prompt if attempts == 1 else f"Repair JSON to satisfy schema. Return only JSON. Previous:\n{last_raw}",
            tools=_tools(cfg),
        )
        text = getattr(resp, "output_text", "") or ""
        last_raw = text
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        valid, errors = validate_json_schema(obj, RESEARCH_SCHEMA)
        if valid:
            store_cached_json_response(cfg["cache_dir"], cache_key, obj)
            append_jsonl(run_log_path, {"ts": now_iso(), "event": "research_agent_success", "attempt": attempts})
            return obj
        append_jsonl(run_log_path, {"ts": now_iso(), "event": "research_agent_schema_error", "attempt": attempts, "errors": errors})

    append_jsonl(run_log_path, {"ts": now_iso(), "event": "research_agent_failure", "raw": last_raw})
    return {
        "recommended_datasets": [],
        "recommended_methods": [],
        "mapping_guidance": [],
        "questions_for_user": ["Research output invalid after retries; inspect run log raw response."],
    }
