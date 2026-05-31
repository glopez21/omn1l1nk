import asyncio
import json
import logging
import time

from app.config import settings
from app.db.session import Pool

logger = logging.getLogger("omn1l1nk.enrich")

_rules_cache: list[dict] | None = None
_rules_cache_ts: float = 0


def _load_json(val):
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        return json.loads(val)
    return {}


async def load_rules():
    global _rules_cache, _rules_cache_ts
    now = time.time()
    if _rules_cache is not None and (now - _rules_cache_ts) < settings.rules_cache_ttl:
        return _rules_cache

    rows = await Pool.fetch(
        "SELECT name, match, enrich FROM enrich_rules WHERE enabled = TRUE ORDER BY priority"
    )
    _rules_cache = [
        {
            "name": r["name"],
            "match": _load_json(r["match"]),
            "enrich": _load_json(r["enrich"]),
        }
        for r in rows
    ]
    _rules_cache_ts = now
    return _rules_cache


def invalidate_rules_cache():
    global _rules_cache
    _rules_cache = None


def _flatten(d: dict, prefix: str = "") -> dict:
    result = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result.update(_flatten(v, key))
        else:
            result[key] = v
    return result


def _get_nested(obj: dict, path: str):
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _match_event(rule_match: dict, event: dict) -> bool:
    flat = _flatten(rule_match)
    for key, expected in flat.items():
        val = _get_nested(event, key)
        if val is None:
            return False
        if isinstance(expected, list):
            if val not in expected:
                return False
        elif val != expected:
            return False
    return True


def enrich_event(event: dict, rules: list[dict]) -> dict:
    event = {**event}
    for rule in rules:
        if not _match_event(rule["match"], event):
            continue
        enrich = rule["enrich"]

        if "severity" in enrich:
            sev_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
            current = sev_order.get(event.get("severity", "low"), 0)
            override = sev_order.get(enrich["severity"], 0)
            if override > current:
                event["severity"] = enrich["severity"]

        if "confidence" in enrich:
            existing = event.get("confidence", 0.5)
            event["confidence"] = round(existing * 0.4 + enrich["confidence"] * 0.6, 3)

        if "tags" in enrich:
            existing = set(event.get("tags", []))
            event["tags"] = list(existing | set(enrich["tags"]))

        if "context" in enrich:
            ctx = event.get("context", {})
            for k, v in enrich["context"].items():
                ctx[k] = v
            event["context"] = ctx

    return event
