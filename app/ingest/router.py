import asyncio
import hashlib
import json
import logging
import time

import httpx
from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.db.session import Pool
from app.models.schemas import IngestEvent, IngestResponse
from app.pipeline.agents import ensure_registered, mark_seen

logger = logging.getLogger("omn1l1nk.ingest")
router = APIRouter(prefix="/api/v1", tags=["ingest"])

_key_hash_cache: set[str] | None = None
_key_hash_cache_ts: float = 0
_KEY_CACHE_TTL = 60


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def _load_key_hashes() -> set[str]:
    global _key_hash_cache, _key_hash_cache_ts
    now = time.time()
    if _key_hash_cache is not None and (now - _key_hash_cache_ts) < _KEY_CACHE_TTL:
        return _key_hash_cache
    rows = await Pool.fetch("SELECT api_key_hash FROM daemon_api_keys WHERE enabled = TRUE")
    _key_hash_cache = {r["api_key_hash"] for r in rows}
    _key_hash_cache_ts = now
    return _key_hash_cache


def invalidate_key_cache():
    global _key_hash_cache
    _key_hash_cache = None


async def _validate_api_key(request: Request) -> bool:
    key = request.headers.get("X-API-Key", "")
    if not key:
        return False
    if settings.ingest_api_keys and key in settings.ingest_api_keys:
        return True
    kh = _hash_key(key)
    hashes = await _load_key_hashes()
    return kh in hashes


@router.post("/ingest", response_model=IngestResponse, status_code=201)
async def ingest_event(event: IngestEvent, request: Request):
    if not await _validate_api_key(request):
        raise HTTPException(status_code=401, detail="invalid_api_key")

    row = await Pool.fetchrow(
        """INSERT INTO event_outbox
           (source, source_instance, event_type, severity, title, payload, context, tags, raw, created_at)
           VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::text[], $9,
                   COALESCE($10, NOW()))
           RETURNING id, (SELECT count(*) FROM event_outbox WHERE pushed = FALSE) AS queue_depth""",
        event.source,
        event.source_instance,
        event.event_type,
        event.severity,
        event.title,
        json.dumps(event.payload or {}),
        json.dumps(event.context or {}),
        event.tags or [],
        event.raw,
        event.created_at,
    )

    agent_http: httpx.AsyncClient | None = getattr(request.app.state, "agent_http", None)
    if agent_http:
        asyncio.ensure_future(ensure_registered(agent_http, event.source, event.source_instance))
        mark_seen(event.source_instance)

    return IngestResponse(
        status="accepted",
        id=str(row["id"]),
        queue_depth=row["queue_depth"],
    )


@router.get("/queue", response_model=dict)
async def queue_status():
    total = await Pool.fetchrow("SELECT count(*) AS cnt FROM event_outbox WHERE pushed = FALSE")

    by_source = await Pool.fetch(
        "SELECT source, count(*) AS cnt FROM event_outbox WHERE pushed = FALSE GROUP BY source"
    )
    by_severity = await Pool.fetch(
        "SELECT severity, count(*) AS cnt FROM event_outbox WHERE pushed = FALSE GROUP BY severity"
    )

    return {
        "total_unpushed": total["cnt"],
        "by_source": {r["source"]: r["cnt"] for r in by_source},
        "by_severity": {r["severity"]: r["cnt"] for r in by_severity},
    }
