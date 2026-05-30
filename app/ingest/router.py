import hashlib
import json
import logging

import httpx
from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.db.session import Pool
from app.models.schemas import IngestEvent, IngestResponse
from app.pipeline.agents import ensure_registered, mark_seen

logger = logging.getLogger("omn1l1nk.ingest")
router = APIRouter(prefix="/api/v1", tags=["ingest"])


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def _validate_api_key(request: Request) -> bool:
    key = request.headers.get("X-API-Key", "")
    if not key:
        return False
    if settings.ingest_api_keys and key in settings.ingest_api_keys:
        return True
    kh = _hash_key(key)
    row = await Pool.fetchrow(
        "SELECT 1 FROM daemon_api_keys WHERE api_key_hash = $1 AND enabled = TRUE",
        kh,
    )
    return row is not None


@router.post("/ingest", response_model=IngestResponse, status_code=201)
async def ingest_event(event: IngestEvent, request: Request):
    if not await _validate_api_key(request):
        raise HTTPException(status_code=401, detail="invalid_api_key")

    row = await Pool.fetchrow(
        """INSERT INTO event_outbox
           (source, source_instance, event_type, severity, title, payload, context, tags, raw, created_at)
           VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::text[], $9,
                   COALESCE($10, NOW()))
           RETURNING id""",
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

    # Register agent + track heartbeat
    agent_http: httpx.AsyncClient | None = getattr(request.app.state, "agent_http", None)
    if agent_http:
        await ensure_registered(agent_http, event.source, event.source_instance)
        mark_seen(event.source_instance)

    depth_row = await Pool.fetchrow("SELECT count(*) AS cnt FROM event_outbox WHERE pushed = FALSE")
    return IngestResponse(
        status="accepted",
        id=str(row["id"]),
        queue_depth=depth_row["cnt"],
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
