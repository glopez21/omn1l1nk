import hashlib
import json
import secrets
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.db.session import Pool

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


# ─── Schemas ─────────────────────────────────────────────────


class ApiKeyCreate(BaseModel):
    label: str = Field(..., max_length=128)
    source: str = Field(..., max_length=64)
    source_instance: str = Field(..., max_length=128)


class ApiKeyUpdate(BaseModel):
    label: str | None = None
    enabled: bool | None = None


class ApiKeyResponse(BaseModel):
    id: str
    label: str
    source: str
    source_instance: str
    enabled: bool
    created_at: datetime
    key: str | None = None


class RuleCreate(BaseModel):
    name: str = Field(..., max_length=255)
    match: dict
    enrich: dict
    priority: int = 100
    enabled: bool = True


class RuleUpdate(BaseModel):
    name: str | None = None
    match: dict | None = None
    enrich: dict | None = None
    priority: int | None = None
    enabled: bool | None = None


class RuleResponse(BaseModel):
    id: str
    name: str
    match: dict
    enrich: dict
    priority: int
    enabled: bool
    created_at: datetime
    updated_at: datetime


class ReorderItem(BaseModel):
    id: str
    priority: int


class ReorderRequest(BaseModel):
    items: list[ReorderItem]


# ─── API Keys ─────────────────────────────────────────────────


@router.get("/api-keys", response_model=list[ApiKeyResponse])
async def list_api_keys():
    rows = await Pool.fetch(
        "SELECT id, label, source, source_instance, enabled, created_at FROM daemon_api_keys ORDER BY created_at DESC"
    )
    return [
        ApiKeyResponse(
            id=str(r["id"]),
            label=r["label"],
            source=r["source"],
            source_instance=r["source_instance"],
            enabled=r["enabled"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


@router.post("/api-keys", response_model=ApiKeyResponse, status_code=201)
async def create_api_key(body: ApiKeyCreate):
    raw_key = secrets.token_urlsafe(32)
    key_hash = _hash_key(raw_key)

    row = await Pool.fetchrow(
        """INSERT INTO daemon_api_keys (api_key_hash, label, source, source_instance)
           VALUES ($1, $2, $3, $4) RETURNING id, created_at""",
        key_hash,
        body.label,
        body.source,
        body.source_instance,
    )

    return ApiKeyResponse(
        id=str(row["id"]),
        label=body.label,
        source=body.source,
        source_instance=body.source_instance,
        enabled=True,
        created_at=row["created_at"],
        key=raw_key,
    )


@router.patch("/api-keys/{key_id}", response_model=ApiKeyResponse)
async def update_api_key(key_id: str, body: ApiKeyUpdate):
    sets = []
    args = []
    idx = 1

    if body.label is not None:
        sets.append(f"label = ${idx}")
        args.append(body.label)
        idx += 1
    if body.enabled is not None:
        sets.append(f"enabled = ${idx}")
        args.append(body.enabled)
        idx += 1

    if not sets:
        raise HTTPException(400, "no fields to update")

    args.append(key_id)
    row = await Pool.fetchrow(
        f"UPDATE daemon_api_keys SET {', '.join(sets)} WHERE id = ${idx} RETURNING id, label, source, source_instance, enabled, created_at",
        *args,
    )
    if not row:
        raise HTTPException(404, "api_key not found")

    return ApiKeyResponse(
        id=str(row["id"]),
        label=row["label"],
        source=row["source"],
        source_instance=row["source_instance"],
        enabled=row["enabled"],
        created_at=row["created_at"],
    )


@router.delete("/api-keys/{key_id}", status_code=204)
async def delete_api_key(key_id: str):
    r = await Pool.execute("DELETE FROM daemon_api_keys WHERE id = $1", key_id)
    if r == "DELETE 0":
        raise HTTPException(404, "api_key not found")


# ─── Enrich Rules ────────────────────────────────────────────


@router.get("/rules", response_model=list[RuleResponse])
async def list_rules():
    rows = await Pool.fetch(
        "SELECT id, name, match, enrich, priority, enabled, created_at, updated_at FROM enrich_rules ORDER BY priority, name"
    )
    return [
        RuleResponse(
            id=str(r["id"]),
            name=r["name"],
            match=json.loads(r["match"]) if isinstance(r["match"], str) else r["match"] if r["match"] else {},
            enrich=json.loads(r["enrich"]) if isinstance(r["enrich"], str) else r["enrich"] if r["enrich"] else {},
            priority=r["priority"],
            enabled=r["enabled"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )
        for r in rows
    ]


@router.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(body: RuleCreate):
    row = await Pool.fetchrow(
        """INSERT INTO enrich_rules (name, match, enrich, priority, enabled)
           VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
           RETURNING id, name, match, enrich, priority, enabled, created_at, updated_at""",
        body.name,
        json.dumps(body.match),
        json.dumps(body.enrich),
        body.priority,
        body.enabled,
    )
    row["match"] = json.loads(row["match"]) if isinstance(row["match"], str) else row["match"]
    row["enrich"] = json.loads(row["enrich"]) if isinstance(row["enrich"], str) else row["enrich"]
    return _row_to_rule(row)


@router.patch("/rules/{rule_id}", response_model=RuleResponse)
async def update_rule(rule_id: str, body: RuleUpdate):
    sets = []
    args = []
    idx = 1

    if body.name is not None:
        sets.append(f"name = ${idx}")
        args.append(body.name)
        idx += 1
    if body.match is not None:
        sets.append(f"match = ${idx}::jsonb")
        args.append(json.dumps(body.match))
        idx += 1
    if body.enrich is not None:
        sets.append(f"enrich = ${idx}::jsonb")
        args.append(json.dumps(body.enrich))
        idx += 1
    if body.priority is not None:
        sets.append(f"priority = ${idx}")
        args.append(body.priority)
        idx += 1
    if body.enabled is not None:
        sets.append(f"enabled = ${idx}")
        args.append(body.enabled)
        idx += 1

    if not sets:
        raise HTTPException(400, "no fields to update")

    sets.append("updated_at = NOW()")
    args.append(rule_id)
    row = await Pool.fetchrow(
        f"UPDATE enrich_rules SET {', '.join(sets)} WHERE id = ${idx} RETURNING id, name, match, enrich, priority, enabled, created_at, updated_at",
        *args,
    )
    if not row:
        raise HTTPException(404, "rule not found")

    row["match"] = json.loads(row["match"]) if isinstance(row["match"], str) else row["match"]
    row["enrich"] = json.loads(row["enrich"]) if isinstance(row["enrich"], str) else row["enrich"]
    return _row_to_rule(row)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: str):
    r = await Pool.execute("DELETE FROM enrich_rules WHERE id = $1", rule_id)
    if r == "DELETE 0":
        raise HTTPException(404, "rule not found")


@router.post("/rules/reorder", status_code=200)
async def reorder_rules(body: ReorderRequest):
    for item in body.items:
        await Pool.execute(
            "UPDATE enrich_rules SET priority = $1, updated_at = NOW() WHERE id = $2",
            item.priority,
            item.id,
        )
    return {"status": "ok"}


# ─── Stats ───────────────────────────────────────────────────


@router.get("/stats")
async def admin_stats():
    total = await Pool.fetchrow("SELECT count(*) AS cnt FROM event_outbox")
    unpushed = await Pool.fetchrow("SELECT count(*) AS cnt FROM event_outbox WHERE pushed = FALSE")
    by_source = await Pool.fetch(
        "SELECT source, count(*) AS cnt FROM event_outbox WHERE pushed = FALSE GROUP BY source"
    )
    by_severity = await Pool.fetch(
        "SELECT severity, count(*) AS cnt FROM event_outbox WHERE pushed = FALSE GROUP BY severity"
    )
    failed = await Pool.fetchrow(
        "SELECT count(*) AS cnt, MAX(created_at) AS oldest FROM event_outbox WHERE delivery_attempts > 0 AND pushed = FALSE"
    )

    return {
        "total_events": total["cnt"],
        "queue_depth": unpushed["cnt"],
        "by_source": {r["source"]: r["cnt"] for r in by_source},
        "by_severity": {r["severity"]: r["cnt"] for r in by_severity},
        "failed": {
            "count": failed["cnt"],
            "oldest": failed["oldest"].isoformat() if failed["oldest"] else None,
        },
    }


@router.get("/events", response_model=dict)
async def list_events(limit: int = 50, offset: int = 0, pushed: bool | None = None):
    where = ""
    args = []
    idx = 1

    if pushed is not None:
        where = f"WHERE pushed = ${idx}"
        args.append(pushed)
        idx += 1

    count_row = await Pool.fetchrow(f"SELECT count(*) AS cnt FROM event_outbox {where}", *args)

    args.append(limit)
    args.append(offset)
    rows = await Pool.fetch(
        f"""SELECT id, source, source_instance, event_type, severity, title,
                   payload, context, tags, raw, created_at, pushed, pushed_at, delivery_attempts, error
            FROM event_outbox {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}""",
        *args,
    )

    return {
        "total": count_row["cnt"],
        "items": [
            {
                "id": str(r["id"]),
                "source": r["source"],
                "source_instance": r["source_instance"],
                "event_type": r["event_type"],
                "severity": r["severity"],
                "title": r["title"],
                "created_at": r["created_at"].isoformat(),
                "pushed": r["pushed"],
                "pushed_at": r["pushed_at"].isoformat() if r["pushed_at"] else None,
                "delivery_attempts": r["delivery_attempts"],
                "error": r["error"],
            }
            for r in rows
        ],
    }


# ─── Helpers ─────────────────────────────────────────────────


def _row_to_rule(row) -> RuleResponse:
    return RuleResponse(
        id=str(row["id"]),
        name=row["name"],
        match=row["match"],
        enrich=row["enrich"],
        priority=row["priority"],
        enabled=row["enabled"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
