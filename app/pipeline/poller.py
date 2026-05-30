import asyncio
import json
import logging
import time
from datetime import datetime

import httpx

from app.config import settings
from app.db.session import Pool
from app.pipeline.enrich import enrich_event, load_rules

logger = logging.getLogger("omn1l1nk.poller")

_running = True
_metrics = {
    "delivered": 0,
    "errors": 0,
    "started_at": 0.0,
    "last_delivery": None,
}


def stop():
    global _running
    _running = False


def get_metrics() -> dict:
    elapsed = time.time() - _metrics.get("started_at", time.time())
    rate = (_metrics["delivered"] / elapsed * 60) if elapsed > 0 else 0
    err_rate = (_metrics["errors"] / elapsed * 60) if elapsed > 0 else 0
    return {
        "delivery_rate_per_min": round(rate, 1),
        "error_rate_per_min": round(err_rate, 1),
        "last_delivery": _metrics["last_delivery"],
    }


async def poll_loop():
    _metrics["started_at"] = time.time()
    client = httpx.AsyncClient(timeout=30)

    while _running:
        rules = await load_rules()
        try:
            rows = await Pool.fetch(
                """SELECT id, source, source_instance, event_type, severity,
                          title, payload, context, tags, raw, created_at
                   FROM event_outbox
                   WHERE pushed = FALSE AND delivery_attempts < $1
                   ORDER BY created_at
                   LIMIT $2
                   FOR UPDATE SKIP LOCKED""",
                settings.max_retries,
                settings.batch_size,
            )

            if not rows:
                await asyncio.sleep(settings.poll_interval)
                continue

            for row in rows:
                event_id = row["id"]
                def _load_json(val):
                    if val is None:
                        return {}
                    if isinstance(val, dict):
                        return val
                    if isinstance(val, str):
                        return json.loads(val)
                    return {}

                event = {
                    "source": row["source"],
                    "source_instance": row["source_instance"],
                    "event_type": row["event_type"],
                    "severity": row["severity"],
                    "title": row["title"],
                    "payload": _load_json(row["payload"]),
                    "context": _load_json(row["context"]),
                    "tags": list(row["tags"]) if row["tags"] else [],
                }

                event = enrich_event(event, rules)
                ts = row["created_at"]
                if ts.tzinfo is not None:
                    ts = ts.replace(tzinfo=None)
                event["timestamp"] = ts.isoformat()

                success = True

                if settings.augur_enabled:
                    ok = await _deliver_to_augur(client, event, event_id)
                    if not ok:
                        success = False

                if settings.threatpulse_enabled:
                    ok = await _deliver_to_threatpulse(client, event, event_id)
                    if not ok:
                        success = False

                if success:
                    await Pool.execute(
                        "UPDATE event_outbox SET pushed = TRUE, pushed_at = NOW() WHERE id = $1",
                        event_id,
                    )
                    _metrics["delivered"] += 1
                    _metrics["last_delivery"] = datetime.utcnow()
                else:
                    await Pool.execute(
                        """UPDATE event_outbox
                           SET delivery_attempts = delivery_attempts + 1, error = $1
                           WHERE id = $2""",
                        "delivery_failed",
                        event_id,
                    )
                    _metrics["errors"] += 1

        except Exception as e:
            logger.exception("poll cycle error: %s", e)
            await asyncio.sleep(settings.poll_interval)

    await client.aclose()


async def _deliver_to_augur(client: httpx.AsyncClient, event: dict, event_id) -> bool:
    try:
        body = {
            "event_type": event["event_type"],
            "severity": event["severity"],
            "source": event["source"],
            "agent_id": event["source_instance"],
            "timestamp": event["timestamp"],
            "title": event["title"],
            "payload": event["payload"],
            "context": event["context"],
            "tags": event["tags"],
        }
        resp = await client.post(
            f"{settings.augur_url}/api/v1/events",
            json=body,
            timeout=15,
        )
        if resp.is_success:
            return True
        logger.warning("augur delivery failed (%s): %s", resp.status_code, resp.text[:200])
        return False
    except httpx.RequestError as e:
        logger.warning("augur unreachable: %s", e)
        return False


async def _deliver_to_threatpulse(client: httpx.AsyncClient, event: dict, event_id) -> bool:
    try:
        resp = await client.post(
            f"{settings.threatpulse_url}/api/v1/ingest",
            json=event,
            timeout=15,
        )
        if resp.is_success:
            return True
        logger.warning("threatpulse delivery failed (%s): %s", resp.status_code, resp.text[:200])
        return False
    except httpx.RequestError as e:
        logger.warning("threatpulse unreachable: %s", e)
        return False
