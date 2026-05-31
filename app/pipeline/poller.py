import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiofiles
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


def _load_json(val):
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        return json.loads(val)
    return {}


async def _write_dead_letter(event_id, event: dict):
    try:
        async with aiofiles.open(settings.dead_letter_path, "a") as f:
            entry = {"id": event_id, "event": event, "failed_at": datetime.now(timezone.utc).isoformat()}
            await f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.error("failed to write dead letter: %s", e)


async def poll_loop(shared_client: httpx.AsyncClient | None = None):
    _metrics["started_at"] = time.time()
    client = shared_client or httpx.AsyncClient(timeout=30)

    while _running:
        rules = await load_rules()
        try:
            rows = await Pool.fetch(
                """SELECT id, source, source_instance, event_type, severity,
                          title, payload, context, tags, raw, created_at, delivery_attempts
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
                    _metrics["last_delivery"] = datetime.now(timezone.utc)
                else:
                    attempts = row["delivery_attempts"] + 1
                    if attempts >= settings.max_retries:
                        await Pool.execute(
                            """UPDATE event_outbox
                               SET pushed = TRUE, error = $1
                               WHERE id = $2""",
                            "dead_letter",
                            event_id,
                        )
                        await _write_dead_letter(event_id, event)
                        logger.warning("event %s moved to dead letter after %d attempts", event_id, attempts)
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

    if client is not shared_client:
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
