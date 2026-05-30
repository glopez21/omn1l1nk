"""Agent lifecycle management — register + heartbeat daemons in Augur."""

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.config import settings

logger = logging.getLogger("omn1l1nk.agents")

_registry: dict[str, str] = {}        # source_instance → agent_id
_last_seen: dict[str, datetime] = {}  # source_instance → last event time
_running = True


def stop():
    global _running
    _running = False


async def ensure_registered(
    client: httpx.AsyncClient,
    source: str,
    source_instance: str,
) -> str | None:
    if source_instance in _registry:
        return _registry[source_instance]

    if not settings.augur_enabled:
        return None

    try:
        resp = await client.post(
            f"{settings.augur_url}/api/v1/agents/register",
            json={
                "name": source_instance,
                "agent_type": source,
                "version": "",
                "hostname": source_instance,
                "ip_address": "",
                "labels": {"managed_by": "omn1l1nk"},
                "config": {},
            },
            timeout=10,
        )
        if resp.is_success:
            data = resp.json()
            agent_id = data["id"]
            _registry[source_instance] = agent_id
            logger.info("registered agent %s → %s", source_instance, agent_id)
            return agent_id
        if resp.status_code == 400 and "already registered" in resp.text:
            # Agent exists but we don't know its ID — fetch it
            list_resp = await client.get(
                f"{settings.augur_url}/api/v1/agents",
                timeout=10,
            )
            if list_resp.is_success:
                for agent in list_resp.json():
                    if agent["name"] == source_instance:
                        _registry[source_instance] = agent["id"]
                        return agent["id"]
        logger.warning("agent registration failed (%s): %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as e:
        logger.warning("augur unreachable during agent registration: %s", e)

    return None


def mark_seen(source_instance: str):
    _last_seen[source_instance] = datetime.now(timezone.utc)


async def heartbeat_loop():
    if not settings.augur_enabled:
        return

    client = httpx.AsyncClient(timeout=15)
    try:
        while _running:
            await asyncio.sleep(30)
            now = datetime.now(timezone.utc)
            for inst, agent_id in list(_registry.items()):
                last = _last_seen.get(inst)
                if last and (now - last).total_seconds() < 300:
                    try:
                        await client.post(
                            f"{settings.augur_url}/api/v1/agents/heartbeat",
                            json={"agent_id": agent_id},
                            timeout=10,
                        )
                    except httpx.RequestError:
                        pass
    finally:
        await client.aclose()
