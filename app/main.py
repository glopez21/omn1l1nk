import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db.session import Pool
from app.ingest.router import router as ingest_router
from app.admin.router import router as admin_router
from app.pipeline.poller import poll_loop, stop as stop_poller, get_metrics
from app.pipeline.agents import heartbeat_loop, stop as stop_agents
from app.models.schemas import HealthResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("omn1l1nk")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("connecting to n3xusdb...")
    await Pool.connect()
    logger.info("starting poll loop...")
    poll_task = asyncio.create_task(poll_loop())

    agent_http = httpx.AsyncClient(timeout=15)
    app.state.agent_http = agent_http
    agent_task = asyncio.create_task(heartbeat_loop())

    yield
    logger.info("shutting down...")
    stop_poller()
    stop_agents()
    poll_task.cancel()
    agent_task.cancel()
    try:
        await poll_task
    except asyncio.CancelledError:
        pass
    try:
        await agent_task
    except asyncio.CancelledError:
        pass
    await agent_http.aclose()
    await Pool.close()


app = FastAPI(
    title="Omn1L1nk",
    description="Unified event delivery pipeline — n3xusDB → Augur / ThreatPulse",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(ingest_router)
app.include_router(admin_router)

_admin_static = Path(__file__).parent / "admin" / "static"
app.mount("/admin", StaticFiles(directory=str(_admin_static), html=True), name="admin")


@app.get("/admin")
async def admin_redirect():
    return RedirectResponse(url="/admin/")


@app.get("/health", response_model=HealthResponse)
async def health():
    pool_ok = Pool._pool is not None and not Pool._pool._closed
    augur_ok = "unknown"
    tp_ok = "unknown"

    import httpx

    if settings.augur_enabled:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{settings.augur_url}/api/v1/health")
                augur_ok = "reachable" if r.is_success else "error"
        except Exception:
            augur_ok = "unreachable"

    if settings.threatpulse_enabled:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{settings.threatpulse_url}/health")
                tp_ok = "reachable" if r.is_success else "error"
        except Exception:
            tp_ok = "unreachable"

    depth = 0
    oldest = None
    try:
        row = await Pool.fetchrow("SELECT count(*) AS cnt FROM event_outbox WHERE pushed = FALSE")
        depth = row["cnt"] if row else 0

        row = await Pool.fetchrow(
            "SELECT EXTRACT(EPOCH FROM (NOW() - created_at)) AS age FROM event_outbox WHERE pushed = FALSE ORDER BY created_at LIMIT 1"
        )
        oldest = float(row["age"]) if row and row["age"] else None
    except Exception:
        pass

    metrics = get_metrics()

    return HealthResponse(
        status="healthy" if pool_ok else "degraded",
        queue_depth=depth,
        oldest_unpushed_seconds=oldest,
        delivery_rate_per_min=metrics["delivery_rate_per_min"],
        error_rate_per_min=metrics["error_rate_per_min"],
        last_delivery=metrics["last_delivery"],
        augur_status=augur_ok,
        threatpulse_status=tp_ok,
        n3xusdb_status="connected" if pool_ok else "disconnected",
    )


def run():
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=9000, reload=True)


if __name__ == "__main__":
    run()
