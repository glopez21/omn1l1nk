from datetime import datetime
from pydantic import BaseModel, Field


class IngestEvent(BaseModel):
    source: str = Field(..., max_length=64)
    source_instance: str = Field(..., max_length=128)
    event_type: str = Field(..., max_length=64)
    severity: str = Field(..., pattern=r"^(low|medium|high|critical)$")
    title: str = Field(default="", max_length=512)
    payload: dict = Field(default_factory=dict)
    context: dict = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    raw: str | None = None
    created_at: datetime | None = None


class IngestResponse(BaseModel):
    status: str = "accepted"
    id: str | None = None
    queue_depth: int = 0


class HealthResponse(BaseModel):
    status: str
    queue_depth: int
    oldest_unpushed_seconds: float | None = None
    delivery_rate_per_min: float = 0
    error_rate_per_min: float = 0
    last_delivery: datetime | None = None
    augur_status: str = "unknown"
    threatpulse_status: str = "unknown"
    n3xusdb_status: str = "unknown"


class QueueDetail(BaseModel):
    total_unpushed: int
    by_source: dict[str, int]
    by_severity: dict[str, int]
    failed: dict
