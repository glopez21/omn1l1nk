# Omn1L1nk — Unified Event Delivery Pipeline

Omn1L1nk is the single service that connects daemons to hubs. It sits between n3xusDB (the central data store) and Augur/ThreatPulse (the visualization and SOAR hubs). It handles ingestion, enrichment, routing, and delivery tracking.

## What It Does

```
Remote daemons ──HTTPS──┐
                         ├──→ Omn1L1nk (:9000)
Local daemons ──direct DB write → n3xusDB.event_outbox ──poll──┘
                                                               │
                                                    enrich rules
                                                               │
                                                    ┌──────────┤
                                                    ▼          ▼
                                                 Augur    ThreatPulse
                                                 (:8001)    (:8081)
```

1. **Accepts events** from remote daemons via HTTP
2. **Reads events** that local daemons wrote directly to n3xusDB
3. **Enriches** each event with severity, confidence, tags, and priority
4. **Routes** to configured hubs (Augur, ThreatPulse, or both)
5. **Tracks delivery** — marks as pushed, retries on failure, exposes queue depth

## What It Is NOT

- Not a database — depends on n3xusDB for all storage
- Not a hub — does not correlate, store, or visualize events
- Not a SIEM — no detection rules, no dashboards, no alert management
- Not an agent — does not run on remote machines

It is a **single-purpose delivery pipeline**. Nothing more.

## Architecture

### Service Layout

```
omn1l1nk/
├── PLAN.md                 ← this file
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
├── app/
│   ├── __init__.py
│   ├── main.py             ← FastAPI entry point
│   ├── config.py           ← Settings (n3xusDB URL, hub URLs, API keys)
│   ├── db/
│   │   ├── __init__.py
│   │   └── session.py      ← asyncpg connection pool
│   ├── ingest/
│   │   ├── __init__.py
│   │   └── router.py       ← POST /api/v1/ingest
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── poller.py        ← Background loop: poll event_outbox
│   │   ├── enrich.py        ← Rule-based enrichment engine
│   │   └── router.py        ← Route to Augur / ThreatPulse
│   └── models/
│       ├── __init__.py
│       └── schemas.py       ← Pydantic models for ingest + enrich
└── enrich_rules/
    └── default.yaml         ← Default enrichment rule set
```

### Core Loop

```
┌──────────────────────────────────────────────────────┐
│  Poller (background task, runs every 1-5s)            │
│                                                        │
│  1. SELECT * FROM event_outbox                         │
│     WHERE pushed = FALSE                               │
│     ORDER BY created_at LIMIT 100                      │
│     FOR UPDATE SKIP LOCKED                             │
│                                                        │
│  2. For each unpushed event:                           │
│       ├── enrich(event) → add severity/confidence/tags │
│       ├── route_to_augur(event)                        │
│       ├── route_to_threatpulse(event)                  │
│       └── mark_pushed(event.id)                        │
│                                                        │
│  3. On failure: increment delivery_attempts,           │
│     store error message, keep pushed = FALSE            │
│     (retry next cycle, max N attempts)                  │
│                                                        │
└──────────────────────────────────────────────────────┘
```

## HTTP Ingest Endpoint

### `POST /api/v1/ingest`

Accepts events from remote daemons. Validates, writes to `event_outbox`, returns immediately.

**Request:**
```json
{
  "source": "logsentry",
  "source_instance": "aws-box-01",
  "event_type": "ssh_bruteforce",
  "severity": "high",
  "title": "SSH brute force from 10.0.0.5 (50 attempts)",
  "payload": {
    "attempts": 50,
    "target_user": "root",
    "target_port": 22
  },
  "context": {
    "ip": "10.0.0.5",
    "host": "mail-01",
    "mitre_id": "T1110"
  },
  "tags": ["ssh", "auth", "bruteforce"],
  "raw": "May 29 10:00:00 mail-01 sshd[1234]: Failed password for root from 10.0.0.5 port 22 ssh2"
}
```

**Auth:** `X-API-Key` header. Per-machine or per-daemon keys.

**Response (201):**
```json
{
  "status": "accepted",
  "id": "uuid-of-outbox-row",
  "queue_depth": 142
}
```

**Response (401):**
```json
{
  "error": "invalid_api_key",
  "message": "No agent found for the provided API key"
}
```

The endpoint is lightweight — it validates, inserts, and returns. The heavy lifting (enrichment, routing) happens in the background poller.

## Enrichment Engine

Enrichment is rule-based. Each rule matches on event fields (source, event_type, payload values, etc.) and enriches with additional metadata.

### Rule format

```yaml
rules:
  - name: "SSH brute force scoring"
    match:
      source: "logsentry"
      event_type: "ssh_bruteforce"
    enrich:
      confidence: 0.85
      tags:
        - "high_fidelity"
      context:
        priority: "high"
        tlp: "amber"

  - name: "Known bad IPs"
    match:
      context.ip: "10.0.0.5"
    enrich:
      severity: "critical"
      confidence: 0.95
      tags:
        - "known_attacker"
        - "blocklist"

  - name: "Off-hours event boost"
    match:
      source: "alertflow"
    evaluate: "hour(created_at) < 6 OR hour(created_at) > 22"
    enrich:
      context:
        off_hours: true
      tags:
        - "off_hours_review"

  - name: "Low priority noise reduction"
    match:
      event_type: "heartbeat"
    enrich:
      tags:
        - "noise"
      context:
        routing: "archive"
```

Rules are evaluated in order. First match wins. If no rule matches, the event passes through with its original severity and tags.

### Augmenting vs Overriding

- `severity` — rule can only **escalate** (never downgrade). An event submitted as "low" can become "critical" but not vice versa.
- `tags` — appended to existing tags, never replaced.
- `confidence` — averaged with any existing score, weighted 60% rule / 40% original.
- `context` — merged shallow (top-level keys override, nested keys deep-merge).

## Routing

### Augur output

Events are sent to `POST /api/v1/events` (single-event endpoint that runs the full pipeline). The event is translated from the outbox schema to Augur's `AugurEvent`:

```python
augur_event = {
    "event_type": outbox.event_type,
    "severity": outbox.severity,
    "source": outbox.source,
    "agent_id": outbox.source_instance,
    "timestamp": outbox.created_at.isoformat(),
    "title": outbox.title,
    "payload": outbox.payload,
    "context": outbox.context,
    "tags": outbox.tags,
}
```

**Why the single-event endpoint, not batch:**
- Augur's `POST /api/v1/events` runs the full pipeline (normalization, dedup, detection, entity tracking, UEBA, correlation, SIEM forwarding)
- Augur's `/batch` endpoint skips the pipeline
- The connector sends one event at a time to get full pipeline processing
- Throughput is ~10-50 events/sec which is fine for a SOC (revisit if >100/sec)

### ThreatPulse output

Same event, but potentially different schema transformation. ThreatPulse's ingest endpoint receives events via its own webhook format (TBD — flexible mapping in the connector).

## Delivery Tracking

After successful delivery to all configured hubs, Omn1L1nk marks the event as pushed:

```sql
UPDATE event_outbox
SET pushed = TRUE, pushed_at = NOW()
WHERE id = :event_id
```

If delivery fails:
```sql
UPDATE event_outbox
SET delivery_attempts = delivery_attempts + 1,
    error = :error_message
WHERE id = :event_id
```

Events with `delivery_attempts > 5` are:
- Logged to a dead-letter file (`/var/log/omn1l1nk/dead_letter.log`)
- Kept in the outbox with `pushed = FALSE` for manual inspection
- Alert sent via Omn1L1nk's health endpoint

## Health Endpoint

### `GET /health`

```json
{
  "status": "healthy",
  "queue_depth": 42,
  "oldest_unpushed_seconds": 15,
  "delivery_rate_per_min": 120,
  "error_rate_per_min": 0,
  "last_delivery": "2026-05-29T10:00:00Z",
  "augur_status": "reachable",
  "threatpulse_status": "reachable",
  "n3xusdb_status": "connected"
}
```

### `GET /health/queue`

Detailed view of the event_outbox queue for debugging:

```json
{
  "total_unpushed": 42,
  "by_source": {
    "logsentry": 30,
    "eventflow": 10,
    "alertflow": 2
  },
  "by_severity": {
    "critical": 1,
    "high": 5,
    "medium": 20,
    "low": 16
  },
  "failed": {
    "count": 3,
    "oldest": "2026-05-29T09:30:00Z",
    "sample": ["Connection refused to augur:8001"]
  }
}
```

## API Key Management

API keys are stored in a `daemon_api_keys` table in `shared_db`:

```sql
CREATE TABLE daemon_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    api_key_hash VARCHAR(64) NOT NULL,       -- SHA-256 hash of the key
    label VARCHAR(128) NOT NULL,             -- human-readable name
    source VARCHAR(64) NOT NULL,             -- 'logsentry', 'eventflow', etc.
    source_instance VARCHAR(128) NOT NULL,   -- 'aws-box-01', etc.
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

The raw API key is shown once on creation, then only the hash is stored. Daemon configs store the raw key.

Omn1L1nk validates: `SHA-256(request_key) IN (SELECT api_key_hash FROM daemon_api_keys WHERE enabled = TRUE)`

## Enrichment Rules Config

Stored in `enrich_rules` table in `shared_db` (with a default YAML file for bootstrapping):

```sql
CREATE TABLE enrich_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    match JSONB NOT NULL,         -- match conditions
    enrich JSONB NOT NULL,        -- enrichment actions
    priority INT DEFAULT 100,     -- lower = evaluated first
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

APIs:
- `GET /api/v1/rules` — list all rules
- `POST /api/v1/rules` — create rule
- `PATCH /api/v1/rules/{id}` — update rule
- `DELETE /api/v1/rules/{id}` — delete rule
- `POST /api/v1/rules/reorder` — reorder rule priority

## Deployment

### Docker Compose

```yaml
services:
  omn1l1nk:
    build: .
    container_name: omn1l1nk
    restart: unless-stopped
    ports:
      - "127.0.0.1:9000:9000"
    environment:
      OMN1L1NK_DB_URL: postgresql+asyncpg://omn1l1nk_user:pass@localhost:5432/shared_db
      OMN1L1NK_AUGUR_URL: http://augur:8001
      OMN1L1NK_AUGUR_ENABLED: "true"
      OMN1L1NK_THREATPULSE_URL: http://threatpulse:8081
      OMN1L1NK_THREATPULSE_ENABLED: "false"
      OMN1L1NK_POLL_INTERVAL: "3"
      OMN1L1NK_BATCH_SIZE: "50"
      OMN1L1NK_MAX_RETRIES: "5"
    networks:
      - n3xus-net
    depends_on:
      n3xusdb:
        condition: service_healthy

networks:
  n3xus-net:
    external: true
```

### Config via environment

| Variable | Default | Description |
|----------|---------|-------------|
| `OMN1L1NK_DB_URL` | — | asyncpg connection string to n3xusDB shared_db |
| `OMN1L1NK_AUGUR_URL` | `http://augur:8001` | Augur hub base URL |
| `OMN1L1NK_AUGUR_ENABLED` | `true` | Enable delivery to Augur |
| `OMN1L1NK_THREATPULSE_URL` | `http://threatpulse:8081` | ThreatPulse base URL |
| `OMN1L1NK_THREATPULSE_ENABLED` | `false` | Enable delivery to ThreatPulse |
| `OMN1L1NK_POLL_INTERVAL` | `3` | Seconds between poll cycles |
| `OMN1L1NK_BATCH_SIZE` | `50` | Max events per poll batch |
| `OMN1L1NK_MAX_RETRIES` | `5` | Max delivery attempts before dead letter |
| `OMN1L1NK_INGEST_API_KEYS` | — | Comma-separated list of valid API keys for the ingest endpoint |

## Integration with Each Project

### LogSentry
- Local: add `outputs.n3xusdb` config — direct DB write to `event_outbox`
- Remote: add `outputs.omn1l1nk` config — POST to `Omn1L1nk:9000/api/v1/ingest`
- Fallback: local SQLite buffer if HTTP fails
- Remove: direct LogSentry → Augur webhook (replaced by Omn1L1nk)

### EventFlow
- Same pattern as LogSentry (n3xusDB output or Omn1L1nk HTTP)
- Events flow through `event_outbox` like all other sources

### AlertFlow
- Same pattern
- AlertFlow's triage results get pushed through Omn1L1nk to Augur

### NetWatch
- Packet alerts, anomaly detections → event_outbox → Omn1L1nk → hubs

## Implementation Phases

```
Phase 1 — Core Pipeline
├── FastAPI project skeleton
├── DB session + event_outbox read
├── Poller background task
├── Augur output (POST /api/v1/events)
├── Delivery tracking (pushed flag, retries)
└── /health endpoint

Phase 2 — HTTP Ingest
├── POST /api/v1/ingest endpoint
├── API key validation (daemon_api_keys table)
├── Write to event_outbox from ingest
└── /health/queue endpoint

Phase 3 — Enrichment
├── enrich_rules table + YAML defaults
├── Rule matching engine
├── Severity/confidence/tag enrichment
└── Rule management API (CRUD + reorder)

Phase 4 — Remote Daemon Deployment
├── Update LogSentry with n3xusdb/omn1l1nk outputs
├── Update EventFlow, AlertFlow, NetWatch
├── Local SQLite buffer on remote machines
└── Bootstrap scripts for new machines
```

## Tech Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Language | Python 3.11+ | Same stack as all SOC projects |
| Framework | FastAPI | Async, Pydantic validation, auto-docs |
| DB driver | asyncpg | Async PostgreSQL, high performance |
| HTTP client | httpx | Async HTTP for hub delivery |
| Config | pydantic-settings | Env var validation |
| Deployment | Docker Compose | Consistent with rest of stack |
