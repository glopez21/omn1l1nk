#!/usr/bin/env python3
"""Generate and insert an API key into n3xusDB's daemon_api_keys table.

Usage:
    python scripts/seed_api_key.py [--db-url POSTGRESQL_URL]

Environment:
    OMN1L1NK_DB_URL  (default: postgresql+asyncpg://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db)

The script strips the +asyncpg driver prefix to get a raw asyncpg URL.
"""

import argparse
import asyncio
import hashlib
import os
import secrets
import sys

import asyncpg


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def seed_api_key(db_url: str, label: str, source: str, source_instance: str) -> str:
    raw_key = secrets.token_urlsafe(32)
    key_hash = _hash_key(raw_key)

    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            """INSERT INTO daemon_api_keys (api_key_hash, label, source, source_instance)
               VALUES ($1, $2, $3, $4)""",
            key_hash,
            label,
            source,
            source_instance,
        )
    finally:
        await conn.close()

    return raw_key


def _parse_args():
    parser = argparse.ArgumentParser(description="Seed an API key in n3xusDB")
    parser.add_argument("--db-url", help="PostgreSQL connection URL (raw asyncpg format)")
    parser.add_argument("--label", default="dev-key", help="Human-readable label for the key")
    parser.add_argument("--source", default="logsentry", help="Daemon source name (logsentry, eventflow, alertflow, netwatch)")
    parser.add_argument("--instance", default="dev-box", help="Source instance identifier")
    return parser.parse_args()


def _resolve_db_url(cli_url: str | None) -> str:
    if cli_url:
        return cli_url
    env_url = os.environ.get("OMN1L1NK_DB_URL", "")
    if env_url:
        return env_url.replace("postgresql+asyncpg://", "postgresql://")
    return "postgresql://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db"


async def main():
    args = _parse_args()
    db_url = _resolve_db_url(args.db_url)
    raw_key = await seed_api_key(db_url, args.label, args.source, args.instance)
    print(raw_key)


if __name__ == "__main__":
    asyncio.run(main())
