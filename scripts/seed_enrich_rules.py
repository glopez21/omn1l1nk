#!/usr/bin/env python3
"""Sync enrich_rules/default.yaml into n3xusDB's enrich_rules table.

Usage:
    python scripts/seed_enrich_rules.py [--db-url POSTGRESQL_URL] [--yaml PATH]

Environment:
    OMN1L1NK_DB_URL  (default: postgresql+asyncpg://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db)

Inserts or replaces rules matched by name. Priority is set from list order (0-based).
"""

import argparse
import asyncio
import json
import os
import sys

import asyncpg
import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


RULE_FIELDS = ("name", "match", "enrich")


async def seed_rules(db_url: str, yaml_path: str) -> int:
    with open(yaml_path) as f:
        data = yaml.load(f, Loader=SafeLoader)

    rules = data.get("rules", [])
    if not rules:
        print("No rules found in YAML file.")
        return 0

    conn = await asyncpg.connect(db_url)
    try:
        count = 0
        for idx, rule in enumerate(rules):
            missing = [f for f in RULE_FIELDS if f not in rule]
            if missing:
                print(f"Skipping rule at index {idx} — missing fields: {missing}")
                continue

            await conn.execute(
                """INSERT INTO enrich_rules (name, match, enrich, priority)
                   VALUES ($1, $2::jsonb, $3::jsonb, $4)
                   ON CONFLICT (name) DO UPDATE
                       SET match = EXCLUDED.match,
                           enrich = EXCLUDED.enrich,
                           priority = EXCLUDED.priority,
                           updated_at = NOW()""",
                rule["name"],
                json.dumps(rule["match"]),
                json.dumps(rule["enrich"]),
                idx,
            )
            count += 1

        return count
    finally:
        await conn.close()


def _parse_args():
    parser = argparse.ArgumentParser(description="Seed enrichment rules from YAML into n3xusDB")
    parser.add_argument("--db-url", help="PostgreSQL connection URL (raw asyncpg format)")
    parser.add_argument("--yaml", dest="yaml_path", default="enrich_rules/default.yaml",
                        help="Path to YAML rules file")
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
    count = await seed_rules(db_url, args.yaml_path)
    print(f"Seeded {count} enrichment rule(s) from {args.yaml_path}")


if __name__ == "__main__":
    asyncio.run(main())
