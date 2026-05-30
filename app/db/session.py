import asyncpg
from app.config import settings


def _dsn_from_url(url: str) -> str:
    url = url.replace("postgresql+asyncpg://", "postgresql://")
    return url.replace("+asyncpg", "")


class Pool:
    _pool: asyncpg.Pool | None = None

    @classmethod
    async def connect(cls):
        dsn = _dsn_from_url(settings.db_url)
        cls._pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    async def fetch(cls, query: str, *args):
        if not cls._pool:
            raise RuntimeError("pool not connected")
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query: str, *args):
        if not cls._pool:
            raise RuntimeError("pool not connected")
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    @classmethod
    async def execute(cls, query: str, *args):
        if not cls._pool:
            raise RuntimeError("pool not connected")
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)
