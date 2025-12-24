from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg
from eclipse24.libs.core.logging import log_event

_PG_DSN = os.getenv("PG_DSN", "postgresql://sentinel:sentinel@localhost:5432/sentinel")

_pool: Optional[asyncpg.Pool] = None


async def init_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=_PG_DSN, min_size=1, max_size=5)
        log_event("pg", "pool_started", {"dsn": _PG_DSN})


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log_event("pg", "pool_closed")


@asynccontextmanager
async def conn():
    if _pool is None:
        await init_pool()
    assert _pool is not None
    async with _pool.acquire() as c:
        yield c


async def execute(sql: str, *args) -> str:
    async with conn() as c:
        return await c.execute(sql, *args)


async def fetch(sql: str, *args) -> list[asyncpg.Record]:
    async with conn() as c:
        return await c.fetch(sql, *args)


async def fetchrow(sql: str, *args) -> Optional[asyncpg.Record]:
    async with conn() as c:
        return await c.fetchrow(sql, *args)


async def insert_extension(
        server_id: str,
        owner_email: Optional[str],
        granted_by: str,
        granted_by_uid: str,
        hours: int,
        expires_at_iso: str,
):
    sql = """
          insert into extensions(server_id, owner_email, granted_by, granted_by_uid, hours, status, expires_at)
          values ($1, $2, $3, $4, $5, 'ACTIVE', $6::timestamptz) \
          """
    await execute(sql, server_id, owner_email, granted_by, granted_by_uid, hours, expires_at_iso)


async def save_decision(
        server_id: str,
        action: str,
        actor_uid: str,
        actor_name: str,
        extra: Optional[dict[str, Any]] = None,
):
    sql = """
          insert into decisions(server_id, action, actor_uid, actor_name, extra)
          values ($1, $2, $3, $4, $5::jsonb) \
          """
    await execute(sql, server_id, action, actor_uid, actor_name, extra or {})


async def upsert_waiting_state(
        server_id: str,
        *,
        posted_at: Optional[str] = None,
        extend_requested_at: Optional[str] = None,
        acted_at: Optional[str] = None,
        acted_by: Optional[str] = None,
        action: Optional[str] = None,
        expires_at: Optional[str] = None,
):
    sql = """
          insert into waiting_state(server_id, posted_at, extend_requested_at, acted_at, acted_by, action, expires_at)
          values ($1, $2::timestamptz, $3::timestamptz, $4::timestamptz, $5, $6,
                  $7::timestamptz) on conflict(server_id) do
          update set
              posted_at = coalesce (excluded.posted_at, waiting_state.posted_at),
              extend_requested_at = coalesce (excluded.extend_requested_at, waiting_state.extend_requested_at),
              acted_at = coalesce (excluded.acted_at, waiting_state.acted_at),
              acted_by = coalesce (excluded.acted_by, waiting_state.acted_by),
              action = coalesce (excluded.action, waiting_state.action),
              expires_at = coalesce (excluded.expires_at, waiting_state.expires_at),
              updated_at = now() \
          """
    await execute(sql, server_id, posted_at, extend_requested_at, acted_at, acted_by, action, expires_at)
