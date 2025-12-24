# src/eclipse24/libs/stores/db.py
import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

import asyncpg
from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


# ----------------------------- pool -----------------------------
async def get_pool(dsn: Optional[str] = None) -> asyncpg.Pool:
    global _pool
    if _pool:
        return _pool
    async with _pool_lock:
        if _pool:
            return _pool
        dsn = dsn or settings.PG_DSN
        if not dsn:
            raise RuntimeError("PG_DSN not configured")
        try:
            _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5, command_timeout=60)
            log_event("db", "pool_ready", {"dsn": dsn})
        except Exception as e:
            log_event("db", "pool_error", {"error": str(e)})
            raise
        return _pool


async def close_pool():
    global _pool
    if _pool:
        try:
            await _pool.close()
            log_event("db", "pool_closed")
        finally:
            _pool = None


# -------------------------- helpers -----------------------------
def _to_ts(val: Any) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return None


# -------------------------- servers -----------------------------
async def save_server(server: Dict[str, Any]) -> None:
    pool = await get_pool()
    sid = server.get("server_id")
    if not sid:
        return
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO servers (server_id, server_name, owner_email, region, status, creation_date, last_seen_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                    ON CONFLICT (server_id) DO UPDATE
                                                   SET server_name=EXCLUDED.server_name,
                                                   owner_email=EXCLUDED.owner_email,
                                                   region=EXCLUDED.region,
                                                   status=EXCLUDED.status,
                                                   creation_date=EXCLUDED.creation_date,
                                                   last_seen_at=now()
                """,
                sid,
                server.get("server_name"),
                server.get("owner_email"),
                server.get("region"),
                server.get("status"),
                _to_ts(server.get("creation_date")),
            )
        log_event("db", "save_server_ok", {"server_id": sid})
    except Exception as e:
        log_event("db", "save_server_err", {"server_id": sid, "error": str(e)})
        raise


# -------------------------- state -------------------------------
async def set_state(server_id: str, status: str, until: Optional[datetime]) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO server_state (server_id, status, until, updated_at)
                VALUES ($1, $2, $3, now())
                    ON CONFLICT (server_id) DO UPDATE
                                                   SET status=EXCLUDED.status,
                                                   until=EXCLUDED.until,
                                                   updated_at=now()
                """,
                server_id,
                status.upper(),
                until,
            )
        log_event(
            "db",
            "set_state_ok",
            {"server_id": server_id, "status": status, "until": (until.isoformat() if until else None)},
        )
    except Exception as e:
        log_event(
            "db",
            "set_state_err",
            {
                "server_id": server_id,
                "status": status,
                "until": (until.isoformat() if until else None),
                "error": str(e),
            },
        )
        raise


async def get_state(server_id: str) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            row = await c.fetchrow(
                "SELECT status, until FROM server_state WHERE server_id=$1",
                server_id,
            )
        return None if not row else {"status": row["status"], "until": row["until"]}
    except Exception as e:
        log_event("db", "get_state_err", {"server_id": server_id, "error": str(e)})
        return None


# -------------------------- actions/audit -----------------------
async def record_action(
        server_id: str,
        action: str,
        actor_uid: Optional[str],
        actor_name: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO actions(server_id, action, actor_uid, actor_name, meta, created_at)
                VALUES ($1, $2, $3, $4, $5, now())
                """,
                server_id,
                action,
                actor_uid,
                actor_name,
                (meta or {}),
            )
        log_event("db", "action_ok", {"server_id": server_id, "action": action})
    except Exception as e:
        log_event("db", "action_err", {"server_id": server_id, "action": action, "error": str(e)})


async def record_audit(
        server_id: str,
        action: str,
        actor: str,
        details: Optional[Dict[str, Any]] = None,
) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO audit_log(server_id, action, actor, details, created_at)
                VALUES ($1, $2, $3, $4, now())
                """,
                server_id,
                action,
                actor,
                (details or {}),
            )
        log_event("db", "audit_ok", {"server_id": server_id, "action": action})
    except Exception as e:
        log_event("db", "audit_err", {"server_id": server_id, "action": action, "error": str(e)})


# -------------------------- extensions -------------------------
async def upsert_extension(
        server_id: str,
        hours: int,
        expires_at: datetime,
        granted_by_uid: Optional[str],
        granted_by: Optional[str],
) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO extensions(server_id, extension_hours, expires_at, granted_by_uid, granted_by, updated_at)
                VALUES ($1, $2, $3, $4, $5, now())
                    ON CONFLICT (server_id) DO UPDATE
                                                   SET extension_hours=EXCLUDED.extension_hours,
                                                   expires_at=EXCLUDED.expires_at,
                                                   granted_by_uid=EXCLUDED.granted_by_uid,
                                                   granted_by=EXCLUDED.granted_by,
                                                   updated_at=now()
                """,
                server_id,
                hours,
                expires_at,
                granted_by_uid,
                granted_by,
            )
        log_event("db", "extension_ok", {"server_id": server_id, "expires_at": expires_at.isoformat()})
    except Exception as e:
        log_event("db", "extension_err", {"server_id": server_id, "error": str(e)})


async def has_ever_extended(server_id: str) -> bool:
    """
    True if server has *ever* received an extension.
    We treat existence of a row in `extensions` as the source of truth.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            row = await c.fetchrow(
                "SELECT 1 FROM extensions WHERE server_id=$1 LIMIT 1",
                server_id,
            )
        return bool(row)
    except Exception as e:
        log_event("db", "has_ever_extended_err", {"server_id": server_id, "error": str(e)})
        return False


# -------------------------- reminders --------------------------
async def touch_reminder(server_id: str, when: Optional[datetime] = None) -> None:
    pool = await get_pool()
    when = when or datetime.now(timezone.utc)
    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO reminders(server_id, last_reminded_at)
                VALUES ($1, $2)
                    ON CONFLICT (server_id) DO UPDATE
                                                   SET last_reminded_at=EXCLUDED.last_reminded_at
                """,
                server_id,
                when,
            )
        log_event("db", "reminder_ok", {"server_id": server_id, "last_reminded_at": when.isoformat()})
    except Exception as e:
        log_event("db", "reminder_err", {"server_id": server_id, "error": str(e)})


# ----------------------- expiry selection ----------------------
async def due_blocks(limit: int = 200) -> List[Tuple[str, datetime]]:
    """
    Return [(server_id, until)] that are EXTENDED and due for block.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as c:
            rows = await c.fetch(
                """
                SELECT server_id, until
                FROM server_state
                WHERE status = 'EXTENDED'
                  AND until IS NOT NULL
                  AND until <= now()
                ORDER BY until ASC
                    LIMIT $1
                """,
                limit,
            )
        return [(r["server_id"], r["until"]) for r in rows]
    except Exception as e:
        log_event("db", "due_blocks_err", {"error": str(e)})
        return []


# ------------------------------ schema init --------------------
async def init_schema(schema_files: List[str]):
    pool = await get_pool()
    for path in schema_files:
        try:
            with open(path, "r") as f:
                sql = f.read()
            async with pool.acquire() as c:
                await c.execute(sql)
            log_event("db", "schema_ok", {"file": path})
        except Exception as e:
            log_event("db", "schema_err", {"file": path, "error": str(e)})


# ------------------------------ CLI ----------------------------
async def _cli_init():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    sql_dir = os.path.join(root, "configs", "sql")
    files = [
        os.path.join(sql_dir, "001_init.sql"),
        os.path.join(sql_dir, "002_state.sql"),
        os.path.join(sql_dir, "003_logging.sql"),
    ]
    log_event("db", "init_start", {"files": files})
    await init_schema(files)
    log_event("db", "init_done", {"status": "ok"})


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--init":
        asyncio.run(_cli_init())
    else:
        print("Usage:\n  python3 -m eclipse24.libs.stores.db --init")