# src/eclipse24/services/blocker/main.py
import asyncio
from typing import Any, Dict, List, Optional

import httpx
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient, KafkaRecord
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.stores.db import record_audit
SLACK = AsyncWebClient(token=settings.SLACK_BOT_TOKEN)

BLOCK_TOPIC = topic_for("block")

STORE_BASE = settings.STORE_API_URL.rstrip("/")
STORE_USER = settings.STORE_USER
STORE_PASSWORD = settings.STORE_PASSWORD

DRY_RUN = bool(getattr(settings, "DRY_RUN", False))

# DB toggle for audit
PERSIST = bool(settings.PERSIST_TO_DB and settings.PG_DSN)


async def _notify_channel(text: str):
    try:
        await SLACK.chat_postMessage(channel=settings.SLACK_APPROVAL_CHANNEL, text=text)
    except SlackApiError as e:
        log_event("blocker", "slack_channel_err", {"error": str(e)})


async def _notify_owner(email: Optional[str], text: str):
    if not email:
        return
    try:
        resp = await SLACK.users_lookupByEmail(email=email)
        if resp.get("ok") and resp["user"]["id"]:
            dm = await SLACK.conversations_open(users=[resp["user"]["id"]])
            if dm.get("ok"):
                await SLACK.chat_postMessage(channel=dm["channel"]["id"], text=text)
    except SlackApiError as e:
        log_event("blocker", "slack_dm_err", {"error": str(e)})


async def _fetch_topology(technical_servername: str) -> List[Dict[str, Any]]:

    if not (STORE_USER and STORE_PASSWORD):
        log_event("blocker", "store_missing_creds", {})
        return []

    url = f"{STORE_BASE}/dashboard/api/enterprise_plus/topology/{technical_servername}"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get(
                url,
                auth=(STORE_USER, STORE_PASSWORD),
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            log_event("blocker", "store_fetch_error", {"url": url, "error": str(e)})
            return []


def _pick_row(rows: List[Dict[str, Any]], account_number: Optional[str | int]) -> Optional[Dict[str, Any]]:

    if not rows:
        return None
    if account_number is not None:
        try:
            acc_int = int(account_number)
        except Exception:
            acc_int = None
        if acc_int is not None:
            for row in rows:
                try:
                    if int(row.get("accountNumber", -1)) == acc_int:
                        return row
                except Exception:
                    continue
    return rows[0]


async def _lookup_account_number(server: Dict[str, Any]) -> Optional[int]:

    # 1) value-provided
    acc = server.get("account_number")
    if acc:
        try:
            return int(acc)
        except Exception:
            pass

    # 2) topology lookup
    technical = (
            server.get("technicalServerName")
            or server.get("server_id")
            or server.get("serverName")
    )
    if not technical:
        return None

    rows = await _fetch_topology(technical)
    row = _pick_row(rows, account_number=None)
    if not row:
        return None
    try:
        return int(row.get("accountNumber"))
    except Exception:
        return None


async def _deactivate_account(account_number: int) -> tuple[bool, str]:

    if not (STORE_USER and STORE_PASSWORD):
        return False, "Store credentials not configured"

    url = f"{STORE_BASE}/dashboard/api/accounts/{account_number}/deactivate_jfrog_subscription"

    if DRY_RUN:
        log_event("blocker", "dry_run", {"deactivate_url": url})
        return True, "DRY_RUN: skipped deactivate call"

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(url, auth=(STORE_USER, STORE_PASSWORD))
            if r.status_code // 100 == 2:
                return True, f"Store responded {r.status_code}"
            return False, f"Store responded {r.status_code}: {r.text[:500]}"
        except Exception as e:
            return False, f"HTTP error: {e}"


async def handle_block(rec: KafkaRecord):
    v = rec.value if isinstance(rec.value, dict) else {}
    server = v.get("server") or {}
    sid = (
            server.get("server_id")
            or server.get("technicalServerName")
            or server.get("serverName")
            or "unknown"
    )
    actor = v.get("approved_by") or v.get("denied_by") or "system"
    reason = v.get("reason") or "block"
    owner_email = server.get("owner_email")

    log_event("blocker", "recv", {"server": sid, "reason": reason})

    if PERSIST and sid != "unknown":
        try:
            await record_audit(sid, "block_start", actor, {"reason": reason})
        except Exception:
            pass

    # 1) Find accountNumber
    acc = server.get("account_number")
    if acc is None:
        acc = await _lookup_account_number(server)

    if acc is None:
        msg = (
            f"Block requested for `{sid}` by *{actor}* (reason: {reason}) "
            "— **FAILED**: could not determine accountNumber."
        )
        await _notify_channel(msg)
        await _notify_owner(owner_email, msg)
        log_event("blocker", "missing_account", {"server": sid})

        if PERSIST and sid != "unknown":
            try:
                await record_audit(
                    sid,
                    "block_failed_no_account",
                    actor,
                    {"reason": reason},
                )
            except Exception:
                pass
        return

    # 2) Deactivate in Store
    ok, detail = await _deactivate_account(int(acc))

    # 3) Notify Slack + audit
    if ok:
        msg = (
            f"Server `{sid}` (account `{acc}`) has been *blocked* "
            f"(reason: {reason}). Triggered by *{actor}*. {detail}"
        )
        await _notify_channel(msg)
        await _notify_owner(owner_email, msg)
        log_event("blocker", "done", {"server": sid, "account": acc})

        if PERSIST and sid != "unknown":
            try:
                await record_audit(
                    sid,
                    "blocked",
                    actor,
                    {
                        "account": int(acc),
                        "detail": detail,
                        "reason": reason,
                        "dry_run": DRY_RUN,
                    },
                )
            except Exception:
                pass
    else:
        msg = (
            f"⚠️ Block attempt for `{sid}` (account `{acc}`) by *{actor}* failed: {detail}"
        )
        await _notify_channel(msg)
        await _notify_owner(owner_email, msg)
        log_event("blocker", "store_error", {"server": sid, "account": acc, "detail": detail})

        if PERSIST and sid != "unknown":
            try:
                await record_audit(
                    sid,
                    "block_failed_store_error",
                    actor,
                    {
                        "account": int(acc),
                        "detail": detail,
                        "reason": reason,
                        "dry_run": DRY_RUN,
                    },
                )
            except Exception:
                pass


async def main():
    bus = await KafkaClient().connect()
    await bus.consume(
        BLOCK_TOPIC,
        handle_block,
        group_id=f"{settings.KAFKA_GROUP_ID_PREFIX}.blocker",
        auto_offset_reset="earliest",
    )


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()