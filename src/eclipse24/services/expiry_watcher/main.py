# src/eclipse24/services/expiry_watcher/main.py

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event, log_error
from eclipse24.libs.messaging.kafka_client import KafkaClient
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.stores.db import (
    save_server,
    set_state,
    upsert_extension,
    record_action,
    record_audit,
)
from eclipse24.services.approver.main import build_owner_card_blocks

EXTENSIONS_TOPIC = topic_for("extensions")
BLOCK_TOPIC = topic_for("block")
WAITING_STATE_TOPIC = topic_for("waiting_state")

PERSIST = bool(settings.PERSIST_TO_DB and settings.PG_DSN)

slack_client = AsyncWebClient(token=settings.SLACK_BOT_TOKEN)

_RULES_BOOT = settings.load_rules() or {}
_CONFIG_BOOT = _RULES_BOOT.get("config") or {}
_SLACK_CFG = _CONFIG_BOOT.get("slack") or {}

_RULE_PRIMARY_CHANNEL = _SLACK_CFG.get("approval_channel")
_RULE_FALLBACK_CHANNEL = _SLACK_CFG.get("fallback_channel")

_ENV_PRIMARY = getattr(settings, "SLACK_APPROVAL_CHANNEL", None)
_ENV_FALLBACK = getattr(settings, "SLACK_FALLBACK_CHANNEL", None)

PRIMARY_CHANNEL = _RULE_PRIMARY_CHANNEL or _ENV_PRIMARY or "#eclipse24-approvals"
FALLBACK_CHANNEL = _RULE_FALLBACK_CHANNEL or _ENV_FALLBACK or PRIMARY_CHANNEL

SCAN_INTERVAL_SECONDS = 60

STATE: Dict[str, Dict[str, Any]] = {}


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _apply_exemptions(exemptions: Dict[str, Any], server_name: str, owner_email: Optional[str]) -> bool:

    if server_name in (exemptions.get("servers") or []):
        return True
    if owner_email and owner_email in (exemptions.get("emails") or []):
        return True
    return False


async def handle_extension(msg: Dict[str, Any]):

    server = msg.get("server") or {}
    server_id = server.get("server_id")
    if not server_id:
        log_event("expiry", "skip", {"reason": "no_server_id"})
        return

    expires_at_iso = msg.get("expires_at")
    expires_at = _parse_iso(expires_at_iso) if expires_at_iso else None
    hours = int(msg.get("extension_hours") or 72)
    by = msg.get("granted_by")
    by_uid = msg.get("granted_by_uid")

    st = STATE.get(server_id, {})
    st.update(
        {
            "server_id": server_id,
            "server": server,
            "expires_at": expires_at_iso,
            "extension_hours": hours,
            "granted_by": by,
            "granted_by_uid": by_uid,
        }
    )
    STATE[server_id] = st

    if not PERSIST:

        log_event(
            "expiry",
            "tracked_no_persist",
            {
                "server": server_id,
                "expires_at": expires_at_iso,
                "hours": hours,
                "granted_by": by,
                "granted_by_uid": by_uid,
            },
        )
        return

    # === DB path (best-effort; failures do NOT affect enforcement) ===
    try:
        await save_server(server)
        if expires_at:
            await set_state(server_id, "EXTENDED", expires_at)
        await upsert_extension(
            server_id,
            hours,
            expires_at or _now(),
            by_uid,
            by,
            )
        await record_action(
            server_id,
            "extend_approved_track",
            by_uid,
            by,
            {"expires_at": expires_at_iso},
        )
        await record_audit(
            server_id,
            "extend_approved_track",
            by or "",
            {"by_uid": by_uid, "expires_at": expires_at_iso},
            )
        log_event("expiry", "tracked", {"server": server_id, "expires_at": expires_at_iso})
    except Exception as e:
        log_error("expiry", e, {"where": "persist_extension", "server": server_id})


async def _on_state(record):

    try:
        value = record.value if isinstance(record.value, dict) else json.loads(record.value)
        if not isinstance(value, dict):
            log_event("expiry", "state_non_dict", {"type": str(type(value))})
            return

        sid = value.get("server_id")
        if not sid:
            return

        prev = STATE.get(sid, {})
        prev.update(value)
        STATE[sid] = prev

        log_event(
            "expiry",
            "state_update",
            {"server": sid, "keys": list(value.keys())},
        )
    except Exception as e:
        log_error("expiry", e, {"where": "state_handler"})


async def _send_pre_expiry_notification(
        server_id: str,
        fields: Dict[str, Any],
        text: str,
):
    """
    Pre-expiry notice:
    """
    server = fields.get("server") or {"server_id": server_id}
    owner_uid: Optional[str] = fields.get("owner_slack_id")
    origin_channel: Optional[str] = fields.get("origin_channel")
    expires_at = fields.get("expires_at")

    status_line = text  # already formatted with {server_id}, {expires_at}
    log_event(
        "expiry_notify",
        "pre_notice_attempt",
        {"server": server_id, "owner_uid": owner_uid, "origin_channel": origin_channel},
    )

    # Try DM to owner if possible
    if owner_uid:
        try:
            dm_open = await slack_client.conversations_open(users=owner_uid)
            if dm_open.get("ok"):
                ch = dm_open["channel"]["id"]
                await slack_client.chat_postMessage(
                    channel=ch,
                    text=text,
                    blocks=build_owner_card_blocks(
                        server,
                        status_line=status_line,
                        with_buttons=False,
                        value_envelope={"server": server},
                    ),
                )
                log_event(
                    "expiry_notify",
                    "pre_notice_owner_dm_ok",
                    {"server": server_id, "owner": owner_uid, "channel": ch, "expires_at": expires_at},
                )
                return
        except SlackApiError as e:
            log_event(
                "expiry_notify",
                "pre_notice_owner_dm_error",
                {"server": server_id, "owner": owner_uid, "error": str(e)},
            )

    target_channel = origin_channel or FALLBACK_CHANNEL

    try:
        await slack_client.chat_postMessage(
            channel=target_channel,
            text=text,
            blocks=build_owner_card_blocks(
                server,
                status_line=status_line,
                with_buttons=False,
                value_envelope={"server": server},
            ),
        )
        log_event(
            "expiry_notify",
            "pre_notice_channel_ok",
            {"server": server_id, "channel": target_channel, "expires_at": expires_at},
        )
    except SlackApiError as e:
        log_event(
            "expiry_notify",
            "pre_notice_channel_error",
            {"server": server_id, "channel": target_channel, "error": str(e)},
        )


async def _send_expiry_notification(server_id: str, fields: Dict[str, Any]):

    server = fields.get("server") or {"server_id": server_id}
    owner_uid: Optional[str] = fields.get("owner_slack_id")
    origin_channel: Optional[str] = fields.get("origin_channel")
    expires_at = fields.get("expires_at")

    status_line = (
        f"⏰ Extension expired for `{server_id}`. "
        f"Server will be blocked (expiry reached `{expires_at}`)."
    )
    text = f"⏰ Extension expired for `{server_id}` – server is being blocked."

    # Try DM to owner if we have a Slack ID
    if owner_uid:
        try:
            dm_open = await slack_client.conversations_open(users=owner_uid)
            if dm_open.get("ok"):
                ch = dm_open["channel"]["id"]
                await slack_client.chat_postMessage(
                    channel=ch,
                    text=text,
                    blocks=build_owner_card_blocks(
                        server,
                        status_line=status_line,
                        with_buttons=False,
                        value_envelope={"server": server},
                    ),
                )
                log_event(
                    "expiry_notify",
                    "owner_dm_ok",
                    {"server": server_id, "owner": owner_uid, "channel": ch},
                )
                return
        except SlackApiError as e:
            log_event(
                "expiry_notify",
                "owner_dm_error",
                {"server": server_id, "owner": owner_uid, "error": str(e)},
            )

    # Fallback to origin channel if known, else to global fallback channel
    target_channel = origin_channel or FALLBACK_CHANNEL
    try:
        await slack_client.chat_postMessage(
            channel=target_channel,
            text=text,
            blocks=build_owner_card_blocks(
                server,
                status_line=status_line,
                with_buttons=False,
                value_envelope={"server": server},
            ),
        )
        log_event(
            "expiry_notify",
            "fallback_channel_ok",
            {"server": server_id, "channel": target_channel},
        )
    except SlackApiError as e:
        log_event(
            "expiry_notify",
            "fallback_channel_error",
            {"server": server_id, "channel": target_channel, "error": str(e)},
        )


async def _scan_and_enforce(bus_state: KafkaClient):

    now = _now()

    # Reload rules on every scan to keep exemptions + expiry config fresh
    rules = settings.load_rules() or {}
    config = rules.get("config") or {}
    exemptions = config.get("exemptions") or {}
    expiry_cfg = config.get("expiry") or {}
    messages_cfg = config.get("messages") or {}

    # notice_before_hours can be 0 (disable) or a small value for testing (e.g., 0.0166667)
    try:
        notice_before_hours = float(expiry_cfg.get("notice_before_hours", 0.0))
    except Exception:
        notice_before_hours = 0.0

    notice_delta = timedelta(hours=notice_before_hours) if notice_before_hours > 0 else None

    # Pre-notice message template
    pre_notice_template = messages_cfg.get(
        "expiry_pre_notice",
        "⏳ Your server *{server_id}* will expire at *{expires_at}*. "
        "If you still need this environment, please request SRE Support.",
    )

    for sid, fields in list(STATE.items()):
        expires_at_str = fields.get("expires_at")
        if not expires_at_str:
            continue

        expires_at = _parse_iso(expires_at_str)
        if not expires_at:
            continue

        server = fields.get("server") or {"server_id": sid}
        owner_email = server.get("owner_email")

        # 0) Exemptions check (fresh every scan)
        if _apply_exemptions(exemptions, sid, owner_email):
            log_event(
                "expiry",
                "exempted_skip",
                {"server": sid, "owner_email": owner_email},
            )
            # We intentionally do NOT block or notify exempted servers
            continue

        # 1) Pre-expiry notice (if window configured and not already sent)
        if (
                notice_delta is not None
                and not fields.get("expiry_pre_notice_sent")
                and now < expires_at
                and now + notice_delta >= expires_at
        ):
            # Format message from template
            text = pre_notice_template.format(server_id=sid, expires_at=expires_at_str)

            await _send_pre_expiry_notification(sid, fields, text)

            # Mark that pre-notice was sent so we don't spam
            new_state_pre = {
                "server_id": sid,
                "expiry_pre_notice_sent": True,
                "expiry_pre_notice_at": now.isoformat(),
            }
            try:
                await bus_state.produce(WAITING_STATE_TOPIC, new_state_pre, key=sid)
                log_event(
                    "expiry",
                    "state_mark_pre_notice",
                    {"server": sid, "topic": WAITING_STATE_TOPIC},
                )
            except Exception as e:
                log_error("expiry", e, {"where": "mark_pre_notice", "server": sid})

            # We still continue to check eventual expiry in same loop (in case window is tiny),
            # but normally expires_at should be significantly after this point.

        # 2) Expiry → move to BLOCK_TOPIC (if not already processed)
        if fields.get("expired_block_emitted"):
            continue

        if now < expires_at:
            continue  # not yet expired

        # Build block event payload
        block_payload = {
            "server": server,
            "reason": "expiry",
            "expired_at": now.isoformat(),
        }

        # 2.a) Produce block event
        try:
            await bus_state.produce(BLOCK_TOPIC, block_payload, key=sid)
            log_event(
                "expiry",
                "block_emitted",
                {"server": sid, "expired_at": now.isoformat(), "topic": BLOCK_TOPIC},
            )
        except Exception as e:
            # If block event fails to go to Kafka, do NOT mark as emitted:
            # we'll retry on next scan.
            log_error("expiry", e, {"where": "emit_block", "server": sid})
            continue

        # 2.b) Notify via Slack about actual block
        await _send_expiry_notification(sid, fields)

        # 2.c) Mark in waiting.state so other services see it's already processed
        new_state_expired = {
            "server_id": sid,
            "expired_block_emitted": True,
            "expired_at": now.isoformat(),
        }

        try:
            await bus_state.produce(WAITING_STATE_TOPIC, new_state_expired, key=sid)
            log_event(
                "expiry",
                "state_mark_expired",
                {"server": sid, "topic": WAITING_STATE_TOPIC},
            )
        except Exception as e:
            log_error("expiry", e, {"where": "mark_expired", "server": sid})


async def main():
    group_ext = f"{settings.KAFKA_GROUP_ID_PREFIX}.expiry_watcher"
    group_state = f"{settings.KAFKA_GROUP_ID_PREFIX}.expiry_state"

    bus_ext = await KafkaClient().connect()
    bus_state_consume = await KafkaClient().connect()
    bus_state_rw = await KafkaClient().connect()

    async def _ext_handler(record):
        try:
            payload = (
                record.value
                if isinstance(record.value, dict)
                else json.loads(record.value)
            )
            await handle_extension(payload)
        except Exception as e:
            log_error("expiry", e, {"where": "ext_handler"})

    async def _state_handler(record):
        await _on_state(record)

    # 1) Consume EXTENSIONS_TOPIC → track extensions (DB + STATE)
    asyncio.create_task(
        bus_ext.consume(
            topic=EXTENSIONS_TOPIC,
            group_id=group_ext,
            handler=_ext_handler,
            auto_offset_reset="earliest",
        )
    )

    # 2) Consume WAITING_STATE_TOPIC → maintain STATE
    asyncio.create_task(
        bus_state_consume.consume(
            topic=WAITING_STATE_TOPIC,
            group_id=group_state,
            handler=_state_handler,
            auto_offset_reset="earliest",
        )
    )

    log_event(
        "expiry",
        "start",
        {
            "extensions_topic": EXTENSIONS_TOPIC,
            "waiting_state_topic": WAITING_STATE_TOPIC,
            "block_topic": BLOCK_TOPIC,
            "group_ext": group_ext,
            "group_state": group_state,
            "persist_to_db": PERSIST,
            "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
        },
    )

    # 3) Periodically scan for expiries and enforce
    while True:
        await _scan_and_enforce(bus_state_rw)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()