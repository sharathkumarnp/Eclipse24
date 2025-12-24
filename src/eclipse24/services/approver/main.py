# src/eclipse24/services/approver/main.py

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from slack_sdk.errors import SlackApiError

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient, KafkaRecord
from eclipse24.libs.stores.db import (
    save_server,
    record_action,
    record_audit,
    touch_reminder,
    has_ever_extended,
)
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.slack.utils import (
    slack_client,
    build_owner_card_blocks as build_owner_base_blocks,
    _lookup_user_id_by_email,
)

_RULES = settings.load_rules() or {}
_CONFIG = _RULES.get("config") or {}
_MESSAGES = _CONFIG.get("messages") or {}
_NOTICE = _MESSAGES.get("notice")
_SLACK_USER_NOT_FOUND = _MESSAGES.get("slack_user_not_found")

_SLACK_CFG = _CONFIG.get("slack") or {}

_RULE_PRIMARY_CHANNEL = _SLACK_CFG.get("approval_channel")
_RULE_FALLBACK_CHANNEL = _SLACK_CFG.get("fallback_channel")

_ENV_PRIMARY = getattr(settings, "SLACK_APPROVAL_CHANNEL", None)
_ENV_FALLBACK = getattr(settings, "SLACK_FALLBACK_CHANNEL", None)

PRIMARY_CHANNEL = _RULE_PRIMARY_CHANNEL or _ENV_PRIMARY or "#eclipse24-approvals"
FALLBACK_CHANNEL = _RULE_FALLBACK_CHANNEL or _ENV_FALLBACK or PRIMARY_CHANNEL

WAITING_TOPIC = topic_for("waiting")
WAITING_STATE_TOPIC = topic_for("waiting_state")
AUDIT_TOPIC = topic_for("audit")

PERSIST = bool(settings.PERSIST_TO_DB and settings.PG_DSN)


def _safe_json(o: Any) -> str:
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _notice_text(server_id: str) -> str:
    if isinstance(_NOTICE, str) and _NOTICE.strip():
        try:
            return _NOTICE.format(server_id=server_id)
        except Exception:
            return _NOTICE
    return f"Eclipse24 wants to BLOCK server {server_id}"


def _owner_card_blocks_with_actions(
        server: Dict[str, Any],
        status_line: Optional[str] = None,
        with_buttons: bool = True,
        value_envelope: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    blocks = build_owner_base_blocks(server, status_line=status_line)
    if not with_buttons:
        return blocks
    value = value_envelope or {"server": server}
    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Approve"},
                    "style": "primary",
                    "action_id": "eclipse24_approve",
                    "value": _safe_json(value),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⏳ Extend 3 days"},
                    "action_id": "eclipse24_extend_3d",
                    "value": _safe_json(value),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⏳ Extend 2 weeks"},
                    "action_id": "eclipse24_extend_2w",
                    "value": _safe_json(value),
                },
            ],
        }
    )
    return blocks


async def _open_dm(user_id: str) -> Optional[str]:
    try:
        open_resp = await slack_client.conversations_open(users=user_id)
        if open_resp.get("ok"):
            return open_resp["channel"]["id"]
    except SlackApiError as e:
        log_event("slack_dm_open", "error", {"user_id": user_id, "error": str(e)})
    return None


async def _mark_waiting_state(server_id: str, **fields):
    bus = await KafkaClient().connect()
    try:
        payload = {"server_id": server_id}
        payload.update(fields)
        await bus.produce(WAITING_STATE_TOPIC, payload, key=server_id)
        log_event(
            "kafka_compacted_state",
            "ok",
            {
                "topic": WAITING_STATE_TOPIC,
                "server_id": server_id,
                "keys": list(fields.keys()),
            },
        )
    finally:
        await bus.close()


async def _produce_audit(
        action: str, server: Dict[str, Any], extra: Optional[Dict[str, Any]] = None
):
    bus = await KafkaClient().connect()
    try:
        payload: Dict[str, Any] = {
            "action": action,
            "server_id": server.get("server_id"),
            "server": server,
            "source": "approver",
            "at": _utcnow().isoformat(),
        }
        if extra:
            payload.update(extra)
        await bus.produce(AUDIT_TOPIC, payload, key=server.get("server_id") or None)
        log_event(
            "audit_produce",
            "ok",
            {
                "topic": AUDIT_TOPIC,
                "action": action,
                "server": server.get("server_id"),
            },
        )
    finally:
        await bus.close()


async def post_owner_cards(server: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    sid = server.get("server_id") or "unknown"
    now = _utcnow()
    origins: Dict[str, Dict[str, str]] = {}

    if PERSIST and sid != "unknown":
        try:
            await save_server(server)
            await record_action(
                sid,
                "notice_post_start",
                None,
                None,
                {"channel": PRIMARY_CHANNEL},
            )
            await record_audit(
                sid,
                "notice_post_start",
                "approver",
                {"channel": PRIMARY_CHANNEL},
            )
            await _produce_audit(
                "notice_post_start",
                server,
                {"channel": PRIMARY_CHANNEL},
            )
        except Exception:
            pass

    extended_before = False
    if PERSIST and sid != "unknown":
        try:
            extended_before = await has_ever_extended(sid)
        except Exception as e:
            log_event(
                "approver", "has_ever_extended_err", {"server": sid, "error": str(e)}
            )

    if extended_before:
        info_line = (
            "ℹ️ This server already used its one-time extension. "
            "No further actions are available; it is now scheduled for automatic blocking."
        )

        try:
            ch_resp = await slack_client.chat_postMessage(
                channel=PRIMARY_CHANNEL,
                text=f"Eclipse24 will now block server {sid}",
                blocks=build_owner_base_blocks(server, status_line=info_line),
            )
            if ch_resp.get("ok"):
                origins["channel"] = {
                    "channel": ch_resp["channel"],
                    "ts": ch_resp["ts"],
                }
        except SlackApiError as e:
            log_event(
                "slack_post",
                "channel_info_error",
                {"error": str(e), "channel": PRIMARY_CHANNEL, "server": sid},
            )

        owner_email = (server.get("owner_email") or "").strip().lower()
        if owner_email:
            uid = await _lookup_user_id_by_email(owner_email)
            if uid:
                dm_chan = await _open_dm(uid)
                if dm_chan:
                    try:
                        await slack_client.chat_postMessage(
                            channel=dm_chan,
                            text=f"Eclipse24 will now block your server `{sid}`.",
                            blocks=build_owner_base_blocks(
                                server,
                                status_line=info_line,
                            ),
                        )
                    except SlackApiError as e:
                        log_event(
                            "slack_post",
                            "dm_info_error",
                            {
                                "error": str(e),
                                "email": owner_email,
                                "server": sid,
                            },
                        )

        if PERSIST and sid != "unknown":
            try:
                await _mark_waiting_state(
                    sid, posted_at=now.isoformat(), extension_limit_reached=True
                )
                await touch_reminder(sid, when=now)
                await record_action(
                    sid,
                    "notice_posted_no_buttons",
                    None,
                    None,
                    {"reason": "already_extended"},
                )
                await record_audit(
                    sid,
                    "notice_posted_no_buttons",
                    "approver",
                    {"reason": "already_extended"},
                )
                await _produce_audit(
                    "notice_posted_no_buttons",
                    server,
                    {"reason": "already_extended"},
                )
            except Exception:
                pass

        return origins

    notice_text = _notice_text(sid)
    owner_email = (server.get("owner_email") or "").strip().lower()
    manager_email = (
        (server.get("manager_email") or server.get("owner_manager_email") or "")
        .strip()
        .lower()
    )

    dm_origin: Optional[Dict[str, str]] = None
    channel_origin: Optional[Dict[str, str]] = None

    owner_slack_id: Optional[str] = None
    manager_slack_id: Optional[str] = None

    if owner_email:
        owner_slack_id = await _lookup_user_id_by_email(owner_email)

    if manager_email:
        manager_slack_id = await _lookup_user_id_by_email(manager_email)

    dm_chan: Optional[str] = None
    if owner_slack_id:
        dm_chan = await _open_dm(owner_slack_id)

    if dm_chan:
        dm_blocks = _owner_card_blocks_with_actions(
            server,
            status_line=_NOTICE,
            with_buttons=True,
            value_envelope={"server": server},
        )
        try:
            dm_resp = await slack_client.chat_postMessage(
                channel=dm_chan,
                text=notice_text,
                blocks=dm_blocks,
            )
        except SlackApiError as e:
            log_event(
                "slack_post",
                "dm_error",
                {"error": str(e), "email": owner_email, "server": sid},
            )
            dm_resp = {"ok": False}

        if dm_resp.get("ok"):
            dm_origin = {"channel": dm_resp["channel"], "ts": dm_resp["ts"]}
            origins["dm"] = dm_origin

            dm_value_final = {"server": server, "origin": dm_origin}
            dm_blocks_sync = _owner_card_blocks_with_actions(
                server,
                status_line=_NOTICE,
                with_buttons=True,
                value_envelope=dm_value_final,
            )
            try:
                await slack_client.chat_update(
                    channel=dm_origin["channel"],
                    ts=dm_origin["ts"],
                    text=notice_text,
                    blocks=dm_blocks_sync,
                )
            except SlackApiError as e:
                log_event(
                    "slack_update",
                    "dm_sync_error",
                    {"error": str(e), "origin": dm_origin},
                )
        else:
            log_event(
                "slack_dm",
                "post_failed_fallback_channel",
                {"email": owner_email, "channel": FALLBACK_CHANNEL},
            )
            dm_origin = None

    if not dm_origin:
        status_line = _SLACK_USER_NOT_FOUND or _NOTICE

        channel_blocks = _owner_card_blocks_with_actions(
            server,
            status_line=status_line,
            with_buttons=True,
            value_envelope={"server": server},
        )
        try:
            ch_resp = await slack_client.chat_postMessage(
                channel=FALLBACK_CHANNEL,
                text=notice_text,
                blocks=channel_blocks,
            )
        except SlackApiError as e:
            log_event(
                "slack_post",
                "channel_error",
                {
                    "error": str(e),
                    "channel": FALLBACK_CHANNEL,
                    "server": sid,
                    "reason": "fallback_for_dm",
                },
            )
            if PERSIST and sid != "unknown":
                try:
                    await record_action(
                        sid,
                        "notice_post_fail",
                        None,
                        None,
                        {"where": "channel_fallback", "error": str(e)},
                    )
                    await record_audit(
                        sid,
                        "notice_post_fail",
                        "approver",
                        {"where": "channel_fallback", "error": str(e)},
                    )
                    await _produce_audit(
                        "notice_post_fail",
                        server,
                        {"where": "channel_fallback", "error": str(e)},
                    )
                except Exception:
                    pass
            raise

        if not ch_resp.get("ok"):
            if PERSIST and sid != "unknown":
                try:
                    await record_action(
                        sid,
                        "notice_post_fail",
                        None,
                        None,
                        {"where": "channel_fallback", "resp": ch_resp},
                    )
                    await record_audit(
                        sid,
                        "notice_post_fail",
                        "approver",
                        {"where": "channel_fallback", "resp": ch_resp},
                    )
                    await _produce_audit(
                        "notice_post_fail",
                        server,
                        {"where": "channel_fallback", "resp": ch_resp},
                    )
                except Exception:
                    pass
            raise RuntimeError(f"Fallback channel post failed: {ch_resp}")

        channel_origin = {"channel": ch_resp["channel"], "ts": ch_resp["ts"]}
        origins["channel"] = channel_origin

        chan_value = {"server": server, "origin": channel_origin}
        chan_blocks_sync = _owner_card_blocks_with_actions(
            server,
            status_line=status_line,
            with_buttons=True,
            value_envelope=chan_value,
        )
        try:
            await slack_client.chat_update(
                channel=channel_origin["channel"],
                ts=channel_origin["ts"],
                text=notice_text,
                blocks=chan_blocks_sync,
            )
        except SlackApiError as e:
            log_event(
                "slack_update",
                "channel_sync_error",
                {"error": str(e), "origin": channel_origin},
            )

    if PERSIST and sid != "unknown":
        try:
            state_payload: Dict[str, Any] = {
                "posted_at": now.isoformat(),
                "has_dm": bool(dm_origin),
                "owner_email": owner_email or None,
                "manager_email": manager_email or None,
                "server": server,
            }

            if dm_origin and owner_slack_id:
                state_payload.update(
                    {
                        "owner_slack_id": owner_slack_id,
                        "origin_channel": dm_origin["channel"],
                        "origin_ts": dm_origin["ts"],
                    }
                )
            elif channel_origin:
                state_payload.update(
                    {
                        "origin_channel": channel_origin["channel"],
                        "origin_ts": channel_origin["ts"],
                    }
                )

            if manager_slack_id:
                state_payload["manager_slack_id"] = manager_slack_id

            await _mark_waiting_state(sid, **state_payload)
            await touch_reminder(sid, when=now)
            await record_action(
                sid,
                "notice_posted",
                None,
                None,
                {
                    "channel": PRIMARY_CHANNEL,
                    "fallback_channel": FALLBACK_CHANNEL,
                    "has_dm": bool(dm_origin),
                    "used_fallback": not bool(dm_origin),
                },
            )
            await record_audit(
                sid,
                "notice_posted",
                "approver",
                {
                    "channel": PRIMARY_CHANNEL,
                    "fallback_channel": FALLBACK_CHANNEL,
                    "has_dm": bool(dm_origin),
                    "used_fallback": not bool(dm_origin),
                },
            )
            await _produce_audit(
                "notice_posted",
                server,
                {
                    "channel": PRIMARY_CHANNEL,
                    "fallback_channel": FALLBACK_CHANNEL,
                    "has_dm": bool(dm_origin),
                    "used_fallback": not bool(dm_origin),
                },
            )
        except Exception:
            pass

    return origins


async def handle_waiting_record(payload: Dict[str, Any]):
    await post_owner_cards(payload)
    log_event("approver", "sent", {"server": payload.get("server_id")})


async def _on_waiting(record: KafkaRecord):
    try:
        value = record.value
        if not isinstance(value, dict) and value is not None:
            try:
                value = json.loads(value)
            except Exception:
                log_event("approver", "bad_payload", {"raw": str(value)})
                return
        if not value:
            return
        await handle_waiting_record(value)
    except Exception as e:
        log_event("approver", "handle_error", {"error": str(e)})


async def main():
    consumer_group = f"{settings.KAFKA_GROUP_ID_PREFIX}.approver"
    bus = await KafkaClient().connect()

    try:
        await bus.ensure_topic(WAITING_TOPIC, partitions=1)
        log_event(
            "approver",
            "ensure_topic_result",
            {"topic": WAITING_TOPIC, "note": "ok_or_already_exists"},
        )
    except Exception as e:
        log_event(
            "approver",
            "ensure_topic_error",
            {"topic": WAITING_TOPIC, "error": str(e)},
        )

    log_event(
        "service",
        "start",
        {"service": "approver", "topic": WAITING_TOPIC, "group": consumer_group},
    )

    while True:
        try:
            await bus.consume(
                topic=WAITING_TOPIC,
                handler=_on_waiting,
                group_id=consumer_group,
                auto_offset_reset="earliest",
            )
        except Exception as e:
            log_event(
                "approver",
                "consume_loop_error",
                {"topic": WAITING_TOPIC, "group": consumer_group, "error": str(e)},
            )
            await asyncio.sleep(5)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()