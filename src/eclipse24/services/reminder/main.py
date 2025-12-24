import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient, KafkaRecord
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.slack.utils import (
    build_block_manager_card_blocks,
    build_reminder_blocks,
    finalize_owner_block_card,
    build_owner_card_blocks,
)

WAITING_STATE_TOPIC = topic_for("waiting_state")
AUDIT_TOPIC = topic_for("audit")
BLOCK_TOPIC = topic_for("block")

slack_client = AsyncWebClient(token=settings.SLACK_BOT_TOKEN)

_RULES = settings.load_rules() or {}
_CONFIG = _RULES.get("config") or {}
_SLACK_CFG = _CONFIG.get("slack") or {}
_MESSAGES = _CONFIG.get("messages") or {}
_REMINDERS = _CONFIG.get("reminders") or {}

_RULE_PRIMARY_CHANNEL = _SLACK_CFG.get("approval_channel")
_RULE_FALLBACK_CHANNEL = _SLACK_CFG.get("fallback_channel")
_RULE_SRE_MANAGER_CHANNEL = _SLACK_CFG.get("sre_manager_channel")

_ENV_PRIMARY = getattr(settings, "SLACK_APPROVAL_CHANNEL", None)
_ENV_FALLBACK = getattr(settings, "SLACK_FALLBACK_CHANNEL", None)
_ENV_SRE_MANAGER_CHANNEL = getattr(settings, "SLACK_SRE_MANAGER_CHANNEL", None)

PRIMARY_CHANNEL = _RULE_PRIMARY_CHANNEL or _ENV_PRIMARY or "#eclipse24-approvals"
FALLBACK_CHANNEL = _RULE_FALLBACK_CHANNEL or _ENV_FALLBACK or PRIMARY_CHANNEL
SRE_MANAGER_CHANNEL = (
        _RULE_SRE_MANAGER_CHANNEL or _ENV_SRE_MANAGER_CHANNEL or FALLBACK_CHANNEL
)

_INITIAL_HOURS = float(_REMINDERS.get("initial_owner_timeout_hours", 24))
_MIN_INTERVAL_HOURS = float(_REMINDERS.get("min_interval_hours", 4))
_MAX_ATTEMPTS = int(_REMINDERS.get("max_attempts", 3))
_ESCALATE_TO_MANAGER = bool(_REMINDERS.get("escalate_to_manager", True))

_BLOCK_AFTER_DAYS = float(_REMINDERS.get("block_after_days", 3))
_PRE_BLOCK_NOTICE_HOURS = float(_REMINDERS.get("pre_block_notice_hours", 24.0))

# how often we scan STATE in memory
SCAN_INTERVAL_SECONDS = 60

STATE: Dict[str, Dict[str, Any]] = {}


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _audit(server_id: str, action: str, extra: Optional[Dict[str, Any]] = None):
    payload: Dict[str, Any] = {
        "server_id": server_id,
        "action": action,
        "source": "reminder",
        "at": _now().isoformat(),
    }
    if extra:
        payload.update(extra)

    bus = await KafkaClient().connect()
    try:
        await bus.produce(AUDIT_TOPIC, payload, key=server_id)
        log_event("reminder_audit", "sent", {"server": server_id, "action": action})
    finally:
        await bus.close()


async def _send_card_dm(
        user_id: str,
        server: Dict[str, Any],
        status_line: str,
        text: str,
        role: str,
        server_id: str,
):
    """DM reminder card to owner/manager."""
    try:
        open_resp = await slack_client.conversations_open(users=user_id)
        if not open_resp.get("ok"):
            log_event(
                "reminder_dm",
                "open_failed",
                {
                    "server": server_id,
                    "role": role,
                    "user_id": user_id,
                    "resp": open_resp,
                },
            )
            return
        channel_id = open_resp["channel"]["id"]

        blocks = build_reminder_blocks(server, status_line=status_line)
        await slack_client.chat_postMessage(
            channel=channel_id,
            text=text,
            blocks=blocks,
        )
        log_event(
            "reminder_dm",
            "ok",
            {
                "server": server_id,
                "role": role,
                "user_id": user_id,
                "channel": channel_id,
            },
        )
    except SlackApiError as e:
        log_event(
            "reminder_dm",
            "error",
            {"server": server_id, "role": role, "user_id": user_id, "error": str(e)},
        )


async def _send_channel_card(
        channel: str,
        server: Dict[str, Any],
        status_line: str,
        text: str,
        server_id: str,
        kind: str,
):
    """Post reminder-style card in a channel (fallback / SRE / etc.)."""
    try:
        blocks = build_reminder_blocks(server, status_line=status_line)
        await slack_client.chat_postMessage(channel=channel, text=text, blocks=blocks)
        log_event(
            "reminder_channel",
            "ok",
            {"server": server_id, "channel": channel, "kind": kind},
        )
    except SlackApiError as e:
        log_event(
            "reminder_channel",
            "error",
            {"server": server_id, "channel": channel, "kind": kind, "error": str(e)},
        )


async def _send_channel_reminder(server_id: str, fields: Dict[str, Any], attempt_no: int):
    """Reminder when we don't have traceable owner/manager Slack IDs."""
    origin_channel = fields.get("origin_channel") or FALLBACK_CHANNEL
    posted_at = _parse_iso(fields.get("posted_at"))
    now = _now()
    elapsed = (now - posted_at).total_seconds() / 3600.0 if posted_at else 0.0
    approx_hours = f"~{int(round(elapsed))}h" if elapsed >= 1 else "~0h"

    server = fields.get("server") or {"server_id": server_id}

    prefix = f"⏰ Reminder #{attempt_no} for server `{server_id}`."
    suffix = (
        f" Owner could not be contacted in Slack (after {approx_hours}). "
        "Please review this server and take action."
    )
    status_line = prefix + " " + suffix
    text = prefix

    await _send_channel_card(
        origin_channel,
        server,
        status_line=status_line,
        text=text,
        server_id=server_id,
        kind="non_traceable_owner",
    )

    await _audit(
        server_id,
        "channel_reminder_posted",
        {"channel": origin_channel, "attempt": attempt_no, "text": text},
    )


async def _send_final_fallback(server_id: str, fields: Dict[str, Any], attempts: int):
    server = fields.get("server") or {"server_id": server_id}

    # Status line that goes *inside* the owner card
    status_line = (
        f"⏰ Final reminder for `{server_id}` – no action taken after {attempts} "
        "reminder(s). Owner and manager did not respond (or could not be contacted). "
        "Routing to this channel for team review."
    )

    # Plain text for the message body
    text = (
        f"⏰ Final reminder for `{server_id}` – routing to this channel for team review."
    )

    try:
        blocks = build_owner_card_blocks(server, status_line=status_line)
        await slack_client.chat_postMessage(
            channel=FALLBACK_CHANNEL,
            text=text,
            blocks=blocks,
        )
        log_event(
            "reminder_channel",
            "ok",
            {"server": server_id, "channel": FALLBACK_CHANNEL, "kind": "final_fallback"},
        )
    except SlackApiError as e:
        log_event(
            "reminder_channel",
            "error",
            {
                "server": server_id,
                "channel": FALLBACK_CHANNEL,
                "kind": "final_fallback",
                "error": str(e),
            },
        )

    await _audit(
        server_id,
        "fallback_escalation",
        {"channel": FALLBACK_CHANNEL, "attempts": attempts, "text": text},
    )


async def _notify_sre_manager_upcoming_block(
        server_id: str,
        fields: Dict[str, Any],
        block_at: datetime,
):
    """Heads-up message to SRE manager channel before auto block routing."""
    server = fields.get("server") or {"server_id": server_id}

    status_line = (
        f"⚠️ Upcoming block decision for `{server_id}` in ~{int(_PRE_BLOCK_NOTICE_HOURS)} hours "
        f"(scheduled at {block_at.isoformat()}). No action from owner/manager after all reminders."
    )
    text = (
        "⚠️ Heads up: this server is scheduled for a block decision soon because "
        "owner/manager did not respond to Eclipse24 reminders."
    )

    await _send_channel_card(
        SRE_MANAGER_CHANNEL,
        server,
        status_line=status_line,
        text=text,
        server_id=server_id,
        kind="pre_block_notice",
    )

    await _audit(
        server_id,
        "pre_block_notice_sent",
        {
            "channel": SRE_MANAGER_CHANNEL,
            "block_scheduled_at": block_at.isoformat(),
            "text": text,
        },
    )


async def _send_sre_manager_block_approval(
        server_id: str,
        fields: Dict[str, Any],
        block_at: datetime,
):
    """Send the block-approval manager card once we've fully escalated."""
    server = fields.get("server") or {"server_id": server_id}

    status_line = (
        f"⚠️ No response from owner or manager. Sending server `{server_id}` for block approval. "
        f"(scheduled at {block_at.isoformat()}). Please review and choose Confirm Block or Keep Running."
    )
    text = (
        "⚠️ Block approval required: no response from owner or manager. "
        "Please review and choose Confirm Block or Keep Running."
    )

    value_envelope: Dict[str, Any] = {"server": server}

    try:
        blocks = build_block_manager_card_blocks(
            server,
            mention_line="",
            status_line=status_line,
            with_buttons=True,
            value_envelope=value_envelope,
        )
        await slack_client.chat_postMessage(
            channel=SRE_MANAGER_CHANNEL,
            text=text,
            blocks=blocks,
        )
        log_event(
            "reminder_block",
            "approval_card_sent",
            {"server": server_id, "channel": SRE_MANAGER_CHANNEL},
        )
    except SlackApiError as e:
        log_event(
            "reminder_block",
            "approval_card_error",
            {"server": server_id, "channel": SRE_MANAGER_CHANNEL, "error": str(e)},
        )
        return

    await _audit(
        server_id,
        "block_approval_card_sent",
        {
            "channel": SRE_MANAGER_CHANNEL,
            "block_scheduled_at": block_at.isoformat(),
            "text": text,
        },
    )


async def _send_owner_manager_reminder(
        server_id: str,
        fields: Dict[str, Any],
        attempt_no: int,
):
    """Normal reminder path: DM owner and optionally manager."""
    owner_uid: Optional[str] = fields.get("owner_slack_id")
    manager_uid: Optional[str] = (
        fields.get("manager_slack_id") if _ESCALATE_TO_MANAGER else None
    )

    posted_at = _parse_iso(fields.get("posted_at"))
    now = _now()
    elapsed = (now - posted_at).total_seconds() / 3600.0 if posted_at else 0.0
    approx_hours = f"~{int(round(elapsed))}h" if elapsed >= 1 else "~0h"

    server = fields.get("server") or {"server_id": server_id}

    base = f"⏰ Reminder #{attempt_no} for server `{server_id}`."
    if attempt_no == 1:
        tail = (
            f" This is the first escalation – owner has not responded within {approx_hours}. "
            "Please review and take action."
        )
    else:
        tail = (
            f" This is escalation #{attempt_no} – still no action after {approx_hours}. "
            "Requested to review and act."
        )

    status_line = base + " " + tail
    text = base

    if owner_uid:
        await _send_card_dm(
            owner_uid,
            server,
            status_line=status_line,
            text=text,
            role="owner",
            server_id=server_id,
        )

    if manager_uid:
        await _send_card_dm(
            manager_uid,
            server,
            status_line=status_line,
            text=text,
            role="manager",
            server_id=server_id,
        )


async def _on_state(record: KafkaRecord):
    try:
        value = record.value
        if not isinstance(value, dict) and value is not None:
            try:
                import json as _json

                value = _json.loads(value)
            except Exception:
                log_event("reminder", "state_bad_json", {"raw": str(value)})
                return

        if not isinstance(value, dict):
            log_event("reminder", "state_non_dict", {"type": str(type(value))})
            return

        sid = value.get("server_id")
        if not sid:
            return

        prev = STATE.get(sid, {})
        prev.update(value)
        STATE[sid] = prev

        log_event(
            "reminder",
            "state_update",
            {"server": sid, "keys": list(value.keys())},
        )
    except Exception as e:
        log_event("reminder", "state_error", {"error": str(e)})


async def _scan_and_remind(bus_state: KafkaClient):
    now = _now()
    initial_delta = timedelta(hours=_INITIAL_HOURS)
    min_interval_delta = timedelta(hours=_MIN_INTERVAL_HOURS)

    for sid, fields in list(STATE.items()):
        posted_at = _parse_iso(fields.get("posted_at"))
        acted_at = _parse_iso(fields.get("acted_at"))
        last_reminded_at = _parse_iso(fields.get("reminded_at"))
        attempts = int(fields.get("reminder_attempts") or 0)

        block_at = _parse_iso(fields.get("block_scheduled_at"))
        block_notice_sent = bool(fields.get("block_notice_sent") or False)
        block_enqueued = bool(fields.get("block_enqueued") or False)

        # nothing to do if we don't know when it was posted
        if not posted_at:
            continue

        # owner/manager already acted, skip
        if acted_at:
            continue

        server = fields.get("server") or {"server_id": sid}

        # === Block scheduling / pre-notice path ===
        if block_at:
            notice_window_start = block_at - timedelta(hours=_PRE_BLOCK_NOTICE_HOURS)
            if (
                    not block_notice_sent
                    and now >= notice_window_start
                    and now < block_at
            ):
                await _notify_sre_manager_upcoming_block(sid, fields, block_at)

                state_update = {
                    "server_id": sid,
                    "block_notice_sent": True,
                }
                try:
                    await bus_state.produce(WAITING_STATE_TOPIC, state_update, key=sid)
                    log_event(
                        "reminder",
                        "state_write_ok",
                        {
                            "server": sid,
                            "topic": WAITING_STATE_TOPIC,
                            "block_notice_sent": True,
                        },
                    )
                    await _audit(
                        sid,
                        "block_notice_flag_set",
                        {"block_notice_sent": True},
                    )
                except Exception as e:
                    log_event(
                        "reminder",
                        "state_write_error",
                        {"server": sid, "error": str(e)},
                    )

            if not block_enqueued and now >= block_at:
                origin_channel = fields.get("origin_channel")
                origin_ts = fields.get("origin_ts")
                if origin_channel and origin_ts:
                    try:
                        await finalize_owner_block_card(
                            origin_channel,
                            origin_ts,
                            server,
                        )
                        log_event(
                            "reminder",
                            "owner_card_finalized",
                            {"server": sid, "channel": origin_channel, "ts": origin_ts},
                        )
                    except Exception as e:
                        log_event(
                            "reminder",
                            "owner_card_finalize_error",
                            {"server": sid, "error": str(e)},
                        )

                await _send_sre_manager_block_approval(sid, fields, block_at)

                state_update = {
                    "server_id": sid,
                    "block_enqueued": True,
                }
                try:
                    await bus_state.produce(WAITING_STATE_TOPIC, state_update, key=sid)
                    log_event(
                        "reminder",
                        "state_write_ok",
                        {
                            "server": sid,
                            "topic": WAITING_STATE_TOPIC,
                            "block_enqueued": True,
                        },
                    )
                    await _audit(
                        sid,
                        "block_approval_flag_set",
                        {"block_enqueued": True},
                    )
                except Exception as e:
                    log_event(
                        "reminder",
                        "state_write_error",
                        {"server": sid, "error": str(e)},
                    )

            # once block_at is set, we don't do normal reminders any more
            continue

        # === Normal reminder / escalation path ===
        escalated = bool(fields.get("escalated") or False)
        if escalated:
            continue

        # respect initial + min interval windows
        if attempts == 0:
            if now < posted_at + initial_delta:
                continue
        else:
            if not last_reminded_at or now < last_reminded_at + min_interval_delta:
                continue

        next_attempt = attempts + 1
        max_reached = next_attempt >= _MAX_ATTEMPTS

        owner_uid: Optional[str] = fields.get("owner_slack_id")
        manager_uid: Optional[str] = (
            fields.get("manager_slack_id") if _ESCALATE_TO_MANAGER else None
        )

        if owner_uid or manager_uid:
            await _send_owner_manager_reminder(sid, fields, next_attempt)
        else:
            await _send_channel_reminder(sid, fields, next_attempt)

        new_state: Dict[str, Any] = {
            "server_id": sid,
            "reminder_attempts": next_attempt,
            "reminded_at": now.isoformat(),
        }

        if max_reached:
            await _send_final_fallback(sid, fields, next_attempt)
            block_scheduled_at = now + timedelta(days=_BLOCK_AFTER_DAYS)

            new_state.update(
                {
                    "escalated": True,
                    "block_scheduled_at": block_scheduled_at.isoformat(),
                    "block_notice_sent": False,
                    "block_enqueued": False,
                }
            )
            await _audit(
                sid,
                "block_scheduled",
                {"block_scheduled_at": block_scheduled_at.isoformat()},
            )

        try:
            await bus_state.produce(WAITING_STATE_TOPIC, new_state, key=sid)
            log_event(
                "reminder",
                "state_write_ok",
                {
                    "server": sid,
                    "topic": WAITING_STATE_TOPIC,
                    "attempt": next_attempt,
                    "escalated": new_state.get("escalated", False),
                },
            )
            await _audit(
                sid,
                "reminder_state_marked",
                {
                    "reminder_attempts": next_attempt,
                    "reminded_at": now.isoformat(),
                    "escalated": new_state.get("escalated", False),
                    "topic": WAITING_STATE_TOPIC,
                },
            )
        except Exception as e:
            log_event(
                "reminder",
                "state_write_error",
                {"server": sid, "error": str(e)},
            )


async def main():
    bus_consume = await KafkaClient().connect()
    bus_state = await KafkaClient().connect()

    group_id = f"{settings.KAFKA_GROUP_ID_PREFIX}.reminder"
    log_event(
        "reminder",
        "start",
        {
            "topic": WAITING_STATE_TOPIC,
            "group": group_id,
            "audit_topic": AUDIT_TOPIC,
            "block_topic": BLOCK_TOPIC,
            "primary_channel": PRIMARY_CHANNEL,
            "fallback_channel": FALLBACK_CHANNEL,
            "sre_manager_channel": SRE_MANAGER_CHANNEL,
            "initial_timeout_hours": _INITIAL_HOURS,
            "min_interval_hours": _MIN_INTERVAL_HOURS,
            "max_attempts": _MAX_ATTEMPTS,
            "escalate_to_manager": _ESCALATE_TO_MANAGER,
            "block_after_days": _BLOCK_AFTER_DAYS,
            "pre_block_notice_hours": _PRE_BLOCK_NOTICE_HOURS,
        },
    )

    asyncio.create_task(
        bus_consume.consume(
            WAITING_STATE_TOPIC,
            _on_state,
            group_id=group_id,
            auto_offset_reset="earliest",
        )
    )

    while True:
        await _scan_and_remind(bus_state)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()