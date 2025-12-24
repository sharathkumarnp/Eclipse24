# src/eclipse24/libs/slack/utils.py
import os
import re
from typing import Any, Dict, List, Optional

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event, log_error, slack_error_meta

slack_client = AsyncWebClient(token=settings.SLACK_BOT_TOKEN)

MANAGER_FIELD_ID = os.getenv("SLACK_MANAGER_FIELD_ID", "").strip()
MENTION_RE = re.compile(r"^<@([A-Z0-9]+)>$")


def _safe_json(o: Any) -> str:
    import json
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False)


def _server_fields(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    sid = s.get("server_id", "unknown")
    servername = s.get("server_name") or s.get("name") or sid
    owner = s.get("owner_email") or "unknown@jfrog.com"
    region = s.get("region") or s.get("cloud_region") or "unknown"
    usage = s.get("usage", "n/a")
    est_cost = s.get("est_cost", "$0.00")
    return [
        {"type": "mrkdwn", "text": f"*Server Name:*\n`{servername}`"},
        {"type": "mrkdwn", "text": f"*Technical Name:*\n`{sid}`"},
        {"type": "mrkdwn", "text": f"*Owner:*\n{owner}"},
        {"type": "mrkdwn", "text": f"*Region:*\n{region}"},
        {"type": "mrkdwn", "text": f"*Usage:*\n{usage}"},
        {"type": "mrkdwn", "text": f"*Est. Cost:*\n{est_cost}"},
    ]


def build_owner_card_blocks(
        server: Dict[str, Any],
        status_line: Optional[str] = None,
) -> List[Dict[str, Any]]:
    b: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Eclipse24 – Decommission Request"},
        },
        {"type": "section", "fields": _server_fields(server)},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Running for: *{server.get('runtime_hours')}h* • Source: *Sentinel*",
                }
            ],
        },
        {"type": "divider"},
    ]
    if status_line:
        b.insert(1, {"type": "section", "text": {"type": "mrkdwn", "text": status_line}})
    return b


def build_manager_card_blocks(
        server: Dict[str, Any],
        mention_line: str,
        status_line: Optional[str] = None,
        with_buttons: bool = True,
        value_envelope: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    txt = (
            status_line
            or f"{mention_line}Owner requested a *3-day extension*. Approve?"
    ).strip()

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Eclipse24 – Extension Approval Needed"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": txt}},
        {"type": "section", "fields": _server_fields(server)},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Running for: *{server.get('runtime_hours')}h* • Source: *Sentinel*",
                }
            ],
        },
        {"type": "divider"},
    ]

    if with_buttons:
        value = value_envelope or {"server": server}
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Approve Extend"},
                        "style": "primary",
                        "action_id": "eclipse24_extend_approve",
                        "value": _safe_json(value),
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Deny Extend"},
                        "style": "danger",
                        "action_id": "eclipse24_extend_deny",
                        "value": _safe_json(value),
                    },
                ],
            }
        )
    return blocks


def build_block_manager_card_blocks(
        server: Dict[str, Any],
        mention_line: str,
        status_line: Optional[str] = None,
        with_buttons: bool = True,
        value_envelope: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    txt = (
            status_line
            or f"{mention_line}Owner approved to *block* this server. Please confirm block action."
    ).strip()

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Eclipse24 – Block Approval Needed"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": txt}},
        {"type": "section", "fields": _server_fields(server)},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Running for: *{server.get('runtime_hours')}h* • Source: *Sentinel*",
                }
            ],
        },
        {"type": "divider"},
    ]

    if with_buttons:
        value = value_envelope or {"server": server}
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Confirm Block"},
                        "style": "primary",
                        "action_id": "eclipse24_block_approve",
                        "value": _safe_json(value),
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Keep Running"},
                        "style": "danger",
                        "action_id": "eclipse24_block_deny",
                        "value": _safe_json(value),
                    },
                ],
            }
        )
    return blocks


def build_sre_manager_2w_card_blocks(
        server: Dict[str, Any],
        status_line: str,
        value_envelope: Dict[str, Any],
) -> List[Dict[str, Any]]:
    txt = status_line.strip()
    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Eclipse24 – 2-week Extension Approval Needed",
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": txt}},
        {"type": "section", "fields": _server_fields(server)},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Running for: *{server.get('runtime_hours')}h* • Source: *Sentinel*",
                }
            ],
        },
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Approve 2-week Extend"},
                    "style": "primary",
                    "action_id": "eclipse24_extend_2w_approve",
                    "value": _safe_json(value_envelope),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ Deny 2-week Extend"},
                    "style": "danger",
                    "action_id": "eclipse24_extend_2w_deny",
                    "value": _safe_json(value_envelope),
                },
            ],
        },
    ]
    return blocks


def build_reminder_blocks(
        server: Dict[str, Any],
        status_line: str,
) -> List[Dict[str, Any]]:
    sid = server.get("server_id", "unknown")
    owner = server.get("owner_email", "unknown@jfrog.com")
    region = server.get("region") or server.get("cloud_region") or "unknown"

    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": status_line,
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Owner: `{owner}` • Region: `{region}` • Server: `{sid}`",
                }
            ],
        },
        {"type": "divider"},
    ]


async def _update_owner_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
        text: str,
) -> None:
    if not channel or not ts:
        return
    try:
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=text,
            blocks=build_owner_card_blocks(server, text),
        )
        log_event(
            "slack_update",
            "owner_ok",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "owner_error", m)


async def _update_manager_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
        mention_line: str,
        text: str,
) -> None:
    if not channel or not ts:
        return
    try:
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=text,
            blocks=build_manager_card_blocks(
                server,
                mention_line,
                text,
                with_buttons=False,
            ),
        )
        log_event(
            "slack_update",
            "manager_ok",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "manager_error", m)


async def _update_block_manager_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
        mention_line: str,
        text: str,
) -> None:
    if not channel or not ts:
        return
    try:
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=text,
            blocks=build_block_manager_card_blocks(
                server,
                mention_line,
                status_line=text,
                with_buttons=False,
            ),
        )
        log_event(
            "slack_update",
            "block_manager_ok",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "block_manager_error", m)


async def finalize_owner_block_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
) -> None:
    if not channel or not ts:
        return
    footer = "No response detected, sending for block. Please get in touch with @sre-managers."
    try:
        blocks = build_owner_card_blocks(server)
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": footer}],
            }
        )
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=footer,
            blocks=blocks,
        )
        log_event(
            "slack_update",
            "owner_finalized_for_block",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "owner_finalized_for_block_error", m)


async def finalize_manager_block_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
) -> None:
    if not channel or not ts:
        return
    footer = (
        "No response detected from owner or manager. "
        "Sent for block review to @sre-managers."
    )
    try:
        blocks = build_block_manager_card_blocks(
            server,
            mention_line="",
            status_line="No response detected. Escalated for block review.",
            with_buttons=False,
        )
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": footer}],
            }
        )
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=footer,
            blocks=blocks,
        )
        log_event(
            "slack_update",
            "manager_finalized_for_block",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "manager_finalized_for_block_error", m)


async def _update_sre_manager_2w_card(
        channel: Optional[str],
        ts: Optional[str],
        server: Dict[str, Any],
        text: str,
) -> None:
    if not channel or not ts:
        return
    try:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Eclipse24 – 2-week Extension Approval",
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            },
            {"type": "section", "fields": _server_fields(server)},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Running for: *{server.get('runtime_hours')}h* • Source: *Sentinel*",
                    }
                ],
            },
            {"type": "divider"},
        ]
        resp = await slack_client.chat_update(
            channel=channel,
            ts=ts,
            text=text,
            blocks=blocks,
        )
        log_event(
            "slack_update",
            "sre_manager_2w_ok",
            {
                "channel": channel,
                "ts": ts,
                "server_id": server.get("server_id"),
                "ok": resp.get("ok", False),
            },
        )
    except SlackApiError as e:
        m = {"channel": channel, "ts": ts, "server_id": server.get("server_id")}
        m.update(slack_error_meta(e))
        log_event("slack_update", "sre_manager_2w_error", m)


async def _update_both_places(
        value_obj: Dict[str, Any],
        text: str,
        server: Dict[str, Any],
) -> None:
    origin = value_obj.get("origin") or {}
    other = value_obj.get("other_origin") or {}
    for (ch, ts, tag) in [
        (origin.get("channel"), origin.get("ts"), "origin"),
        (other.get("channel"), other.get("ts"), "other_origin"),
    ]:
        try:
            await _update_owner_card(ch, ts, server, text)
            log_event(
                "slack_update",
                "owner_place_updated",
                {
                    "place": tag,
                    "channel": ch,
                    "ts": ts,
                    "server_id": server.get("server_id"),
                },
            )
        except Exception as e:
            log_error(
                "slack_update",
                e,
                {
                    "place": tag,
                    "channel": ch,
                    "ts": ts,
                    "server_id": server.get("server_id"),
                },
            )


async def _lookup_user_id_by_email(email: str) -> Optional[str]:
    if not email:
        return None
    try:
        resp = await slack_client.users_lookupByEmail(email=email)
        if resp.get("ok") and resp.get("user", {}).get("id"):
            uid = resp["user"]["id"]
            log_event("slack_lookup", "ok", {"email": email, "uid": uid})
            return uid
        log_event(
            "slack_lookup",
            "not_found",
            {"email": email, "ok": resp.get("ok", False)},
        )
    except SlackApiError as e:
        m: Dict[str, Any] = {"email": email}
        m.update(slack_error_meta(e))
        log_event("slack_lookup", "error", m)
    return None


async def _find_manager_id_for_email(owner_email: Optional[str]) -> Optional[str]:
    if not owner_email or not MANAGER_FIELD_ID:
        log_event(
            "slack_manager",
            "skip",
            {
                "reason": "missing_owner_or_field",
                "has_owner": bool(owner_email),
                "field_set": bool(MANAGER_FIELD_ID),
            },
        )
        return None
    try:
        user = await slack_client.users_lookupByEmail(email=owner_email)
        if not user.get("ok"):
            log_event("slack_manager", "owner_not_found", {"owner_email": owner_email})
            return None
        uid = user["user"]["id"]
        prof = await slack_client.users_profile_get(user=uid)
        if not prof.get("ok"):
            log_event("slack_manager", "profile_error", {"owner_uid": uid})
            return None
        fields = prof["profile"].get("fields") or {}
        raw = (fields.get(MANAGER_FIELD_ID, {}).get("value") or "").strip()
        if not raw:
            log_event("slack_manager", "no_manager_field", {"owner_uid": uid})
            return None
        m = MENTION_RE.match(raw)
        if m:
            return m.group(1)
        if raw.startswith("U") and raw.isalnum():
            return raw
        if "@" in raw:
            return await _lookup_user_id_by_email(raw)
        log_event("slack_manager", "unrecognized_value", {"raw": raw})
    except SlackApiError as e:
        log_event("slack_manager", "error", slack_error_meta(e))
    return None