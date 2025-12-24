# src/eclipse24/services/api/extend_2w.py

from datetime import timedelta
from typing import Any, Dict, Optional

from fastapi.responses import JSONResponse

from eclipse24.libs.core.logging import log_event, slack_error_meta
from eclipse24.libs.slack.utils import (
    slack_client,
    build_sre_manager_2w_card_blocks,
    build_owner_card_blocks,
    _update_owner_card,
    _update_sre_manager_2w_card,
    _lookup_user_id_by_email,
)
from eclipse24.libs.kafka.utils import kafka_produce, kafka_produce_compacted
from eclipse24.libs.stores.db import (
    save_server,
    set_state,
    record_action,
    record_audit,
    upsert_extension,
)
from eclipse24.libs.jira.utils import jira_create_extension_issue, JIRA_BASE_URL

from .common import (
    BLOCK_TOPIC,
    EXTENSIONS_TOPIC,
    WAITING_STATE_TOPIC,
    _EXTENDED_ONCE,
    _PENDING_EXTEND,
    utcnow,
    utcnow_iso,
    format_message,
    _SRE_MANAGER_CHANNEL,
    _EXTEND_2W_TMPL,
)


async def handle_extend_2w_request(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
) -> JSONResponse:
    if server_id in _EXTENDED_ONCE:
        return JSONResponse({"ok": True, "action": "extend_2w_rejected_already_used"})

    if server_id in _PENDING_EXTEND:
        await _update_owner_card(
            value_obj.get("origin", {}).get("channel"),
            value_obj.get("origin", {}).get("ts"),
            server,
            f"⏳ Already awaiting extension decision for `{server_id}`.",
        )
        return JSONResponse({"ok": True, "action": "extend_2w_already_pending"})

    try:
        await save_server(server)
        await set_state(server_id, "PENDING", None)
        await record_action(server_id, "extend_2w_requested", actor_uid, actor_name, {})
        await record_audit(
            server_id, "extend_2w_requested", actor_name, {"by_uid": actor_uid}
        )
    except Exception:
        pass

    user_mention = f"<@{actor_uid}>" if actor_uid else actor_name
    notice_text = format_message(_EXTEND_2W_TMPL, server_id=server_id)

    await _update_owner_card(
        value_obj.get("origin", {}).get("channel"),
        value_obj.get("origin", {}).get("ts"),
        server,
        notice_text,
    )

    status_line = (
        f"{user_mention} requested a 2-week extension for `{server_id}`. "
        "Please review and approve or deny."
    )

    value_for_sre = {
        "server": server,
        "owner_origin": value_obj.get("origin"),
    }

    _PENDING_EXTEND[server_id] = utcnow().timestamp()

    try:
        blocks = build_sre_manager_2w_card_blocks(
            server,
            status_line=status_line,
            value_envelope=value_for_sre,
        )
        msg = await slack_client.chat_postMessage(
            channel=_SRE_MANAGER_CHANNEL,
            text=status_line,
            blocks=blocks,
        )
        log_event(
            "slack_post",
            "sre_manager_2w_ok",
            {"channel": _SRE_MANAGER_CHANNEL, "server_id": server_id},
        )
    except Exception as e:
        log_event(
            "slack_post",
            "sre_manager_2w_error",
            {"server_id": server_id, **slack_error_meta(e)},
        )

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "extend_requested_at": utcnow_iso(),
            "extend_requested_by": actor_uid,
            "extend_requested_type": "2w",
        },
    )

    return JSONResponse({"ok": True, "action": "extend_2w_requested"})


async def handle_extend_2w_approve(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
        sre_channel: Optional[str],
        sre_ts: Optional[str],
) -> JSONResponse:
    owner_origin = value_obj.get("owner_origin") or {}

    if server_id in _EXTENDED_ONCE:
        await _update_sre_manager_2w_card(sre_channel, sre_ts, server, "ℹ️ Already handled earlier.")
        _PENDING_EXTEND.pop(server_id, None)
        return JSONResponse({"ok": True, "action": "extend_2w_already_used"})

    now = utcnow()
    expires_at_dt = now + timedelta(days=14)
    expires_at = expires_at_dt.isoformat()
    _EXTENDED_ONCE.add(server_id)

    try:
        await save_server(server)
        await set_state(server_id, "EXTENDED", expires_at_dt)
        await upsert_extension(server_id, 336, expires_at_dt, actor_uid, actor_name)
        await record_action(
            server_id, "extend_2w_approved", actor_uid, actor_name, {"expires_at": expires_at}
        )
        await record_audit(
            server_id,
            "extend_2w_approved",
            actor_name,
            {"by_uid": actor_uid, "expires_at": expires_at},
        )
    except Exception:
        pass

    extension_payload = {
        "server": server,
        "granted_by": actor_name,
        "granted_by_uid": actor_uid,
        "extension_hours": 336,
        "expires_at": expires_at,
    }

    jira_key = None
    try:
        jira_key = await jira_create_extension_issue(
            server, server_id, expires_at, actor_uid, actor_name
        )
    except Exception:
        pass

    if jira_key:
        extension_payload["jira_key"] = jira_key

    await kafka_produce(EXTENSIONS_TOPIC, extension_payload)

    jira_line = f"\n• JIRA: <{JIRA_BASE_URL}/browse/{jira_key}|{jira_key}>" if jira_key else ""

    manager_text = (
        f"✅ 2-week extension approved by <@{actor_uid}>. Expires `{expires_at}`.{jira_line}"
    )
    owner_text = (
        f"✅ Extension approved. Expires `{expires_at}`.{jira_line}"
    )

    await _update_sre_manager_2w_card(sre_channel, sre_ts, server, manager_text)
    await _update_owner_card(owner_origin.get("channel"), owner_origin.get("ts"), server, owner_text)

    try:
        owner_email = server.get("owner_email")
        if owner_email:
            owner_id = await _lookup_user_id_by_email(owner_email)
            if owner_id:
                dm = await slack_client.conversations_open(users=owner_id)
                if dm.get("ok"):
                    dm_text = (
                        f"✅ Your 2-week extension for `{server_id}` was approved. "
                        f"Expires `{expires_at}`."
                    )
                    if jira_key:
                        dm_text += f" JIRA: {jira_key}"
                    await slack_client.chat_postMessage(
                        channel=dm["channel"]["id"],
                        text=dm_text,
                        blocks=build_owner_card_blocks(server, owner_text),
                    )
    except Exception:
        pass

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "acted_at": utcnow_iso(),
            "acted_by": actor_uid,
            "action": "extend_2w_approved",
            "expires_at": expires_at,
            "jira_key": jira_key,
        },
    )

    _PENDING_EXTEND.pop(server_id, None)

    return JSONResponse({"ok": True, "action": "extend_2w_approved"})


async def handle_extend_2w_deny(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
        sre_channel: Optional[str],
        sre_ts: Optional[str],
) -> JSONResponse:
    owner_origin = value_obj.get("owner_origin") or {}

    try:
        await save_server(server)
        await set_state(server_id, "BLOCKED", None)
        await record_action(server_id, "extend_2w_denied", actor_uid, actor_name, {})
        await record_audit(server_id, "extend_2w_denied", actor_name, {"by_uid": actor_uid})
    except Exception:
        pass

    await kafka_produce(
        BLOCK_TOPIC,
        {
            "server": server,
            "denied_by": actor_name,
            "denied_by_uid": actor_uid,
            "reason": "extend_2w_denied",
        },
    )

    manager_text = "❌ 2-week extension denied. Server will be blocked."
    owner_text = "❌ 2-week extension request was denied. Server will be blocked."

    await _update_sre_manager_2w_card(sre_channel, sre_ts, server, manager_text)
    await _update_owner_card(owner_origin.get("channel"), owner_origin.get("ts"), server, owner_text)

    try:
        owner_email = server.get("owner_email")
        if owner_email:
            owner_id = await _lookup_user_id_by_email(owner_email)
            if owner_id:
                dm = await slack_client.conversations_open(users=owner_id)
                if dm.get("ok"):
                    await slack_client.chat_postMessage(
                        channel=dm["channel"]["id"],
                        text=f"❌ Your 2-week extension for `{server_id}` was denied. Server will be blocked.",
                        blocks=build_owner_card_blocks(server, owner_text),
                    )
    except Exception:
        pass

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {"acted_at": utcnow_iso(), "acted_by": actor_uid, "action": "extend_2w_denied"},
    )

    _PENDING_EXTEND.pop(server_id, None)

    return JSONResponse({"ok": True, "action": "extend_2w_denied"})