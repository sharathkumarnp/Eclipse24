# src/eclipse24/services/api/extend_3d.py
from datetime import timedelta
from typing import Any, Dict, Optional

from fastapi.responses import JSONResponse

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event, slack_error_meta
from eclipse24.libs.slack.utils import (
    slack_client,
    build_manager_card_blocks,
    build_owner_card_blocks,
    _update_both_places,
    _update_manager_card,
    _update_owner_card,
    _lookup_user_id_by_email,
    _find_manager_id_for_email,
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
    _MANAGER_EXTEND_TMPL,
    _SLACK_EXTEND_TMPL,
)


async def handle_extend_3d_request(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
) -> JSONResponse:
    if server_id in _EXTENDED_ONCE:
        log_event(
            "slack_action",
            "extend_3d_already_used",
            {"server": server_id, "by": actor_name},
        )
        return JSONResponse({"ok": True, "action": "extend_rejected_already_used"})

    if server_id in _PENDING_EXTEND:
        await _update_both_places(
            value_obj,
            f"⏳ Already awaiting manager approval for `{server_id}`.",
            server,
        )
        return JSONResponse({"ok": True, "action": "extend_already_pending"})

    try:
        await save_server(server)
        await set_state(server_id, "PENDING", None)
        await record_action(
            server_id,
            "extend_requested",
            actor_uid,
            actor_name,
            {},
        )
        await record_audit(
            server_id,
            "extend_requested",
            actor_name,
            {"by_uid": actor_uid},
        )
    except Exception:
        pass

    owner_email = server.get("owner_email")
    mgr_id = await _find_manager_id_for_email(owner_email)

    user_mention = f"<@{actor_uid}>" if actor_uid else (actor_name or "unknown-user")
    manager_mention = f"<@{mgr_id}>" if mgr_id else "their manager"

    slack_text = format_message(
        _SLACK_EXTEND_TMPL,
        user_id=user_mention,
        server_id=server_id,
        manager_id=manager_mention,
    )
    manager_dm_text = format_message(
        _MANAGER_EXTEND_TMPL,
        user_id=user_mention,
        server_id=server_id,
    )

    mention_line = f"{manager_mention} " if mgr_id else ""
    _PENDING_EXTEND[server_id] = utcnow().timestamp()

    owner_origin = value_obj.get("origin") or {}

    mgr_msg: Dict[str, Any] = {"ok": False}
    mgr_origin: Optional[Dict[str, Any]] = None

    try:
        mgr_value = {
            "server": server,
            "owner_origin": owner_origin,
            "origin": None,
            "manager_peer_origin": None,
            "manager": {"id": mgr_id} if mgr_id else {},
        }
        mgr_blocks = build_manager_card_blocks(
            server,
            mention_line,
            status_line=slack_text,
            with_buttons=True,
            value_envelope=mgr_value,
        )
        mgr_msg = await slack_client.chat_postMessage(
            channel=settings.SLACK_APPROVAL_CHANNEL,
            text=slack_text,
            blocks=mgr_blocks,
        )
        log_event(
            "slack_post",
            "manager_channel_ok",
            {
                "channel": settings.SLACK_APPROVAL_CHANNEL,
                "server_id": server_id,
                "ok": mgr_msg.get("ok", False),
            },
        )
    except Exception as e:
        log_event(
            "slack_post",
            "manager_channel_error",
            {
                "channel": settings.SLACK_APPROVAL_CHANNEL,
                "server_id": server_id,
                **slack_error_meta(e),
            },
        )

    if mgr_msg.get("ok"):
        mgr_origin = {"channel": mgr_msg["channel"], "ts": mgr_msg["ts"]}
        sync_blocks = build_manager_card_blocks(
            server,
            mention_line,
            status_line=slack_text,
            with_buttons=True,
            value_envelope={
                "server": server,
                "owner_origin": owner_origin,
                "origin": mgr_origin,
                "manager_peer_origin": None,
                "manager": {"id": mgr_id} if mgr_id else {},
            },
        )
        try:
            await slack_client.chat_update(
                channel=mgr_origin["channel"],
                ts=mgr_origin["ts"],
                text=slack_text,
                blocks=sync_blocks,
            )
        except Exception as e:
            log_event(
                "slack_update",
                "manager_sync_error",
                {"server_id": server_id, "origin": mgr_origin, **slack_error_meta(e)},
            )

    if mgr_id:
        try:
            dm_open = await slack_client.conversations_open(users=mgr_id)
            if dm_open.get("ok"):
                dm_channel = dm_open["channel"]["id"]
                m_dm = await slack_client.chat_postMessage(
                    channel=dm_channel,
                    text=manager_dm_text,
                    blocks=build_manager_card_blocks(
                        server,
                        mention_line="",
                        status_line=manager_dm_text,
                        with_buttons=True,
                        value_envelope={
                            "server": server,
                            "owner_origin": owner_origin,
                            "origin": {"channel": dm_channel, "ts": "pending"},
                            "manager_peer_origin": mgr_origin
                            if mgr_origin
                            else None,
                            "manager": {"id": mgr_id},
                        },
                    ),
                )
                if m_dm.get("ok"):
                    dm_origin = {"channel": m_dm["channel"], "ts": m_dm["ts"]}
                    try:
                        await slack_client.chat_update(
                            channel=dm_origin["channel"],
                            ts=dm_origin["ts"],
                            text=manager_dm_text,
                            blocks=build_manager_card_blocks(
                                server,
                                "",
                                status_line=manager_dm_text,
                                with_buttons=True,
                                value_envelope={
                                    "server": server,
                                    "owner_origin": owner_origin,
                                    "origin": dm_origin,
                                    "manager_peer_origin": mgr_origin
                                    if mgr_origin
                                    else None,
                                    "manager": {"id": mgr_id},
                                },
                            ),
                        )
                    except Exception as e:
                        log_event(
                            "slack_update",
                            "manager_dm_sync_error",
                            {"server_id": server_id, **slack_error_meta(e)},
                        )
        except Exception as e:
            log_event(
                "slack_dm",
                "manager_dm_open_error",
                {"server_id": server_id, "manager": mgr_id, **slack_error_meta(e)},
            )

    await _update_both_places(
        value_obj,
        f"⏳ Extension requested by <@{actor_uid}>. Awaiting manager approval. "
        "Please get it approved in 24h. If your manager does not respond, contact @sre-managers directly." 
        "Server will be blocked after extension period expires.",
        server,
    )
    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "extend_requested_at": utcnow_iso(),
            "extend_requested_by": actor_uid,
            "manager_slack_id": mgr_id,
        },
    )
    log_event(
        "slack_action",
        "extend_requested",
        {"server": server_id, "by": actor_name, "manager_id": mgr_id},
    )
    return JSONResponse({"ok": True, "action": "extend_requested"})


async def handle_extend_approve_3d(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
) -> JSONResponse:
    origin = value_obj.get("origin") or {}
    owner_origin = value_obj.get("owner_origin") or {}
    peer_origin = value_obj.get("manager_peer_origin") or {}
    mgr_uid = (value_obj.get("manager") or {}).get("id") or actor_uid

    if server_id in _EXTENDED_ONCE:
        await _update_manager_card(
            origin.get("channel"),
            origin.get("ts"),
            server,
            "",
            "ℹ️ Already handled earlier.",
        )
        _PENDING_EXTEND.pop(server_id, None)
        return JSONResponse({"ok": True, "action": "extend_already_used"})

    now = utcnow()
    expires_at_dt = now + timedelta(days=3)
    expires_at = expires_at_dt.isoformat()
    _EXTENDED_ONCE.add(server_id)

    try:
        await save_server(server)
        await set_state(server_id, "EXTENDED", expires_at_dt)
        await upsert_extension(
            server_id,
            72,
            expires_at_dt,
            actor_uid,
            actor_name,
        )
        await record_action(
            server_id,
            "extend_approved",
            actor_uid,
            actor_name,
            {"expires_at": expires_at},
        )
        await record_audit(
            server_id,
            "extend_approved",
            actor_name,
            {"by_uid": actor_uid, "expires_at": expires_at},
        )
    except Exception:
        pass

    extension_payload: Dict[str, Any] = {
        "server": server,
        "granted_by": actor_name,
        "granted_by_uid": actor_uid,
        "extension_hours": 72,
        "expires_at": expires_at,
    }

    jira_key: Optional[str] = None
    try:
        jira_key = await jira_create_extension_issue(
            server,
            server_id,
            expires_at,
            mgr_uid,
            actor_name,
        )
    except Exception as e:
        log_event(
            "jira",
            "extension_track_error",
            {"server": server_id, "error": str(e)},
        )

    if jira_key:
        extension_payload["jira_key"] = jira_key

    await kafka_produce(EXTENSIONS_TOPIC, extension_payload)

    jira_line = (
        f"\n• JIRA: <{JIRA_BASE_URL}/browse/{jira_key}|{jira_key}>"
        if jira_key
        else ""
    )

    mention_line = f"<@{mgr_uid}> " if mgr_uid else ""
    manager_text = (
        f"✅ Extension approved by <@{actor_uid}>. Expires `{expires_at}`."
        f"{jira_line}"
    )
    owner_text = (
        f"✅ Extension approved. Expires `{expires_at}`."
        f"{jira_line}"
    )

    await _update_manager_card(
        origin.get("channel"),
        origin.get("ts"),
        server,
        mention_line,
        manager_text,
    )

    if peer_origin.get("channel") and peer_origin.get("ts"):
        await _update_manager_card(
            peer_origin.get("channel"),
            peer_origin.get("ts"),
            server,
            mention_line,
            manager_text,
        )

    if owner_origin.get("channel") and owner_origin.get("ts"):
        await _update_owner_card(
            owner_origin.get("channel"),
            owner_origin.get("ts"),
            server,
            owner_text,
        )

    try:
        owner_email = server.get("owner_email")
        if owner_email:
            owner_id = await _lookup_user_id_by_email(owner_email)
            if owner_id:
                dm = await slack_client.conversations_open(users=owner_id)
                if dm.get("ok"):
                    dm_text = (
                        f"✅ Your extension for `{server_id}` was approved. "
                        f"Expires `{expires_at}`."
                    )
                    if jira_key:
                        dm_text += f" JIRA: {jira_key}"
                    await slack_client.chat_postMessage(
                        channel=dm["channel"]["id"],
                        text=dm_text,
                        blocks=build_owner_card_blocks(
                            server,
                            owner_text,
                        ),
                    )
    except Exception as e:
        log_event(
            "slack_dm",
            "owner_dm_error",
            {"server_id": server_id, **slack_error_meta(e)},
        )

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "acted_at": utcnow_iso(),
            "acted_by": actor_uid,
            "action": "extend_approved",
            "expires_at": expires_at,
            "jira_key": jira_key,
        },
    )

    _PENDING_EXTEND.pop(server_id, None)
    log_event(
        "slack_action",
        "extend_approved",
        {
            "server": server_id,
            "by": actor_name,
            "expires_at": expires_at,
            "jira_key": jira_key,
        },
    )
    return JSONResponse({"ok": True, "action": "extend_approved"})


async def handle_extend_deny_3d(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
) -> JSONResponse:
    origin = value_obj.get("origin") or {}
    owner_origin = value_obj.get("owner_origin") or {}
    peer_origin = value_obj.get("manager_peer_origin") or {}
    mgr_uid = (value_obj.get("manager") or {}).get("id") or actor_uid

    try:
        await save_server(server)
        await set_state(server_id, "BLOCKED", None)
        await record_action(
            server_id,
            "extend_denied",
            actor_uid,
            actor_name,
            {},
        )
        await record_audit(
            server_id,
            "extend_denied",
            actor_name,
            {"by_uid": actor_uid},
        )
    except Exception:
        pass

    await kafka_produce(
        BLOCK_TOPIC,
        {
            "server": server,
            "denied_by": actor_name,
            "denied_by_uid": actor_uid,
            "reason": "extend_denied",
        },
    )
    mention_line = f"<@{mgr_uid}> " if mgr_uid else ""

    await _update_manager_card(
        origin.get("channel"),
        origin.get("ts"),
        server,
        mention_line,
        "❌ Extension denied. Server will be blocked.",
    )

    if peer_origin.get("channel") and peer_origin.get("ts"):
        await _update_manager_card(
            peer_origin.get("channel"),
            peer_origin.get("ts"),
            server,
            mention_line,
            "❌ Extension denied. Server will be blocked.",
        )

    if owner_origin.get("channel") and owner_origin.get("ts"):
        await _update_owner_card(
            owner_origin.get("channel"),
            owner_origin.get("ts"),
            server,
            "❌ Manager denied extension. Server will be blocked.",
        )

    try:
        owner_email = server.get("owner_email")
        if owner_email:
            owner_id = await _lookup_user_id_by_email(owner_email)
            if owner_id:
                dm = await slack_client.conversations_open(users=owner_id)
                if dm.get("ok"):
                    await slack_client.chat_postMessage(
                        channel=dm["channel"]["id"],
                        text=(
                            f"❌ Your extension for `{server_id}` was denied. "
                            "Server will be blocked."
                        ),
                        blocks=build_owner_card_blocks(
                            server,
                            "❌ Manager denied extension. Server will be blocked.",
                        ),
                    )
    except Exception as e:
        log_event(
            "slack_dm",
            "owner_dm_error",
            {"server_id": server_id, **slack_error_meta(e)},
        )

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "acted_at": utcnow_iso(),
            "acted_by": actor_uid,
            "action": "extend_denied",
        },
    )
    _PENDING_EXTEND.pop(server_id, None)
    log_event(
        "slack_action",
        "extend_denied",
        {"server": server_id, "by": actor_name},
    )
    return JSONResponse({"ok": True, "action": "extend_denied"})