# src/eclipse24/services/api/block.py

from typing import Any, Dict, Optional

from fastapi.responses import JSONResponse

from eclipse24.libs.core.logging import log_event, slack_error_meta
from eclipse24.libs.slack.utils import (
    slack_client,
    build_block_manager_card_blocks,
    build_owner_card_blocks,
    _update_block_manager_card,
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
)

from .common import (
    BLOCK_TOPIC,
    WAITING_STATE_TOPIC,
    utcnow_iso,
    _SRE_MANAGER_CHANNEL,
)


async def handle_owner_approve_block(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
) -> JSONResponse:
    try:
        await save_server(server)
        await set_state(server_id, "PENDING_BLOCK_APPROVAL", None)
        await record_action(
            server_id,
            "owner_block_requested",
            actor_uid,
            actor_name,
            {"source": "owner_card"},
        )
        await record_audit(
            server_id,
            "block_request_from_owner",
            actor_name,
            {"by_uid": actor_uid},
        )
    except Exception:
        pass

    owner_email = server.get("owner_email")
    mgr_id = await _find_manager_id_for_email(owner_email)

    manager_mention = f"<@{mgr_id}>" if mgr_id else "SRE team"
    status_line = (
        f"{manager_mention} Owner approved to block this server. "
        "Please confirm whether to block or keep running."
    )

    owner_origin = value_obj.get("origin") or {}

    manager_value = {
        "server": server,
        "owner_origin": owner_origin,
        "manager": {"id": mgr_id} if mgr_id else {},
    }

    try:
        manager_msg = await slack_client.chat_postMessage(
            channel=_SRE_MANAGER_CHANNEL,
            text=status_line,
            blocks=build_block_manager_card_blocks(
                server,
                mention_line=f"{manager_mention} ",
                status_line=status_line,
                with_buttons=True,
                value_envelope=manager_value,
            ),
        )
        log_event(
            "slack_post",
            "block_manager_channel_ok",
            {
                "channel": _SRE_MANAGER_CHANNEL,
                "server_id": server_id,
                "ok": manager_msg.get("ok", False),
            },
        )
    except Exception as e:
        log_event(
            "slack_post",
            "block_manager_channel_error",
            {
                "channel": _SRE_MANAGER_CHANNEL,
                "server_id": server_id,
                **slack_error_meta(e),
            },
        )

    await _update_both_places_for_block_request(
        value_obj,
        f"✅ Block requested by <@{actor_uid}>. Server will be blocked shortly.",
        server,
    )

    try:
        await kafka_produce_compacted(
            WAITING_STATE_TOPIC,
            server_id,
            {
                "acted_at": utcnow_iso(),
                "acted_by": actor_uid,
                "action": "owner_block_requested",
                "manager_slack_id": mgr_id,
            },
        )
    except Exception:
        pass

    return JSONResponse({"ok": True, "action": "owner_block_requested"})


async def _update_both_places_for_block_request(
        value_obj: Dict[str, Any],
        text: str,
        server: Dict[str, Any],
) -> None:
    origin = value_obj.get("origin") or {}
    other = value_obj.get("other_origin") or {}
    for ch, ts in [
        (origin.get("channel"), origin.get("ts")),
        (other.get("channel"), other.get("ts")),
    ]:
        if not ch or not ts:
            continue
        await _update_owner_card(ch, ts, server, text)


async def handle_block_approve(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
        container_channel: Optional[str],
        container_ts: Optional[str],
) -> JSONResponse:
    owner_origin = value_obj.get("owner_origin") or {}
    mgr_uid = (value_obj.get("manager") or {}).get("id") or actor_uid

    try:
        await save_server(server)
        await set_state(server_id, "BLOCKED", None)
        await record_action(
            server_id,
            "block_approved",
            actor_uid,
            actor_name,
            {"source": "manager_block_card"},
        )
        await record_audit(
            server_id,
            "block_approved",
            actor_name,
            {"by_uid": actor_uid},
        )
    except Exception:
        pass

    await kafka_produce(
        BLOCK_TOPIC,
        {
            "server": server,
            "approved_by": actor_name,
            "approved_by_uid": actor_uid,
            "reason": "manager_block_approved",
        },
    )

    mention_line = f"<@{mgr_uid}> " if mgr_uid else ""
    await _update_block_manager_card(
        container_channel,
        container_ts,
        server,
        mention_line,
        f"✅ Block approved by <@{actor_uid}>. Server will be blocked.",
    )

    await _update_owner_card(
        owner_origin.get("channel"),
        owner_origin.get("ts"),
        server,
        "✅ Manager approved block. Server will be blocked.",
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
                            f"✅ Your server `{server_id}` was approved for block by "
                            f"<@{actor_uid}>. It will be blocked shortly."
                        ),
                        blocks=build_owner_card_blocks(
                            server,
                            "✅ Manager approved block. Server will be blocked.",
                        ),
                    )
    except Exception as e:
        log_event(
            "slack_dm",
            "owner_block_dm_error",
            {"server_id": server_id, **slack_error_meta(e)},
        )

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "acted_at": utcnow_iso(),
            "acted_by": actor_uid,
            "action": "block_approved",
        },
    )
    log_event(
        "slack_action",
        "block_approved",
        {"server": server_id, "by": actor_name},
    )
    return JSONResponse({"ok": True, "action": "block_approved"})


async def handle_block_deny(
        server: Dict[str, Any],
        server_id: str,
        actor_uid: Optional[str],
        actor_name: str,
        value_obj: Dict[str, Any],
        container_channel: Optional[str],
        container_ts: Optional[str],
) -> JSONResponse:
    owner_origin = value_obj.get("owner_origin") or {}
    mgr_uid = (value_obj.get("manager") or {}).get("id") or actor_uid

    try:
        await save_server(server)
        await set_state(server_id, "KEEP_RUNNING", None)
        await record_action(
            server_id,
            "block_denied_keep_running",
            actor_uid,
            actor_name,
            {"source": "manager_block_card"},
        )
        await record_audit(
            server_id,
            "block_denied_keep_running",
            actor_name,
            {"by_uid": actor_uid},
        )
    except Exception:
        pass

    mention_line = f"<@{mgr_uid}> " if mgr_uid else ""
    await _update_block_manager_card(
        container_channel,
        container_ts,
        server,
        mention_line,
        "❌ Block denied. Server will continue running and is marked as *Keep Running*.",
    )

    await _update_owner_card(
        owner_origin.get("channel"),
        owner_origin.get("ts"),
        server,
        "❌ Manager denied block request. Server will continue running and is marked as *Keep Running*.",
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
                            f"❌ Your block request for `{server_id}` was denied. "
                            "Server will continue running and has been marked as *Keep Running* by your manager."
                        ),
                        blocks=build_owner_card_blocks(
                            server,
                            "❌ Manager denied block request. Server will continue running and is marked as *Keep Running*.",
                        ),
                    )
    except Exception as e:
        log_event(
            "slack_dm",
            "owner_block_dm_error",
            {"server_id": server_id, **slack_error_meta(e)},
        )

    await kafka_produce_compacted(
        WAITING_STATE_TOPIC,
        server_id,
        {
            "acted_at": utcnow_iso(),
            "acted_by": actor_uid,
            "action": "block_denied_keep_running",
        },
    )
    log_event(
        "slack_action",
        "block_denied_keep_running",
        {"server": server_id, "by": actor_name},
    )
    return JSONResponse({"ok": True, "action": "block_denied_keep_running"})