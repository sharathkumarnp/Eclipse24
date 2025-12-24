import json
import traceback
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event, log_error
from eclipse24.libs.slack.utils import slack_client, MANAGER_FIELD_ID
from eclipse24.libs.jira.utils import (
    jira_enabled,
    JIRA_BASE_URL,
    JIRA_ISSUE_TYPE,
    JIRA_TEAMS_VALUE,
    JIRA_REQUESTING_TEAM_VALUE,
    JIRA_REQUEST_TYPE_VALUE,
)
from .common import (
    BLOCK_TOPIC,
    EXTENSIONS_TOPIC,
    WAITING_STATE_TOPIC,
    _PENDING_EXTEND,
    _EXTENDED_ONCE,
    decision_mark,
)
from .extend_3d import (
    handle_extend_3d_request,
    handle_extend_approve_3d,
    handle_extend_deny_3d,
)
from .extend_2w import (
    handle_extend_2w_request,
    handle_extend_2w_approve,
    handle_extend_2w_deny,
)
from .block import (
    handle_owner_approve_block,
    handle_block_approve,
    handle_block_deny,
)

app = FastAPI(title="Eclipse API", version="0.1.0")


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "topics": {
            "block": BLOCK_TOPIC,
            "extensions": EXTENSIONS_TOPIC,
            "waiting_state": WAITING_STATE_TOPIC,
        },
        "manager_field_id": MANAGER_FIELD_ID or None,
        "pending_extend_count": len(_PENDING_EXTEND),
        "extended_once_count": len(_EXTENDED_ONCE),
        "persist_to_db": bool(settings.PERSIST_TO_DB and settings.PG_DSN),
        "jira_enabled": jira_enabled(),
        "jira_issue_type": JIRA_ISSUE_TYPE,
        "jira_teams_value": JIRA_TEAMS_VALUE,
        "jira_requesting_team_value": JIRA_REQUESTING_TEAM_VALUE,
        "jira_request_type_value": JIRA_REQUEST_TYPE_VALUE,
    }


@app.post("/eclipse/webhooks/slack")
async def slack_webhook(request: Request):
    try:
        form = await request.form()
        payload_raw = form.get("payload")
        if not payload_raw:
            return JSONResponse({"ok": False, "error": "missing payload"}, status_code=400)

        payload = json.loads(payload_raw)
        actions = payload.get("actions") or []
        if not actions:
            return JSONResponse({"ok": False, "error": "no actions"}, status_code=400)

        action = actions[0]
        action_id = action.get("action_id")
        value_obj = json.loads(action.get("value") or "{}")
        server = value_obj.get("server") or {}
        server_id = server.get("server_id", "unknown")

        user = payload.get("user", {}) or {}
        actor_uid: Optional[str] = user.get("id")
        actor_name = user.get("username") or user.get("name") or actor_uid or "unknown"

        container = payload.get("container") or {}
        container_channel: Optional[str] = container.get("channel_id")
        container_ts: Optional[str] = container.get("message_ts")

        log_event(
            "slack_action",
            "received",
            {"action_id": action_id, "server_id": server_id, "actor_uid": actor_uid},
        )

        dedupe_ids = (
            "eclipse24_approve",
            "eclipse24_extend_3d",
            "eclipse24_extend_2w",
            "eclipse24_extend_approve",
            "eclipse24_extend_deny",
            "eclipse24_extend_2w_approve",
            "eclipse24_extend_2w_deny",
            "eclipse24_block_approve",
            "eclipse24_block_deny",
            "eclipse24_extend_permanent",
        )

        if action_id in dedupe_ids:
            if not decision_mark(f"{action_id}:{server_id}"):
                from eclipse24.libs.slack.utils import _update_both_places
                await _update_both_places(
                    value_obj,
                    "ℹ️ Already handled by someone else.",
                    server,
                )
                return JSONResponse({"ok": True, "action": "noop_already_handled"})

        if action_id == "eclipse24_approve":
            return await handle_owner_approve_block(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
            )

        if action_id == "eclipse24_extend_3d":
            return await handle_extend_3d_request(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
            )

        if action_id == "eclipse24_extend_2w":
            return await handle_extend_2w_request(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
            )

        if action_id == "eclipse24_extend_approve":
            return await handle_extend_approve_3d(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
            )

        if action_id == "eclipse24_extend_deny":
            return await handle_extend_deny_3d(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
            )

        if action_id == "eclipse24_extend_2w_approve":
            return await handle_extend_2w_approve(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
                container_channel,
                container_ts,
            )

        if action_id == "eclipse24_extend_2w_deny":
            return await handle_extend_2w_deny(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
                container_channel,
                container_ts,
            )

        if action_id == "eclipse24_block_approve":
            return await handle_block_approve(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
                container_channel,
                container_ts,
            )

        if action_id == "eclipse24_block_deny":
            return await handle_block_deny(
                server,
                server_id,
                actor_uid,
                actor_name,
                value_obj,
                container_channel,
                container_ts,
            )

        if action_id == "eclipse24_extend_permanent":
            from eclipse24.libs.slack.utils import _update_both_places
            await _update_both_places(
                value_obj,
                "ℹ️ Permanent extension is not supported. Please contact SRE.",
                server,
            )
            log_event(
                "slack_action",
                "extend_permanent_clicked",
                {"server": server_id, "by": actor_name},
            )
            return JSONResponse({"ok": True, "action": "extend_permanent_not_supported"})

        return JSONResponse({"ok": False, "error": "unknown action"}, status_code=400)

    except Exception as e:
        log_error(
            "slack_action",
            e,
            {"where": "webhook_handler", "trace": traceback.format_exc()},
        )
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=200)