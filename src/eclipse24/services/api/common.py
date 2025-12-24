# src/eclipse24/services/api/common.py

from datetime import datetime, timezone
import time
from typing import Any, Dict, Optional, Set

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.queue_config import topic_for

_RULES = settings.load_rules() or {}
_CONFIG = _RULES.get("config") or {}
_MESSAGES = _CONFIG.get("messages") or {}
_SLACK_CFG = _CONFIG.get("slack") or {}

BLOCK_TOPIC = topic_for("block")
EXTENSIONS_TOPIC = topic_for("extensions")
WAITING_STATE_TOPIC = topic_for("waiting_state")

_SRE_MANAGER_CHANNEL = (
        _SLACK_CFG.get("sre_manager_channel")
        or getattr(settings, "SLACK_SRE_MANAGER_CHANNEL", None)
        or settings.SLACK_APPROVAL_CHANNEL
)

_MANAGER_EXTEND_TMPL = _MESSAGES.get(
    "manager_extend_notice",
    (
        "This is a notification from Eclipse24 tool used for managing internal JPD servers. "
        "You're receiving this message as the manager of the user {user_id} who created the server {server_id}. "
        "The user is asking to extend the usage of this server beyond the allowed period. "
        "Please review the justification and take appropriate action."
    ),
)

_SLACK_EXTEND_TMPL = _MESSAGES.get(
    "slack_extend_notice",
    (
        ":warning: *Eclipse24 Notification*: User *{user_id}* has requested an extension for server *{server_id}*.\n"
        "Manager *{manager_id}* has been notified to approve or deny this request."
    ),
)

_EXTEND_2W_TMPL = _MESSAGES.get(
    "extend_2_weeks_notice",
    (
        ":warning: *Eclipse24 Notification*: You have requested a *2-week* extension for server *{server_id}*.\n"
        "This longer extension is intended only for exceptional cases and must be approved by the SRE manager group.\n"
        "Your request will be reviewed in the SRE managers channel. You will receive a confirmation in Slack once "
        "the request is approved or denied."
    ),
)

_DECISIONS: Dict[str, float] = {}
_DECISION_TTL = 600
_EXTENDED_ONCE: Set[str] = set()
_PENDING_EXTEND: Dict[str, float] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().isoformat()


def format_message(tmpl: str, **kwargs: Any) -> str:
    try:
        return tmpl.format(**kwargs)
    except Exception as e:
        log_event(
            "rules_message",
            "format_error",
            {
                "template": tmpl[:160],
                "error": str(e),
                "kwargs": list(kwargs.keys()),
            },
        )
        return tmpl


def decision_mark(key: str) -> bool:
    now = time.time()
    for k, v in list(_DECISIONS.items()):
        if now - v > _DECISION_TTL:
            _DECISIONS.pop(k, None)
    for k, v in list(_PENDING_EXTEND.items()):
        if now - v > (_DECISION_TTL * 3):
            _PENDING_EXTEND.pop(k, None)

    if _DECISIONS.get(key):
        return False
    _DECISIONS[key] = now
    return True