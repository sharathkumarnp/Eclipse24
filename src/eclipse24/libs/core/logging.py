import json
import time
import traceback
from typing import Optional, Dict, Any


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _pack(action: str, status: str, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "TIMESTAMP": _ts(),
        "action": action,
        "status": status,
        "meta": meta or {},
    }


def log_event(action: str, status: str, meta: Optional[Dict[str, Any]] = None):
    """
    Simple structured log line (JSON). Keep existing signature so all callers continue to work.
    """
    try:
        print(json.dumps(_pack(action, status, meta)))
    except Exception:
        # Last-resort fallback to avoid breaking handlers on bad meta
        print(json.dumps(_pack(action, status, {"warn": "log_event_json_fail"})))


def log_error(action: str, err: BaseException, meta: Optional[Dict[str, Any]] = None):
    """
    Like log_event but captures exception details & traceback.
    """
    m = meta.copy() if isinstance(meta, dict) else {}
    m.update({
        "exc_type": type(err).__name__,
        "exc_msg": str(err),
        "trace": traceback.format_exc(limit=20),
    })
    log_event(action, "error", m)


def slack_error_meta(err: BaseException) -> Dict[str, Any]:
    """
    Extract useful bits from SlackApiError without exploding if fields are missing.
    """
    info: Dict[str, Any] = {
        "exc_type": type(err).__name__,
        "exc_msg": str(err),
    }
    # SlackApiError has .response with .status_code and .data
    resp = getattr(err, "response", None)
    if resp is not None:
        try:
            info["slack_status"] = getattr(resp, "status_code", None)
            # resp.data is a dict (usually: {'ok': False, 'error': '...'})
            data = getattr(resp, "data", None)
            if data is not None:
                # Avoid dumping huge payloads
                info["slack_resp"] = data if len(json.dumps(data)) < 8000 else {"size": "large"}
        except Exception:
            pass
    return info
