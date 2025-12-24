# src/eclipse24/libs/jira/utils.py

import os
from typing import Any, Dict, Optional

import httpx

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event

_RULES = settings.load_rules() or {}
_CONFIG = _RULES.get("config") or {}
_JIRA_CFG = _CONFIG.get("jira") or {}

JIRA_BASE_URL = (_JIRA_CFG.get("base_url") or "").rstrip("/")
JIRA_PROJECT_KEY = _JIRA_CFG.get("project_key") or None
JIRA_ISSUE_TYPE = _JIRA_CFG.get("issue_type") or "Task"

JIRA_USER = getattr(settings, "JIRA_USER", None) or os.getenv("ECLIPSE_JIRA_USER") or ""
JIRA_TOKEN = getattr(settings, "JIRA_API_TOKEN", None) or os.getenv("ECLIPSE_JIRA_TOKEN") or ""

JIRA_TEAMS_VALUE: Optional[str] = _JIRA_CFG.get("teams_field_id")
JIRA_REQUESTING_TEAM_VALUE: Optional[str] = _JIRA_CFG.get("requesting_team")
JIRA_REQUEST_TYPE_VALUE: Optional[str] = _JIRA_CFG.get("request_type")

JIRA_TEAMS_FIELD_KEY = "customfield_10129"
JIRA_REQUESTING_TEAM_FIELD_KEY = "customfield_14249"
JIRA_REQUEST_TYPE_FIELD_KEY = "customfield_10457"


def jira_enabled() -> bool:
    return bool(JIRA_BASE_URL and JIRA_PROJECT_KEY and JIRA_USER and JIRA_TOKEN)


async def jira_create_issue(summary: str, description: str) -> Optional[str]:
    if not jira_enabled():
        return None

    desc_doc = {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [
                    {
                        "type": "text",
                        "text": description,
                    }
                ],
            }
        ],
    }

    try:
        async with httpx.AsyncClient(
                timeout=15.0,
                auth=(JIRA_USER, JIRA_TOKEN),
        ) as client:
            url = f"{JIRA_BASE_URL}/rest/api/3/issue"

            fields: Dict[str, Any] = {
                "project": {"key": JIRA_PROJECT_KEY},
                "summary": summary,
                "issuetype": {"name": JIRA_ISSUE_TYPE},
                "description": desc_doc,
            }

            if JIRA_TEAMS_VALUE:
                fields[JIRA_TEAMS_FIELD_KEY] = [{"value": JIRA_TEAMS_VALUE}]

            if JIRA_REQUESTING_TEAM_VALUE:
                fields[JIRA_REQUESTING_TEAM_FIELD_KEY] = {"value": JIRA_REQUESTING_TEAM_VALUE}

            if JIRA_REQUEST_TYPE_VALUE:
                fields[JIRA_REQUEST_TYPE_FIELD_KEY] = {"value": JIRA_REQUEST_TYPE_VALUE}

            payload = {"fields": fields}
            resp = await client.post(url, json=payload)

            if resp.status_code >= 400:
                log_event(
                    "jira",
                    "create_http_error",
                    {
                        "status_code": resp.status_code,
                        "body": resp.text[:2000],
                    },
                )
                return None

            data = resp.json()
            key = data.get("key")
            log_event("jira", "issue_created", {"key": key})
            return key
    except Exception as e:
        log_event("jira", "create_error", {"error": str(e)})
        return None


async def jira_create_extension_issue(
        server: Dict[str, Any],
        server_id: str,
        expires_at: str,
        manager_uid: Optional[str],
        manager_name: str,
) -> Optional[str]:
    if not jira_enabled():
        return None

    owner_email = server.get("owner_email") or "unknown@jfrog.com"
    region = server.get("region") or server.get("cloud_region") or "unknown"
    runtime = server.get("runtime_hours")

    summary = f"Eclipse24: Extension approved for {server_id}"
    lines = [
        "Eclipse24 automatic tracking ticket for JPD extension.",
        "",
        f"Server ID: {server_id}",
        f"Owner: {owner_email}",
        f"Region: {region}",
        f"Runtime (h): {runtime}",
        f"Extension approved by: {manager_name} ({manager_uid or 'unknown'})",
        f"New expiry: {expires_at}",
    ]
    description = "\n".join(lines)

    key = await jira_create_issue(summary, description)
    return key