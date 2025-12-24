import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event

# ---------- Config ----------

CLOUD_USAGE_BASE = settings.CLOUD_USAGE_BASE.rstrip("/")
CLOUD_USAGE_API_KEY = settings.CLOUD_USAGE_API_KEY

STORE_DASHBOARD_BASE = settings.STORE_API_URL.rstrip("/")
STORE_USER = settings.STORE_USER
STORE_PASSWORD = settings.STORE_PASSWORD

# File is at: repo_root/src/internalserver.txt
INTERNAL_SERVERS_FILE = Path(__file__).resolve().parents[3] / "internalserver.txt"


def progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


async def fetch_cloud_usage(technical_servername: str) -> Dict[str, Any]:
    if not CLOUD_USAGE_API_KEY:
        raise RuntimeError("CLOUD_USAGE_API_KEY is not configured")

    url = f"{CLOUD_USAGE_BASE}/api/v1/monitor/server/{technical_servername}"
    headers = {"x-api-key": CLOUD_USAGE_API_KEY}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json() or {}


async def fetch_store_dashboard(technical_servername: str) -> List[Dict[str, Any]]:
    if not (STORE_USER and STORE_PASSWORD):
        raise RuntimeError("STORE_USER / STORE_PASSWORD not configured")
    if not STORE_DASHBOARD_BASE:
        raise RuntimeError("STORE_API_URL (STORE_DASHBOARD_BASE) not configured")

    url = f"{STORE_DASHBOARD_BASE}/dashboard/api/enterprise_plus/topology/{technical_servername}"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            url,
            auth=(STORE_USER, STORE_PASSWORD),
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()
        data = r.json()
        return data or []


async def resolve_email(server_input_name: str) -> Dict[str, Any]:
    """
    Resolve owner email for a server name (technical server / namespace).
    Uses the same cloud-usage + store-dashboard approach as validator.
    """
    name = server_input_name.strip()
    if not name:
        return {}

    # Step 1: cloud-usage to normalize technical name + get accountNumber
    try:
        cu = await fetch_cloud_usage(name)
    except Exception as e:
        return {
            "server_input": name,
            "error": f"cloud_usage_error: {e}",
        }

    cu_server = (cu.get("server") or {})
    cu_store = (cu_server.get("store") or {})
    cu_narc = (cu_server.get("narcissus") or {})

    technical_name = cu_store.get("technicalServerName") or name
    display_name = (
            cu_store.get("serverName")
            or cu_narc.get("jpdBaseName")
            or technical_name
    )
    account_number = cu_store.get("accountNumber")

    # Step 2: store dashboard to get email
    try:
        sd = await fetch_store_dashboard(technical_name)
    except Exception as e:
        return {
            "server_input": name,
            "server_id": technical_name,
            "server_name": display_name,
            "account_number": account_number,
            "error": f"store_dashboard_error: {e}",
        }

    owner_email: Optional[str] = None
    matched_row: Optional[Dict[str, Any]] = None

    if sd:
        # Prefer matching account number (same logic style as validator)
        if account_number is not None:
            for row in sd:
                try:
                    if int(row.get("accountNumber", 0)) == int(account_number):
                        matched_row = row
                        break
                except Exception:
                    pass

        if not matched_row:
            matched_row = sd[0]

        if isinstance(matched_row, dict):
            owner_email = matched_row.get("email") or matched_row.get("clientId")

    if not owner_email:
        return {
            "server_input": name,
            "server_id": technical_name,
            "server_name": display_name,
            "account_number": account_number,
            "error": "email_not_found",
        }

    return {
        "server_input": name,
        "server_id": technical_name,
        "server_name": display_name,
        "account_number": account_number,
        "owner_email": owner_email,
    }


async def main() -> None:
    if not INTERNAL_SERVERS_FILE.exists():
        raise FileNotFoundError(f"Internal servers file not found at {INTERNAL_SERVERS_FILE}")

    raw_lines = INTERNAL_SERVERS_FILE.read_text().splitlines()
    server_names = [ln.strip() for ln in raw_lines if ln.strip() and not ln.strip().startswith("#")]

    progress(f"Starting internal email resolver for {len(server_names)} servers (list: {INTERNAL_SERVERS_FILE})")

    results: List[Dict[str, Any]] = []
    unique_emails: Dict[str, int] = {}

    for idx, s in enumerate(server_names, start=1):
        progress(f"[{idx}/{len(server_names)}] Resolving owner email: {s}")

        res = await resolve_email(s)
        results.append(res)

        if res.get("error"):
            progress(f"   → ERROR: {res['error']}")
        else:
            email = str(res.get("owner_email"))
            unique_emails[email] = unique_emails.get(email, 0) + 1
            progress(f"   → {email}")

    summary = {
        "servers_count": len(server_names),
        "resolved_count": sum(1 for r in results if r.get("owner_email")),
        "failed_count": sum(1 for r in results if r.get("error")),
        "unique_emails_count": len(unique_emails),
        "unique_emails": sorted(unique_emails.keys()),
        "email_to_server_count": dict(sorted(unique_emails.items(), key=lambda x: (-x[1], x[0]))),
        "servers": results,
    }

    log_event(
        "internal_emails",
        "done",
        {
            "servers_count": summary["servers_count"],
            "resolved_count": summary["resolved_count"],
            "failed_count": summary["failed_count"],
            "unique_emails_count": summary["unique_emails_count"],
        },
    )

    # Final JSON to stdout (so you can redirect to a file)
    print(json.dumps(summary, indent=2, sort_keys=True))


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()