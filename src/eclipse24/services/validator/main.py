# src/eclipse24/services/validator/main.py
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple

import httpx
from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient, KafkaRecord
from eclipse24.libs.stores.db import save_server, set_state, get_state
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.stores.narcissus_client import (
    servers_with_flag as narcissus_servers_with_flag,
)

# UmbrellaCost (cost + usage)
try:
    from eclipse24.libs.external.umbrellacost_client import UmbrellaCostClient
except Exception:
    UmbrellaCostClient = None  # type: ignore

VALIDATION_TOPIC = topic_for("validation")
WAITING_TOPIC = topic_for("waiting")
AUDIT_TOPIC = topic_for("audit")

# DB toggle
PERSIST = bool(settings.PERSIST_TO_DB and settings.PG_DSN)

CLOUD_USAGE_BASE = settings.CLOUD_USAGE_BASE.rstrip("/")
CLOUD_USAGE_API_KEY = settings.CLOUD_USAGE_API_KEY
STORE_DASHBOARD_BASE = settings.STORE_API_URL.rstrip("/")
STORE_USER = settings.STORE_USER
STORE_PASSWORD = settings.STORE_PASSWORD

# UmbrellaCost client singleton
_UMBRELLA = UmbrellaCostClient() if UmbrellaCostClient else None


def _load_policy() -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    rules = settings.load_rules() or {}
    config = rules.get("config") or {}
    validation_rules = config.get("validation") or {}
    exemptions = config.get("exemptions") or {}
    dry_run = bool(config.get("dry_run", settings.DRY_RUN))
    return validation_rules, exemptions, dry_run


def _parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _runtime_hours_from_creation(creation: Optional[str]) -> Optional[int]:
    if not creation:
        return None
    dt = _parse_iso(creation)
    if not dt:
        return None
    return int((datetime.now(timezone.utc) - dt).total_seconds() // 3600)


def _cloud_from_region(region: Optional[str]) -> str:
    r = (region or "").lower()
    if "gcprod" in r or "gcp" in r:
        return "gcp"
    if "azprod" in r or "azure" in r:
        return "azure"
    if "prod" in r or "aws" in r:
        return "aws"
    return "aws"


async def fetch_cloud_usage(technical_servername: str) -> Dict[str, Any]:
    if not CLOUD_USAGE_API_KEY:
        return {}
    url = f"{CLOUD_USAGE_BASE}/api/v1/monitor/server/{technical_servername}"
    headers = {"x-api-key": CLOUD_USAGE_API_KEY}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            return r.json() or {}
        except Exception as e:
            log_event(
                "validator",
                "cloud_usage_error",
                {"server": technical_servername, "error": str(e)},
            )
            return {}


async def fetch_store_dashboard(technical_servername: str) -> List[Dict[str, Any]]:
    if not (STORE_USER and STORE_PASSWORD):
        return []
    url = f"{STORE_DASHBOARD_BASE}/dashboard/api/enterprise_plus/topology/{technical_servername}"
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(
                url,
                auth=(STORE_USER, STORE_PASSWORD),
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            return r.json() or []
        except Exception as e:
            log_event(
                "validator",
                "store_error",
                {"server": technical_servername, "error": str(e)},
            )
            return []


async def fetch_umbrellacost(
        technical_servername: str,
        region: Optional[str],
        creation_date: Optional[str],
) -> Dict[str, Any]:
    if not _UMBRELLA:
        return {}

    cloud = _cloud_from_region(region)
    now = datetime.now(timezone.utc)

    end_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_dt.date().isoformat()

    start_dt: Optional[datetime] = None
    if creation_date:
        start_dt = _parse_iso(creation_date)
    if not start_dt:
        start_dt = end_dt - timedelta(days=7)

    if start_dt > end_dt:
        start_dt = end_dt

    start_date = start_dt.date().isoformat()

    if start_date > end_date:
        log_event(
            "umbrellacost",
            "date_range_invalid",
            {
                "server": technical_servername,
                "region": region,
                "cloud": cloud,
                "start_date_before": start_date,
                "end_date_before": end_date,
            },
        )
        start_date = end_date

    try:
        cost_obj = await _UMBRELLA.get_cost_for_namespace(
            namespace=technical_servername,
            cloud=cloud,
            start_date=start_date,
            end_date=end_date,
            granularity="daily",
        )

        total: Optional[float] = None
        currency = "USD"

        if isinstance(cost_obj, dict):
            if isinstance(cost_obj.get("total_cost"), (int, float)):
                total = float(cost_obj["total_cost"])
            elif isinstance(cost_obj.get("totalCost"), (int, float)):
                total = float(cost_obj["totalCost"])

            if isinstance(cost_obj.get("currency"), str):
                currency = cost_obj["currency"]
            elif isinstance(cost_obj.get("currencyCode"), str):
                currency = cost_obj["currencyCode"]

            if total is None:
                results = cost_obj.get("results") or cost_obj.get("data") or []
                if isinstance(results, list) and results:
                    acc = 0.0
                    found_any = False
                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        c = item.get("cost")
                        if not isinstance(c, (int, float)):
                            c = item.get("totalCost")
                        if isinstance(c, (int, float)):
                            acc += float(c)
                            found_any = True
                        if isinstance(item.get("currency"), str):
                            currency = item["currency"]
                        elif isinstance(item.get("currencyCode"), str):
                            currency = item["currencyCode"]
                    if found_any:
                        total = acc

        elif isinstance(cost_obj, str):
            log_event(
                "umbrellacost",
                "string_response",
                {
                    "server": technical_servername,
                    "region": region,
                    "cloud": cloud,
                    "start_date": start_date,
                    "end_date": end_date,
                    "snippet": cost_obj[:200],
                },
            )
            return {}
        else:
            log_event(
                "umbrellacost",
                "unexpected_type",
                {
                    "server": technical_servername,
                    "region": region,
                    "cloud": cloud,
                    "start_date": start_date,
                    "end_date": end_date,
                    "type": str(type(cost_obj)),
                },
            )
            return {}

        if total is None:
            log_event(
                "umbrellacost",
                "no_cost",
                {
                    "server": technical_servername,
                    "region": region,
                    "cloud": cloud,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            return {}

        est_cost = f"{currency} {total:.2f}"
        usage_hint = "k8s-cost"
        if isinstance(cost_obj, dict):
            uh = cost_obj.get("usage_hint")
            if isinstance(uh, str):
                usage_hint = uh

        return {
            "est_cost": est_cost,
            "usage": usage_hint,
        }
    except Exception as e:
        log_event(
            "umbrellacost",
            "error",
            {
                "server": technical_servername,
                "region": region,
                "cloud": cloud,
                "error": str(e),
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return {}


def apply_exemptions(
        exemptions: Dict[str, Any], server_name: str, owner_email: Optional[str]
) -> bool:
    if server_name in (exemptions.get("servers") or []):
        return True
    if owner_email and owner_email in (exemptions.get("emails") or []):
        return True
    return False


def passes_rules(
        validation_rules: Dict[str, Any],
        owner_email: Optional[str],
        status: Optional[str],
        runtime_hours: Optional[int],
) -> bool:
    domains = (validation_rules.get("email_domain") or {}).get(
        "allow_domains", []
    )
    if owner_email:
        if not any(owner_email.endswith(f"@{d}") for d in domains):
            return False
    else:
        return False

    allowed_status = (validation_rules.get("status") or {}).get(
        "allowed_values", []
    )
    if status and allowed_status and status.lower() not in [
        s.lower() for s in allowed_status
    ]:
        return False

    min_hours_raw = (validation_rules.get("runtime") or {}).get(
        "min_hours", 0
    )
    try:
        min_hours = float(min_hours_raw)
    except Exception:
        min_hours = 0.0
    if runtime_hours is None or float(runtime_hours) < min_hours:
        return False

    return True


def narcissus_excluded_servers() -> set[str]:
    """
    Fetch servers that have the Narcissus flag `newrelic_exclude_internal_server_tag`.
    """
    flag = "newrelic_exclude_internal_server_tag"
    try:
        names = narcissus_servers_with_flag(flag, state="active")
        names = names or []
        log_event(
            "validator",
            "narcissus_exclude_refresh",
            {"flag": flag, "count": len(names)},
        )
        return {str(n).lower() for n in names}
    except Exception as e:
        log_event(
            "validator",
            "narcissus_exclude_error",
            {"flag": flag, "error": str(e)},
        )
        return set()


async def build_server_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    technical = (
            raw.get("technicalServerName")
            or raw.get("server_id")
            or raw.get("serverName")
    )

    cu = await fetch_cloud_usage(technical)
    cu_server = (cu.get("server") or {})
    cu_store = (cu_server.get("store") or {})
    cu_narc = (cu_server.get("narcissus") or {})

    technical_name = cu_store.get("technicalServerName") or technical
    display_name = (
            cu_store.get("serverName")
            or cu_narc.get("jpdBaseName")
            or technical_name
    )
    status = cu_store.get("status") or (
        cu_narc.get("state") if isinstance(cu_narc.get("state"), str) else None
    )
    creation_date = cu_store.get("creationDate") or cu_narc.get("createdAt")
    account_number = cu_store.get("accountNumber")
    region = cu_store.get("region") or cu_narc.get("region")

    runtime_hours = _runtime_hours_from_creation(creation_date)

    sd = await fetch_store_dashboard(technical_name)
    owner_email = None
    if sd:
        match = None
        for row in sd:
            try:
                if account_number and int(row.get("accountNumber", 0)) == int(
                        account_number
                ):
                    match = row
                    break
            except Exception:
                pass
        if not match:
            match = sd[0]
        owner_email = match.get("email") or match.get("clientId")

    umb = await fetch_umbrellacost(technical_name, region, creation_date)

    return {
        "server_id": technical_name,
        "server_name": display_name,
        "owner_email": owner_email,
        "status": status,
        "creation_date": creation_date,
        "runtime_hours": runtime_hours,
        "account_number": account_number,
        "region": region,
        "usage": umb.get("usage") or "n/a",
        "est_cost": umb.get("est_cost") or "$0.00",
    }


def _state_suppresses(row: Optional[Dict[str, Any]]) -> bool:
    """
    Decide if DB state should suppress *new* validation / Slack notices.

    States we ignore:
      - PENDING                 → already in approval flow
      - PENDING_BLOCK_APPROVAL  → manager deciding block
      - EXTENDED (until>now)    → in an active extension window
      - KEEP_RUNNING            → manager explicitly said "keep running"
      - BLOCKED                 → already blocked, nothing more to do
    """
    if not row:
        return False

    st = (row.get("status") or "").upper()
    now = datetime.now(timezone.utc)

    # Already waiting in some approval or extension flow
    if st in {"PENDING", "PENDING_BLOCK_APPROVAL"}:
        return True

    # Actively extended, and extension not yet expired
    if st == "EXTENDED":
        u = row.get("until")
        if isinstance(u, datetime) and now < u:
            return True

    # Explicit permanent decisions
    if st in {"KEEP_RUNNING", "BLOCKED"}:
        return True

    return False


def _audit_payload(
        action: str,
        *,
        server_id: Optional[str] = None,
        reason: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Builds a standard audit event for validator actions."""
    base: Dict[str, Any] = {
        "component": "validator",
        "action": action,
        "server_id": server_id,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        base["details"] = extra
    return base


async def _on_validation(
        record: KafkaRecord, bus_waiting: KafkaClient, bus_audit: KafkaClient
):
    validation_rules, exemptions, dry_run = _load_policy()

    try:
        payload = (
            record.value
            if isinstance(record.value, dict)
            else json.loads(record.value)
        )
    except Exception:
        log_event("validate", "failed", {"reason": "invalid_json"})
        await bus_audit.produce(
            AUDIT_TOPIC,
            _audit_payload(
                "validation_invalid_json",
                reason="invalid_json",
                extra={
                    "raw_topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            ),
        )
        return

    server_id_hint = payload.get("server_id") or payload.get("technicalServerName")
    if not server_id_hint:
        log_event("validate", "skipped", {"reason": "missing_server_id"})
        await bus_audit.produce(
            AUDIT_TOPIC,
            _audit_payload(
                "validation_skipped",
                reason="missing_server_id",
                extra={
                    "raw_topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            ),
        )
        return

    server = await build_server_metadata(payload)
    owner_email = server.get("owner_email")
    status = server.get("status")
    runtime_hours = server.get("runtime_hours")
    server_name = server.get("server_id")

    # Narcissus flag-based exclusion:

    try:
        excluded_servers = narcissus_excluded_servers()
        if server_name and str(server_name).lower() in excluded_servers:
            flag = "newrelic_exclude_internal_server_tag"
            log_event(
                "validate",
                "narcissus_exclude_flag",
                {
                    "server": server_name,
                    "flag": flag,
                },
            )
            await bus_audit.produce(
                AUDIT_TOPIC,
                _audit_payload(
                    "validation_excluded_by_narcissus_flag",
                    server_id=server_name,
                    reason="narcissus_exclude_flag",
                    extra={"flag": flag},
                ),
            )
            return
    except Exception as e:
        # Don’t fail validation if this check explodes, just log and continue
        log_event(
            "validate",
            "narcissus_exclude_check_error",
            {"server": server_name, "error": str(e)},
        )

    if apply_exemptions(exemptions, server_name, owner_email):
        log_event(
            "validate",
            "exempted",
            {"server": server_name, "email": owner_email},
        )
        await bus_audit.produce(
            AUDIT_TOPIC,
            _audit_payload(
                "validation_exempted",
                server_id=server_name,
                reason="matches_exemptions",
                extra={"owner_email": owner_email},
            ),
        )
        return

    if passes_rules(validation_rules, owner_email, status, runtime_hours):
        if PERSIST:
            row = await get_state(server_name)
            if _state_suppresses(row):
                log_event(
                    "validate",
                    "suppressed_db_state",
                    {
                        "server": server_name,
                        "db_status": (row.get("status") if row else None),
                        "until": (
                            row.get("until").isoformat()
                            if row and row.get("until")
                            else None
                        ),
                    },
                )
                await bus_audit.produce(
                    AUDIT_TOPIC,
                    _audit_payload(
                        "validation_suppressed",
                        server_id=server_name,
                        reason="db_state_suppresses",
                        extra={
                            "db_status": (row.get("status") if row else None)
                            if row
                            else None,
                            "until": (
                                row.get("until").isoformat()
                                if row and row.get("until")
                                else None
                            ),
                        },
                    ),
                )
                return

        if PERSIST:
            await save_server(server)
            await set_state(server_name, "PENDING", None)

        server["dry_run"] = dry_run

        await bus_waiting.produce(WAITING_TOPIC, server, key=server_name)
        log_event(
            "validate",
            "passed",
            {
                "server": server_name,
                "owner_email": owner_email,
                "status": status,
                "runtime": runtime_hours,
                "est_cost": server.get("est_cost"),
                "usage": server.get("usage"),
                "dry_run": dry_run,
            },
        )
        await bus_audit.produce(
            AUDIT_TOPIC,
            _audit_payload(
                "validation_passed",
                server_id=server_name,
                extra={
                    "owner_email": owner_email,
                    "status": status,
                    "runtime_hours": runtime_hours,
                    "est_cost": server.get("est_cost"),
                    "usage": server.get("usage"),
                    "dry_run": dry_run,
                },
            ),
        )
    else:
        log_event(
            "validate",
            "failed_rules",
            {
                "server": server_name,
                "status": status,
                "email": owner_email,
                "runtime": runtime_hours,
            },
        )
        await bus_audit.produce(
            AUDIT_TOPIC,
            _audit_payload(
                "validation_failed_rules",
                server_id=server_name,
                reason="rules_not_passed",
                extra={
                    "owner_email": owner_email,
                    "status": status,
                    "runtime_hours": runtime_hours,
                },
            ),
        )


async def main():
    bus_waiting = await KafkaClient().connect()
    bus_validation = await KafkaClient().connect()
    bus_audit = await KafkaClient().connect()

    group_validation = f"{settings.KAFKA_GROUP_ID_PREFIX}.validator"

    log_event(
        "service",
        "start",
        {
            "service": "validator",
            "validation_topic": VALIDATION_TOPIC,
            "waiting_topic": WAITING_TOPIC,
            "audit_topic": AUDIT_TOPIC,
        },
    )

    await bus_validation.consume(
        VALIDATION_TOPIC,
        lambda rec: _on_validation(rec, bus_waiting, bus_audit),
        group_id=group_validation,
        auto_offset_reset="earliest",
    )


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()