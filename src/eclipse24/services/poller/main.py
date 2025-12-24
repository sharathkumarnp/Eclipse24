import asyncio
import json
import random
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient
from eclipse24.libs.stores.narcissus_client import servers_with_flag
from eclipse24.libs.external.newrelic_client import NewRelicClient
from eclipse24.libs.queue_config import topic_for
from eclipse24.libs.stores.db import record_audit
from eclipse24.libs.slack.utils import (
    build_block_manager_card_blocks,
    _find_manager_id_for_email,
    build_owner_card_blocks,
)
from eclipse24.services.api.common import _SRE_MANAGER_CHANNEL

POLL_INTERVAL_MINUTES = 10

VALIDATION_TOPIC = topic_for("validation")
EXTENSIONS_TOPIC = topic_for("extensions")
BLOCK_TOPIC = topic_for("block")
WAITING_STATE_TOPIC = topic_for("waiting_state")

DEFAULT_FLAG = "is_chaos_enabled"

PERSIST = bool(settings.PERSIST_TO_DB and settings.PG_DSN)

slack_client = AsyncWebClient(token=getattr(settings, "SLACK_BOT_TOKEN", None))


def _sleep_seconds_with_jitter(base_minutes: int) -> int:
    base = base_minutes * 60
    jitter = int(base * 0.1)
    return base + random.randint(-jitter, jitter)


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "yes", "y", "on")


def _load_policy():
    rules = settings.load_rules() or {}
    config = rules.get("config") or {}

    use_newrelic = _to_bool(config.get("use_newrelic", False))
    narcissus_flag = (config.get("narcissus_flag") or DEFAULT_FLAG).strip() or DEFAULT_FLAG
    exemptions = config.get("exemptions") or {}
    excl_servers = set((exemptions.get("servers") or []))
    excl_emails = set((exemptions.get("emails") or []))

    expiry_cfg = config.get("expiry") or {}
    notice_before_hours = float(expiry_cfg.get("notice_before_hours") or 12.0)

    slack_cfg = config.get("slack") or {}
    primary_channel = (
            slack_cfg.get("approval_channel")
            or getattr(settings, "SLACK_APPROVAL_CHANNEL", None)
            or "#eclipse24-approvals"
    )
    fallback_channel = (
            slack_cfg.get("fallback_channel")
            or getattr(settings, "SLACK_FALLBACK_CHANNEL", None)
            or primary_channel
    )

    log_event(
        "poller",
        "rules_loaded",
        {
            "rules_path": str(settings.RULES_PATH),
            "use_newrelic": use_newrelic,
            "narcissus_flag": narcissus_flag,
            "exempt_servers_count": len(excl_servers),
            "exempt_emails_count": len(excl_emails),
            "notice_before_hours": notice_before_hours,
            "primary_channel": primary_channel,
            "fallback_channel": fallback_channel,
        },
    )
    return (
        use_newrelic,
        narcissus_flag,
        excl_servers,
        excl_emails,
        notice_before_hours,
        primary_channel,
        fallback_channel,
    )


async def _nr_fetch_all_internal_servers() -> List[str]:
    client = NewRelicClient()
    try:
        rows = await client.fetch_internal_servers()
        log_event(
            "poller",
            "nr_raw_results",
            {"count": len(rows), "region": client.region},
        )

        seen = set()
        servers: List[str] = []
        for row in rows:
            name = row.get("technicalServerName")
            if not name:
                continue
            if name in seen:
                continue
            seen.add(name)
            servers.append(name)

        log_event(
            "poller",
            "nr_servers_extracted",
            {
                "unique_count": len(servers),
                "region": client.region,
                "sample": servers[:10],
            },
        )
        return servers
    finally:
        await client.close()


async def _poll_narcissus(flag_name: str) -> List[str]:
    try:
        servers = servers_with_flag(flag_field=flag_name, state="active")
        log_event(
            "poller",
            "narcissus_servers",
            {"flag": flag_name, "count": len(servers)},
        )
        return servers
    except Exception as e:
        log_event("poller", "narcissus_error", {"flag": flag_name, "error": str(e)})
        return []


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


async def _consume_topic_snapshot(topic: str, short_seconds: float = 2.0) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    bus = await KafkaClient().connect()
    group = f"{settings.KAFKA_GROUP_ID_PREFIX}.poller.snapshot.{int(time.time()*1000)}"

    async def handler(record):
        try:
            v = record.value if isinstance(record.value, dict) else json.loads(record.value)
        except Exception:
            return
        sid = v.get("server_id") or (v.get("server") or {}).get("server_id")
        if not sid:
            return
        out[sid] = v

    task = asyncio.create_task(
        bus.consume(
            topic=topic,
            handler=handler,
            group_id=group,
            auto_offset_reset="earliest",
        )
    )
    try:
        await asyncio.sleep(short_seconds)
    finally:
        try:
            await bus.close()
        except Exception:
            pass
        try:
            task.cancel()
        except Exception:
            pass
    return out


async def _send_preexpiry_notice(
        name: str,
        ext: Dict[str, Any],
        waiting: Dict[str, Any],
        notice_before_hours: float,
        fallback_channel: str,
):

    server = ext.get("server") or {"server_id": name}
    expires_at_str = ext.get("expires_at") or ext.get("expiresAt") or ext.get("expired_at")
    messages = (settings.load_rules() or {}).get("config", {}).get("messages", {}) or {}
    pre_tpl = messages.get("expiry_pre_notice") or "Your server {server_id} will expire at {expires_at}."
    try:
        formatted = pre_tpl.format(server_id=name, expires_at=expires_at_str or "unknown")
    except Exception:
        formatted = pre_tpl

    # ✅ Use base owner card from slack utils (no buttons here)
    blocks = build_owner_card_blocks(
        server,
        status_line=formatted,
    )

    owner_email = (
            (server.get("owner_email") or server.get("ownerEmail") or "").strip().lower() or None
    )
    sent = False
    if owner_email:
        try:
            resp = await slack_client.users_lookupByEmail(email=owner_email)
            if resp.get("ok") and resp.get("user", {}).get("id"):
                owner_uid = resp["user"]["id"]
                open_resp = await slack_client.conversations_open(users=owner_uid)
                if open_resp.get("ok"):
                    ch = open_resp["channel"]["id"]
                    await slack_client.chat_postMessage(
                        channel=ch, text=formatted, blocks=blocks
                    )
                    log_event(
                        "poller",
                        "preexpiry_dm_sent",
                        {"server": name, "owner_uid": owner_uid, "channel": ch},
                    )
                    sent = True
        except SlackApiError as e:
            log_event(
                "poller",
                "preexpiry_dm_error",
                {"server": name, "email": owner_email, "error": str(e)},
            )

    if not sent:
        try:
            await slack_client.chat_postMessage(
                channel=fallback_channel, text=formatted, blocks=blocks
            )
            log_event(
                "poller", "preexpiry_channel_sent", {"server": name, "channel": fallback_channel}
            )
            sent = True
        except SlackApiError as e:
            log_event(
                "poller",
                "preexpiry_channel_error",
                {"server": name, "channel": fallback_channel, "error": str(e)},
            )

    if sent:
        try:
            bus = await KafkaClient().connect()
            try:
                await bus.produce(
                    WAITING_STATE_TOPIC,
                    {"server_id": name, "pre_expiry_notified": True},
                    key=name,
                )
                log_event("poller", "preexpiry_marked", {"server": name})
            finally:
                await bus.close()
        except Exception as e:
            log_event(
                "poller", "preexpiry_mark_error", {"server": name, "error": str(e)}
            )

        if PERSIST:
            try:
                await record_audit(
                    name,
                    "preexpiry_notified",
                    "poller",
                    json.dumps(
                        {
                            "notice_before_hours": notice_before_hours,
                            "expires_at": expires_at_str,
                        },
                        separators=(",", ":"),
                    ),
                )
            except Exception as e:
                log_event(
                    "poller", "preexpiry_audit_error", {"server": name, "error": str(e)}
                )


async def poll_and_publish() -> None:
    (
        use_newrelic,
        narcissus_flag,
        excl_servers,
        excl_emails,
        notice_before_hours,
        primary_channel,
        fallback_channel,
    ) = _load_policy()

    if use_newrelic:
        source = "newrelic"
        print("🔍 Polling New Relic for internal servers...")
        log_event("poller", "start_cycle", {"source": source})
        server_names = await _nr_fetch_all_internal_servers()
    else:
        source = "narcissus"
        print(f"🔍 Polling Narcissus for internal servers (flag='{narcissus_flag}')...")
        log_event("poller", "start_cycle", {"source": source, "flag": narcissus_flag})
        server_names = await _poll_narcissus(narcissus_flag)

    if not server_names:
        print("⚠️ No servers returned.")
        log_event("poller", "no_servers", {"source": source})
        return

    print("⏳ Building short snapshots from Kafka (extensions + waiting_state)")
    extensions_map: Dict[str, Dict[str, Any]] = {}
    waiting_map: Dict[str, Dict[str, Any]] = {}
    try:
        extensions_map = await _consume_topic_snapshot(EXTENSIONS_TOPIC, short_seconds=2.0)
    except Exception as e:
        log_event("poller", "extensions_snapshot_error", {"error": str(e)})
    try:
        waiting_map = await _consume_topic_snapshot(WAITING_STATE_TOPIC, short_seconds=1.0)
    except Exception as e:
        log_event("poller", "waiting_snapshot_error", {"error": str(e)})

    bus = await KafkaClient().connect()
    try:
        try:
            res = await bus.ensure_topic(VALIDATION_TOPIC, partitions=1)
            log_event(
                "kafka",
                "ensure_topic_result",
                {"topic": VALIDATION_TOPIC, "note": str(res)},
            )
        except Exception as e:
            log_event(
                "kafka",
                "ensure_topic_error",
                {"topic": VALIDATION_TOPIC, "error": str(e)},
            )

        print(f"✅ Found {len(server_names)} servers, processing with extension rules")
        log_event(
            "poller",
            "found",
            {"count": len(server_names), "topic": VALIDATION_TOPIC, "source": source},
        )

        for name in server_names:
            try:
                if name in excl_servers:
                    log_event("poller", "skip_exempt_server", {"server": name})
                    continue

                ext = extensions_map.get(name)
                waiting = waiting_map.get(name) or {}
                now = datetime.now(timezone.utc)

                if ext:
                    expires_at_str = (
                            ext.get("expires_at")
                            or ext.get("expiresAt")
                            or ext.get("expired_at")
                            or None
                    )
                    expires_at = _parse_iso(expires_at_str) if expires_at_str else None
                    s = ext.get("server") or {}
                    owner_email = (
                            (s.get("owner_email") or s.get("ownerEmail") or "")
                            .strip()
                            .lower()
                            or None
                    )
                    if owner_email and owner_email in excl_emails:
                        log_event(
                            "poller",
                            "skip_exempt_owner_email",
                            {"server": name, "email": owner_email},
                        )
                        continue

                    if expires_at and now < expires_at:
                        time_left = (expires_at - now).total_seconds()
                        notice_seconds = float(notice_before_hours) * 3600.0
                        pre_notified = bool(waiting.get("pre_expiry_notified") or False)
                        if time_left <= notice_seconds and not pre_notified:
                            await _send_preexpiry_notice(
                                name,
                                ext,
                                waiting,
                                notice_before_hours,
                                fallback_channel,
                            )
                            continue
                        log_event(
                            "poller",
                            "in_extension_skip",
                            {"server": name, "expires_at": expires_at_str},
                        )
                        continue

                    # Extension has expired
                    expired_block_emitted = bool(
                        waiting.get("expired_block_emitted") or False
                    )
                    if expired_block_emitted:
                        log_event(
                            "poller",
                            "expired_already_emitted",
                            {"server": name},
                        )
                        continue

                    expiry_block_requested = bool(
                        waiting.get("expiry_block_requested") or False
                    )
                    if expiry_block_requested:
                        log_event(
                            "poller",
                            "expiry_block_already_requested",
                            {"server": name},
                        )
                        continue

                    # Extension expired & no block approval requested yet → send SRE block card
                    server_obj = s or {"server_id": name}

                    mgr_id: Optional[str] = None
                    try:
                        mgr_id = await _find_manager_id_for_email(owner_email)
                    except Exception:
                        mgr_id = None

                    mention_line = f"<@{mgr_id}> " if mgr_id else "SRE team "

                    status_line = (
                        f"⏳ Extension expired for `{name}`. "
                        f"Please confirm whether to *block* or *keep running* this server."
                    )

                    value_envelope: Dict[str, Any] = {
                        "server": server_obj,
                        "owner_origin": None,
                        "manager": {"id": mgr_id} if mgr_id else {},
                    }

                    blocks = build_block_manager_card_blocks(
                        server_obj,
                        mention_line=mention_line,
                        status_line=status_line,
                        with_buttons=True,
                        value_envelope=value_envelope,
                    )

                    slack_ok = False
                    target_channel = _SRE_MANAGER_CHANNEL or primary_channel
                    try:
                        msg = await slack_client.chat_postMessage(
                            channel=target_channel,
                            text=status_line,
                            blocks=blocks,
                        )
                        slack_ok = bool(msg.get("ok"))
                        log_event(
                            "poller",
                            "expiry_block_request_sent",
                            {
                                "server": name,
                                "channel": target_channel,
                                "ok": slack_ok,
                            },
                        )
                    except Exception as e:
                        log_event(
                            "poller",
                            "expiry_block_request_error",
                            {"server": name, "error": str(e)},
                        )

                    if slack_ok:
                        try:
                            await bus.produce(
                                WAITING_STATE_TOPIC,
                                {
                                    "server_id": name,
                                    "expiry_block_requested": True,
                                    "expired_at": now.isoformat(),
                                },
                                key=name,
                            )
                            log_event(
                                "poller",
                                "waiting_state_marked_expiry_block_requested",
                                {"server": name},
                            )
                        except Exception as e:
                            log_event(
                                "poller",
                                "waiting_mark_error_expiry_block_requested",
                                {"server": name, "error": str(e)},
                            )

                        if PERSIST:
                            try:
                                await record_audit(
                                    name,
                                    "expiry_block_requested",
                                    "poller",
                                    json.dumps(
                                        {
                                            "reason": "extension_expired",
                                            "expired_at": now.isoformat(),
                                        },
                                        separators=(",", ":"),
                                    ),
                                )
                            except Exception as e:
                                log_event(
                                    "poller",
                                    "audit_record_error_expiry_block_requested",
                                    {"server": name, "error": str(e)},
                                )

                        continue

                    # If Slack failed, fall back to original behaviour: auto block
                    block_payload = {
                        "server": server_obj,
                        "reason": "expiry",
                        "expired_at": now.isoformat(),
                    }
                    try:
                        await bus.produce(BLOCK_TOPIC, block_payload, key=name)
                        log_event(
                            "poller",
                            "block_emitted",
                            {"server": name, "topic": BLOCK_TOPIC},
                        )
                    except Exception as e:
                        log_event(
                            "poller",
                            "block_emit_error",
                            {"server": name, "error": str(e)},
                        )
                        continue
                    try:
                        await bus.produce(
                            WAITING_STATE_TOPIC,
                            {
                                "server_id": name,
                                "expired_block_emitted": True,
                                "expired_at": now.isoformat(),
                            },
                            key=name,
                        )
                        log_event(
                            "poller",
                            "waiting_state_marked",
                            {"server": name},
                        )
                    except Exception as e:
                        log_event(
                            "poller",
                            "waiting_mark_error",
                            {"server": name, "error": str(e)},
                        )
                    if PERSIST:
                        try:
                            await record_audit(
                                name,
                                "expired_block_emitted",
                                "poller",
                                json.dumps(
                                    {
                                        "reason": "extension_expired_auto_block",
                                        "expired_at": now.isoformat(),
                                    },
                                    separators=(",", ":"),
                                ),
                            )
                        except Exception as e:
                            log_event(
                                "poller",
                                "audit_record_error",
                                {"server": name, "error": str(e)},
                            )
                    continue

                # No extension for this server: send to validator
                payload = {
                    "server_id": name,
                    "technicalServerName": name,
                    "source": source,
                }
                try:
                    res = await bus.produce(VALIDATION_TOPIC, payload, key=name)
                    log_event(
                        "poller",
                        "produce_ok",
                        {
                            "server": name,
                            "topic": res.topic,
                            "partition": res.partition,
                            "offset": res.offset,
                        },
                    )
                    if PERSIST:
                        try:
                            await record_audit(
                                name,
                                "queued_for_validation",
                                "poller",
                                json.dumps(
                                    {"source": source, "topic": VALIDATION_TOPIC},
                                    separators=(",", ":"),
                                ),
                            )
                        except Exception as e:
                            log_event(
                                "poller",
                                "audit_error",
                                {"server": name, "error": str(e)},
                            )
                except Exception as e:
                    log_event(
                        "poller",
                        "produce_error",
                        {"server": name, "error": str(e)},
                    )
            except Exception as e:
                log_event(
                    "poller",
                    "server_loop_error",
                    {"server": name, "error": str(e)},
                )

        log_event(
            "poller",
            "end_cycle",
            {"processed": len(server_names), "source": source},
        )
    finally:
        await bus.close()


async def scheduler() -> None:
    while True:
        print(f"\n⏰ Poller triggered at {time.strftime('%X')}")
        try:
            await poll_and_publish()
        except Exception as e:
            log_event("poller", "cycle_error", {"error": str(e)})
        sleep_for = _sleep_seconds_with_jitter(POLL_INTERVAL_MINUTES)
        print(f"🕒 Sleeping for {sleep_for}s...\n")
        await asyncio.sleep(sleep_for)


def main() -> None:
    print("🚀 Eclipse24 Poller (New Relic / Narcissus → Kafka)")
    try:
        asyncio.run(scheduler())
    except KeyboardInterrupt:
        print("👋 Poller stopped.")


if __name__ == "__main__":
    main()