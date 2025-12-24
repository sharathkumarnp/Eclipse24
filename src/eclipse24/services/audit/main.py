# src/eclipse24/services/audit/main.py
import asyncio
import json
from typing import Any, Dict

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient, KafkaRecord
from eclipse24.libs.queue_config import topic_for

AUDIT_TOPIC = topic_for("audit")


async def _handle_audit(record: KafkaRecord) -> None:

    payload: Dict[str, Any]

    try:
        if isinstance(record.value, dict):
            payload = record.value
        else:
            payload = json.loads(record.value)
    except Exception as e:
        log_event(
            "audit_service",
            "parse_error",
            {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "error": str(e),
            },
        )
        return

    event = {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "key": record.key,
        "payload": payload,
    }

    log_event("audit_service", "event", event)


async def main() -> None:
    bus_audit = await KafkaClient().connect()
    group_id = f"{settings.KAFKA_GROUP_ID_PREFIX}.audit_db"

    log_event(
        "service",
        "start",
        {
            "service": "audit",
            "audit_topic": AUDIT_TOPIC,
            "group_id": group_id,
        },
    )

    await bus_audit.consume(
        AUDIT_TOPIC,
        _handle_audit,
        group_id=group_id,
        auto_offset_reset="earliest",
    )


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()