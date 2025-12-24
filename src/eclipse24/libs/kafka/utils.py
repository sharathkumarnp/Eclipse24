from typing import Any, Dict, Optional

from eclipse24.libs.core.logging import log_event
from eclipse24.libs.messaging.kafka_client import KafkaClient


async def kafka_produce(
        topic: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
) -> None:
    bus = await KafkaClient().connect()
    try:
        if key is not None:
            await bus.produce(topic, payload, key=key)
        else:
            await bus.produce(topic, payload)
        meta: Dict[str, Any] = {"topic": topic}
        if key is not None:
            meta["key"] = key
        log_event("kafka_produce", "ok", meta)
    finally:
        await bus.close()


async def kafka_produce_compacted(
        topic: str,
        server_id: str,
        fields: Dict[str, Any],
) -> None:
    value: Dict[str, Any] = {"server_id": server_id}
    value.update(fields)
    await kafka_produce(topic, value, key=server_id)
    log_event(
        "kafka_compacted_state",
        "ok",
        {
            "topic": topic,
            "server_id": server_id,
            "keys": list(fields.keys()),
        },
    )