# src/eclipse24/libs/messaging/kafka_client.py
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event
from eclipse24.libs.queue_config import topic_config_for_topic, all_topic_configs


@dataclass
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Any  # already decoded JSON


def _encode(v: Any) -> bytes:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _decode(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def _oauth_cb(_oauth_config):

    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(settings.KAFKA_REGION)
    return token, expiry_ms / 1000.0


class KafkaClient:

    _producer_shared: Optional[Producer] = None
    _producer_lock: asyncio.Lock = asyncio.Lock()

    def __init__(self):
        self._consumer: Optional[Consumer] = None

        self._brokers = getattr(settings, "KAFKA_BROKERS", "localhost:9092")
        self._sec_proto = getattr(settings, "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self._sasl_mech = (getattr(settings, "KAFKA_SASL_MECHANISM", "") or "").upper()
        self._sasl_user = getattr(settings, "KAFKA_SASL_USERNAME", None)
        self._sasl_pass = getattr(settings, "KAFKA_SASL_PASSWORD", None)
        self._client_id = getattr(settings, "KAFKA_CLIENT_ID", "eclipse24")
        self._group_prefix = getattr(settings, "KAFKA_GROUP_ID_PREFIX", "eclipse24")
        self._region = getattr(settings, "KAFKA_REGION", "ap-south-1")
        self._auto_create = getattr(settings, "KAFKA_AUTO_CREATE_TOPICS", True)

        base_conf = {
            "bootstrap.servers": self._brokers,
            "client.id": self._client_id,
            "socket.timeout.ms": 20000,        # 20s
            "request.timeout.ms": 20000,
            "message.timeout.ms": 30000,
            "metadata.max.age.ms": 15000,
            "retries": 3,
            "retry.backoff.ms": 300,
            "linger.ms": 5,
            "queue.buffering.max.ms": 50,
        }

        if self._sec_proto and self._sec_proto != "PLAINTEXT":
            base_conf["security.protocol"] = self._sec_proto

        if self._sasl_mech:
            base_conf["sasl.mechanism"] = self._sasl_mech
            if self._sasl_mech == "OAUTHBEARER":
                base_conf["oauth_cb"] = _oauth_cb
            else:
                if self._sasl_user:
                    base_conf["sasl.username"] = self._sasl_user
                if self._sasl_pass:
                    base_conf["sasl.password"] = self._sasl_pass

        self._base_conf = base_conf


    def _admin(self) -> AdminClient:
        return AdminClient(self._base_conf)

    async def ensure_topic(self, topic: str, partitions: int = 1, replication: int = 1):

        if not self._auto_create:
            log_event("kafka", "ensure_topic_skip", {"topic": topic})
            return

        cfg = topic_config_for_topic(topic)
        if cfg is not None:
            partitions = cfg.partitions
            replication = cfg.replication_factor

        try:
            admin = self._admin()
            new_topic = NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=replication,
            )
            fs = admin.create_topics([new_topic])
            for t, f in fs.items():
                try:
                    f.result(timeout=5)
                    log_event(
                        "kafka",
                        "ensure_topic_ok",
                        {"topic": t, "partitions": partitions, "replication": replication},
                    )
                except Exception as e:
                    log_event("kafka", "ensure_topic_result", {"topic": t, "note": str(e)})
        except Exception as e:
            log_event("kafka", "ensure_topic_err", {"topic": topic, "error": repr(e)})

    async def ensure_all_topics(self):

        if not self._auto_create:
            log_event("kafka", "ensure_all_topics_skip", {})
            return

        cfgs = all_topic_configs().values()
        if not cfgs:
            log_event("kafka", "ensure_all_topics_no_config", {})
            return

        try:
            admin = self._admin()
            new_topics = [
                NewTopic(
                    cfg.topic,
                    num_partitions=cfg.partitions,
                    replication_factor=cfg.replication_factor,
                )
                for cfg in cfgs
            ]
            fs = admin.create_topics(new_topics)
            for t, f in fs.items():
                try:
                    f.result(timeout=5)
                    log_event("kafka", "ensure_all_topics_ok", {"topic": t})
                except Exception as e:
                    log_event("kafka", "ensure_all_topics_result", {"topic": t, "note": str(e)})
        except Exception as e:
            log_event("kafka", "ensure_all_topics_err", {"error": repr(e)})


    async def connect(self) -> "KafkaClient":

        if KafkaClient._producer_shared is None:
            async with KafkaClient._producer_lock:
                if KafkaClient._producer_shared is None:
                    KafkaClient._producer_shared = Producer(self._base_conf)
                    log_event("kafka", "producer_started", {"brokers": self._brokers})
        return self

    async def close(self):

        if self._consumer is not None:
            try:
                self._consumer.close()
            finally:
                self._consumer = None
            log_event("kafka", "consumer_stopped")

    @classmethod
    async def close_shared(cls, flush_timeout: float = 5.0):

        if cls._producer_shared is not None:
            try:
                cls._producer_shared.flush(timeout=flush_timeout)
            finally:
                cls._producer_shared = None
            log_event("kafka", "producer_stopped", {})

    async def produce(self, topic: str, payload: Any, key: Optional[str] = None) -> KafkaRecord:

        if KafkaClient._producer_shared is None:
            raise RuntimeError("Producer not started. Call connect() first.")

        await self.ensure_topic(topic)

        attempts = 3
        last_exc: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            fut: asyncio.Future = asyncio.get_event_loop().create_future()

            def _delivery(err, msg):
                if err:
                    status = "error"
                    meta = {
                        "topic": topic,
                        "code": getattr(err, "code", lambda: None)(),
                        "retriable": isinstance(err, KafkaError) and err.retriable(),
                        "fatal": isinstance(err, KafkaError) and err.fatal(),
                        "attempt": attempt,
                        "error": str(err),
                    }
                    log_event("kafka_produce", status, meta)
                    if not fut.done():
                        fut.set_exception(KafkaException(err))
                else:
                    meta = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}
                    log_event("kafka_produce", "ok", meta)
                    if not fut.done():
                        fut.set_result(
                            KafkaRecord(
                                topic=msg.topic(),
                                partition=msg.partition(),
                                offset=msg.offset(),
                                key=key,
                                value=payload,
                            )
                        )

            try:
                KafkaClient._producer_shared.produce(
                    topic,
                    _encode(payload),
                    key=(key.encode() if key else None),
                    callback=_delivery,
                )
            except BufferError as e:
                log_event("kafka_produce", "buffer_full", {"attempt": attempt, "error": str(e)})
                KafkaClient._producer_shared.poll(0.5)

            deadline = time.time() + 20.0
            while not fut.done() and time.time() < deadline:
                KafkaClient._producer_shared.poll(0.2)
                await asyncio.sleep(0.05)

            if fut.done():
                try:
                    return await fut
                except Exception as e:
                    last_exc = e
                    await asyncio.sleep(0.3 * attempt)
                    continue
            else:
                last_exc = TimeoutError("delivery timeout")
                log_event("kafka_produce", "timeout", {"topic": topic, "attempt": attempt})
                await asyncio.sleep(0.3 * attempt)

        raise last_exc or RuntimeError("produce failed")


    async def consume(
            self,
            topic: str,
            handler: Callable[[KafkaRecord], Awaitable[None]],
            *,
            group_id: Optional[str] = None,
            auto_offset_reset: str = "latest",
            enable_auto_commit: bool = True,
    ):

        gid = group_id or f"{self._group_prefix}.{topic}"
        conf = {
            **self._base_conf,
            "group.id": gid,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "socket.timeout.ms": 30000,
            "session.timeout.ms": 15000,
            "max.poll.interval.ms": 300000,
        }

        self._consumer = Consumer(conf)
        self._consumer.subscribe([topic])
        log_event("kafka", "consumer_started", {"topic": topic, "group": gid})

        try:
            while True:
                try:
                    msg = self._consumer.poll(1.0)
                except KafkaException as e:
                    log_event("kafka_consume", "poll_exception", {"topic": topic, "error": repr(e)})
                    await asyncio.sleep(0.5)
                    continue

                if msg is None:
                    await asyncio.sleep(0.1)
                    continue

                if msg.error():
                    ke = msg.error()
                    log_event(
                        "kafka_consume",
                        "error",
                        {
                            "topic": topic,
                            "code": getattr(ke, "code", lambda: None)(),
                            "retriable": ke.retriable() if hasattr(ke, "retriable") else None,
                            "fatal": ke.fatal() if hasattr(ke, "fatal") else None,
                            "error": str(ke),
                        },
                    )
                    if hasattr(ke, "fatal") and ke.fatal():
                        break
                    continue

                try:
                    record = KafkaRecord(
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        key=(msg.key().decode() if msg.key() else None),
                        value=_decode(msg.value()),
                    )
                except Exception as e:
                    log_event(
                        "kafka_consume",
                        "decode_error",
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "error": repr(e),
                        },
                    )
                    continue

                try:
                    await handler(record)
                except Exception as e:
                    log_event(
                        "kafka_consume",
                        "handler_error",
                        {
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                            "error": repr(e),
                        },
                    )
        finally:
            await self.close()