# src/eclipse24/libs/queue_config.py
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Optional

import yaml

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event


@dataclass
class TopicConfig:
    logical_name: str
    topic: str
    partitions: int
    replication_factor: int


@lru_cache(maxsize=1)
def _load_raw() -> Dict:

    path = getattr(settings, "QUEUES_PATH", None)
    if not path:
        raise RuntimeError("QUEUES_PATH not set in settings/env")

    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}

    return data


@lru_cache(maxsize=1)
def _all_topics() -> Dict[str, TopicConfig]:
    data = _load_raw()
    kafka_cfg = data.get("kafka", {})
    defaults = kafka_cfg.get("default", {}) or {}
    topics = kafka_cfg.get("topics", {}) or {}

    def_partitions = int(defaults.get("partitions", 1))
    def_replication = int(defaults.get("replication_factor", 1))

    out: Dict[str, TopicConfig] = {}
    for logical_name, t_cfg in topics.items():
        topic_name = t_cfg.get("topic")
        if not topic_name:
            log_event("queue_config", "missing_topic_name", {"logical": logical_name})
            continue

        partitions = int(t_cfg.get("partitions", def_partitions))
        replication = int(t_cfg.get("replication_factor", def_replication))

        out[logical_name] = TopicConfig(
            logical_name=logical_name,
            topic=topic_name,
            partitions=partitions,
            replication_factor=replication,
        )

    return out


def topic_for(logical_name: str) -> str:

    topics = _all_topics()
    if logical_name not in topics:
        raise KeyError(f"Unknown logical topic '{logical_name}' in queues.yaml")
    return topics[logical_name].topic


def topic_config_for_topic(topic_name: str) -> Optional[TopicConfig]:

    for cfg in _all_topics().values():
        if cfg.topic == topic_name:
            return cfg
    return None


def all_topic_configs() -> Dict[str, TopicConfig]:

    return _all_topics().copy()