from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from aio_pika import IncomingMessage  # type: ignore  # keep for signature parity

# We keep the callback type identical to your RMQ consumer to minimize changes.
ConsumerCallback = Callable[[IncomingMessage], Awaitable[None]]


@dataclass
class ProduceResult:
    topic: str
    partition: int
    offset: int


class MessagingClient:
    async def connect(self) -> "MessagingClient":
        raise NotImplementedError

    async def produce(self, topic: str, payload: dict) -> ProduceResult:
        raise NotImplementedError

    async def consume(self, topic: str, callback: ConsumerCallback, group_id: Optional[str] = None) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
