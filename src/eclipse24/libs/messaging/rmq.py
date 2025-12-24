import json
from typing import Optional, Callable, Awaitable

import aio_pika
from aio_pika import Message, DeliveryMode
from eclipse24.libs.core.config import settings


class RMQClient:
    def __init__(self, url: Optional[str] = None):
        self.url = url or settings.RMQ_URL or "amqp://guest:guest@localhost/"
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None

    async def connect(self) -> "RMQClient":
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        return self

    async def declare_queue(self, name: str, durable: bool = True):
        return await self.channel.declare_queue(name, durable=durable)

    async def publish(self, queue: str, payload: dict):
        if not self.channel:
            raise RuntimeError("Channel not initialized. Call connect() first.")
        body = json.dumps(payload).encode()
        msg = Message(body, content_type="application/json", delivery_mode=DeliveryMode.PERSISTENT)
        await self.channel.default_exchange.publish(msg, routing_key=queue)

    async def consume(self, queue_name: str, callback: Callable[[aio_pika.IncomingMessage], Awaitable[None]]):
        if not self.channel:
            raise RuntimeError("Channel not initialized. Call connect() first.")
        queue = await self.declare_queue(queue_name)
        await queue.consume(callback)
        print(f"✅ Consuming messages from: {queue_name}")

    async def close(self):
        if self.connection:
            await self.connection.close()
