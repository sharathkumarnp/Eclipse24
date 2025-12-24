import asyncio
import os

from eclipse24.libs.messaging.rmq import RMQClient


async def main():
    rmq_url = os.getenv("RMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
    print(f"➡️  Connecting to RMQ at: {rmq_url}")

    try:
        rmq = await RMQClient(rmq_url).connect()

        # 1) make sure queue exists
        await rmq.declare_queue("eclipse24.validation")

        # 2) publish a test message
        await rmq.publish(
            "eclipse24.validation",
            {
                "server_id": "demo-123",
                "source": "local-test",
                "uptime_hours": 72,
            },
        )
        print("✅ Test message sent to eclipse24.validation")

        await rmq.close()
    except Exception as e:
        print("❌ Error talking to RabbitMQ:", repr(e))


if __name__ == "__main__":
    asyncio.run(main())
