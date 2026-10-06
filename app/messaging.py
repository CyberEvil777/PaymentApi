import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from pamqp.commands import Basic
from sqlalchemy import select

from app.models import Outbox
from app.processing import error_summary, utcnow

log = logging.getLogger(__name__)
NEW = RabbitQueue("payments.new", durable=True)
DLQ = RabbitQueue("payments.dlq", durable=True)


def make_broker(url: str):
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=25,
    )


async def declare_queues(broker):
    await broker.declare_queue(NEW)
    await broker.declare_queue(DLQ)


async def confirmed_publish(broker, payload, routing_key, message_id):
    confirmation = await broker.publish(
        payload,
        routing_key=routing_key,
        persist=True,
        mandatory=True,
        message_id=str(message_id),
        timeout=10,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("Broker did not return a publisher ACK")


class OutboxPublisher:
    def __init__(self, sessions, broker, now=utcnow):
        self.sessions = sessions
        self.broker = broker
        self.now = now

    async def publish_one(self) -> bool:
        async with self.sessions.begin() as session:
            event = await session.scalar(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= self.now())
                .order_by(Outbox.available_at, Outbox.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False
            event.publish_attempts += 1
            try:
                await confirmed_publish(self.broker, event.payload, event.routing_key, event.id)
            except Exception as exc:
                event.last_error = error_summary(exc)
                event.available_at = self.now() + timedelta(
                    seconds=min(30, 2 ** min(event.publish_attempts, 5))
                )
                log.warning(
                    "payment_id=%s attempt=%s publication_error=%s",
                    event.payment_id,
                    event.payload.get("attempt"),
                    event.last_error,
                )
            else:
                event.published_at = self.now()
                event.last_error = None
        return True

    async def run(self, stop: asyncio.Event, poll_seconds: float):
        while not stop.is_set():
            try:
                if await self.publish_one():
                    continue
            except Exception:
                log.exception("Outbox database failure; event remains pending")
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
            except TimeoutError:
                pass
