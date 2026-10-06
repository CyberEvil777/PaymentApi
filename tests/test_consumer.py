import asyncio
import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, PropertyMock
from uuid import uuid4

import pytest
from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError

from app.consumer import register_handler
from app.messaging import make_broker
from app.processing import InvalidPaymentEvent, PaymentEvent


@pytest.fixture
def worker(monkeypatch):
    broker = make_broker("amqp://guest:guest@localhost/")
    processor = AsyncMock()
    stop, failed = asyncio.Event(), asyncio.Event()
    message = AsyncMock()
    message.message_id = "stable-amqp-id"
    message.raw_message = SimpleNamespace(channel=object())
    publish = AsyncMock()
    monkeypatch.setattr("app.consumer.confirmed_publish", publish)
    monkeypatch.setattr("app.consumer.pause_until_retry", AsyncMock())
    handle = register_handler(broker, processor, stop, failed=failed)
    event = PaymentEvent(payment_id=uuid4(), attempt=1)
    return SimpleNamespace(
        handle=handle,
        processor=processor,
        stop=stop,
        failed=failed,
        message=message,
        publish=publish,
        body=event.model_dump_json().encode(),
    )


@pytest.mark.parametrize("body", [b"[]", b'"text"', b"{broken", b"\xff", b'{"attempt":true}'])
async def test_poison_message_is_confirmed_in_dlq_before_ack(worker, body):
    order = []

    async def publish(*args):
        order.append("published")

    async def ack():
        order.append("acked")

    worker.publish.side_effect = publish
    worker.message.ack.side_effect = ack
    await worker.handle(body, worker.message)
    assert order == ["published", "acked"]
    payload = worker.publish.call_args.args[1]
    assert base64.b64decode(payload["original_base64"]) == body
    assert payload["attempts"] == 0
    assert worker.publish.call_args.args[2:] == ("payments.dlq", "stable-amqp-id")
    worker.processor.process.assert_not_awaited()


async def test_poison_dlq_broker_failure_keeps_original_until_confirm(worker):
    worker.publish.side_effect = [AMQPConnectionError(), None]
    await worker.handle(b"{broken", worker.message)
    assert worker.publish.await_count == 2
    worker.message.ack.assert_awaited_once()
    assert not worker.failed.is_set()


async def test_unknown_payment_is_quarantined(worker):
    worker.processor.process.side_effect = InvalidPaymentEvent("Unknown payment")
    await worker.handle(worker.body, worker.message)
    worker.publish.assert_awaited_once()
    worker.message.ack.assert_awaited_once()


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("unexpected bug"),
        RuntimeError("unexpected bug"),
        IntegrityError("INSERT", {}, ValueError("constraint")),
        ProgrammingError("SELECT", {}, ValueError("column missing")),
    ],
)
async def test_permanent_processing_failure_stops_worker_without_ack(worker, exc):
    worker.processor.process.side_effect = exc
    await worker.handle(worker.body, worker.message)
    assert worker.stop.is_set() and worker.failed.is_set()
    worker.processor.process.assert_awaited_once()
    worker.message.ack.assert_not_awaited()
    worker.publish.assert_not_awaited()


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError(),
        OperationalError("SELECT", {}, ConnectionError(), connection_invalidated=True),
    ],
)
async def test_transient_database_failure_recovers_before_ack(worker, exc):
    worker.processor.process.side_effect = [exc, None]
    await worker.handle(worker.body, worker.message)
    assert worker.processor.process.await_count == 2
    worker.message.ack.assert_awaited_once()
    assert not worker.stop.is_set()


async def test_closed_ack_channel_does_not_repeat_processing(worker):
    worker.message.ack.side_effect = ChannelInvalidStateError()
    await worker.handle(worker.body, worker.message)
    worker.processor.process.assert_awaited_once()
    worker.message.ack.assert_awaited_once()
    assert not worker.failed.is_set()


async def test_other_ack_failure_stops_worker_without_reprocessing(worker):
    worker.message.ack.side_effect = AMQPConnectionError()
    await worker.handle(worker.body, worker.message)
    worker.processor.process.assert_awaited_once()
    assert worker.stop.is_set() and worker.failed.is_set()


async def test_already_closed_delivery_is_left_for_redelivery(worker):
    raw = AsyncMock()
    type(raw).channel = PropertyMock(side_effect=ChannelInvalidStateError())
    worker.message.raw_message = raw
    await worker.handle(worker.body, worker.message)
    worker.processor.process.assert_not_awaited()
    worker.message.ack.assert_not_awaited()


async def test_cancellation_is_not_treated_as_failure_or_ack(worker):
    worker.processor.process.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await worker.handle(worker.body, worker.message)
    worker.message.ack.assert_not_awaited()
    assert not worker.failed.is_set()
