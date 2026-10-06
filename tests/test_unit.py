import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, PropertyMock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import status as http_codes
from pamqp.commands import Basic
from pydantic import ValidationError

from app.api import create_app
from app.config import Settings
from app.consumer import register_handler
from app.dependencies import get_session
from app.messaging import confirmed_publish, make_broker
from app.processing import PaymentEvent, error_summary, gateway
from app.schemas import PaymentCreate
from tests.conftest import API_KEY, BODY


@pytest.mark.parametrize(
    "amount", ["0", "-1", "0.001", "10000000000000000", "NaN", "Infinity", 1.23, True]
)
def test_invalid_amount(amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**(BODY | {"amount": amount}))


@pytest.mark.parametrize("amount", ["0.01", "9999999999999999.99", 1, "12.340"])
def test_decimal_amount(amount):
    payment = PaymentCreate(**(BODY | {"amount": amount}))
    assert isinstance(payment.amount, Decimal)


def test_canonical_body():
    a = PaymentCreate(**(BODY | {"amount": "12.3"}))
    b = PaymentCreate(**(BODY | {"amount": "12.30"}))
    assert a.canonical() == b.canonical()


@pytest.mark.parametrize(
    "patch",
    [
        {"currency": "BTC"},
        {"webhook_url": "ftp://host/a"},
        {"webhook_url": "oops"},
        {"metadata": []},
        {"extra": 1},
    ],
)
def test_body_validation(patch):
    with pytest.raises(ValidationError):
        PaymentCreate(**(BODY | patch))


@pytest.mark.parametrize("key", [None, "wrong-key"])
@pytest.mark.parametrize(
    "method,path", [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid4()}")]
)
async def test_api_key_before_database(key, method, path):
    app = create_app(Settings(api_key=API_KEY), sessions=object())

    async def unused_session():
        yield object()

    app.dependency_overrides[get_session] = unused_session
    headers = {} if key is None else {"X-API-Key": key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        response = await c.request(method, path, headers=headers, json=BODY)
    assert response.status_code == http_codes.HTTP_401_UNAUTHORIZED


async def test_missing_key_and_invalid_uuid():
    app = create_app(Settings(api_key=API_KEY), sessions=object())

    async def unused_session():
        yield object()

    app.dependency_overrides[get_session] = unused_session
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        response = await c.post("/api/v1/payments", json=BODY, headers={"X-API-Key": API_KEY})
        assert response.status_code == http_codes.HTTP_422_UNPROCESSABLE_CONTENT
        response = await c.get("/api/v1/payments/no-uuid", headers={"X-API-Key": API_KEY})
        assert response.status_code == http_codes.HTTP_422_UNPROCESSABLE_CONTENT


@pytest.mark.parametrize(
    "draw,result", [(0, "succeeded"), (0.899, "succeeded"), (0.9, "failed"), (0.999, "failed")]
)
async def test_gateway_deterministic(draw, result):
    sleep = AsyncMock()
    assert await gateway(sleep=sleep, uniform=lambda a, b: 3, draw=lambda: draw) == result
    sleep.assert_awaited_once_with(3)


@pytest.mark.parametrize("ack", [None, False, Basic.Nack()])
async def test_publish_requires_positive_confirmation(ack):
    broker = AsyncMock()
    broker.publish.return_value = ack
    with pytest.raises(RuntimeError):
        await confirmed_publish(broker, {}, "payments.new", uuid4())


async def test_publish_persistent_mandatory():
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    await confirmed_publish(broker, {}, "payments.new", "stable-id")
    kwargs = broker.publish.call_args.kwargs
    assert kwargs["persist"] is True and kwargs["mandatory"] is True
    assert kwargs["message_id"] == "stable-id"


async def test_manual_ack_after_durable_processing():
    processor = AsyncMock()
    message = AsyncMock()
    order = []

    async def process(event):
        order.append("committed")

    async def ack():
        order.append("acked")

    processor.process.side_effect = process
    message.ack.side_effect = ack
    broker = make_broker("amqp://guest:guest@localhost/")
    handle = register_handler(broker, processor, asyncio.Event())
    await handle(json.dumps({"payment_id": str(uuid4()), "attempt": 1}).encode(), message)
    assert order == ["committed", "acked"]
    message.nack.assert_not_awaited()


async def test_database_failure_is_not_acked_or_requeued():
    stop = asyncio.Event()
    processor = AsyncMock()
    message = AsyncMock()

    async def fail(event):
        stop.set()
        raise ConnectionError("database down")

    processor.process.side_effect = fail
    handle = register_handler(make_broker("amqp://guest:guest@localhost/"), processor, stop)
    await handle(json.dumps({"payment_id": str(uuid4()), "attempt": 1}).encode(), message)
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


def test_error_does_not_leak_credentials():
    assert error_summary(ConnectionError("https://user:secret@host")) == "ConnectionError"


@pytest.mark.parametrize("attempt", [0, 4, "1", True])
def test_invalid_attempt(attempt):
    with pytest.raises(ValidationError):
        PaymentEvent(payment_id=uuid4(), attempt=attempt)


async def test_faststream_dispatches_to_manual_handler():
    import errno
    import socket

    left, right = socket.socketpair()
    try:
        left.send(b"wake")
    except OSError as exc:
        if exc.errno == errno.EPERM:
            pytest.skip("Sandbox forbids socketpair send required by AnyIO thread wakeup")
        raise
    finally:
        left.close()
        right.close()

    from faststream.rabbit import TestRabbitBroker
    from faststream.rabbit.testing import PatchedMessage

    processor = AsyncMock()
    broker = make_broker("amqp://guest:guest@localhost/")
    register_handler(broker, processor, asyncio.Event())
    payment_id = uuid4()
    # У TestRabbitBroker нет реального канала; aio-pika считает стандартный мок закрытым.
    with patch.object(PatchedMessage, "channel", new_callable=PropertyMock, return_value=object()):
        async with asyncio.timeout(3), TestRabbitBroker(broker):
            await broker.publish(
                {"payment_id": str(payment_id), "attempt": 1}, queue="payments.new"
            )
    processor.process.assert_awaited_once_with(PaymentEvent(payment_id=payment_id, attempt=1))


@pytest.mark.parametrize(
    "status",
    [
        http_codes.HTTP_302_FOUND,
        http_codes.HTTP_400_BAD_REQUEST,
        http_codes.HTTP_500_INTERNAL_SERVER_ERROR,
    ],
)
def test_webhook_error_summary(status):
    request = httpx.Request("POST", "https://secret:password@example.test/hook")
    response = httpx.Response(status, request=request)
    with pytest.raises(httpx.HTTPStatusError) as caught:
        response.raise_for_status()
    assert error_summary(caught.value) == f"Webhook HTTP {status}"


def test_nonfinite_metadata_is_rejected():
    with pytest.raises(ValidationError):
        PaymentCreate(**(BODY | {"metadata": {"value": float("nan")}}))
