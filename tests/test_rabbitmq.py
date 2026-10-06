import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import status as http_codes
from faststream import AckPolicy, BaseMiddleware
from faststream.rabbit import RabbitMessage
from sqlalchemy import select, update

from app.consumer import register_handler
from app.messaging import DLQ, NEW, OutboxPublisher, declare_queues, make_broker
from app.models import Outbox
from app.processing import Processor
from tests.conftest import BODY, HEADERS

pytestmark = pytest.mark.integration


@pytest.fixture
async def rabbit():
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to an isolated real RabbitMQ test vhost")
    broker = make_broker(url)
    await broker.connect()
    await declare_queues(broker)
    # Фикстура очищает эти две очереди только в тестовом vhost.
    for queue in (NEW, DLQ):
        await (await broker.declare_queue(queue)).purge()
    try:
        yield broker
    finally:
        await broker.stop()


@pytest.mark.parametrize(
    "webhook_status,gateway_status",
    [
        (http_codes.HTTP_204_NO_CONTENT, "succeeded"),
        (http_codes.HTTP_204_NO_CONTENT, "failed"),
        (http_codes.HTTP_503_SERVICE_UNAVAILABLE, "succeeded"),
    ],
)
async def test_real_end_to_end(client, sessions, rabbit, webhook_status, gateway_status):
    requests = []
    received_dlq = []
    dlq_ready = asyncio.Event()

    async def receive(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            body = json.loads(await reader.readexactly(length))
            requests.append((asyncio.get_running_loop().time(), headers, body))
            writer.write(
                f"HTTP/1.1 {webhook_status} Test\r\nContent-Length: 0\r\n"
                "Connection: close\r\n\r\n".encode()
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    stop = asyncio.Event()
    pay = AsyncMock(return_value=gateway_status)

    @rabbit.subscriber(DLQ, ack_policy=AckPolicy.MANUAL)
    async def dead_letter(body: dict, message: RabbitMessage):
        received_dlq.append(body)
        await message.ack()
        dlq_ready.set()

    publisher_task = None
    first_processed = asyncio.Event()
    replays_processed = asyncio.Event()
    completed = 0
    try:
        async with httpx.AsyncClient(timeout=2, trust_env=False) as http:
            processor = Processor(sessions, http, pay=pay)
            original_process = processor.process

            async def tracked_process(event):
                nonlocal completed
                await original_process(event)
                completed += 1
                first_processed.set()
                if completed == (2 if webhook_status == http_codes.HTTP_204_NO_CONTENT else 6):
                    replays_processed.set()

            processor.process = tracked_process
            register_handler(rabbit, processor, stop)
            await rabbit.start()
            response = await client.post(
                "/api/v1/payments",
                headers=HEADERS,
                json=BODY | {"webhook_url": f"http://127.0.0.1:{port}/"},
            )
            assert response.status_code == http_codes.HTTP_202_ACCEPTED
            payment_id = UUID(response.json()["payment_id"])
            publisher = OutboxPublisher(sessions, rabbit)
            publisher_task = asyncio.create_task(publisher.run(stop, 0.05))

            if webhook_status == http_codes.HTTP_204_NO_CONTENT:
                await asyncio.wait_for(first_processed.wait(), timeout=5)
                assert len(requests) == 1
                assert requests[0][2]["status"] == gateway_status
                assert not received_dlq
            else:
                await asyncio.wait_for(dlq_ready.wait(), timeout=15)
                assert len(requests) == 3
                assert requests[1][0] - requests[0][0] >= 1.9
                assert requests[2][0] - requests[1][0] >= 3.9
                assert received_dlq[0]["attempts"] == 3
                assert received_dlq[0]["error"] == "Webhook HTTP 503"
                assert received_dlq[0]["status"] == gateway_status
            pay.assert_awaited_once()
            assert len({item[2]["event_id"] for item in requests}) == 1
            # Повторяем исходные сообщения, как при сбое между подтверждением брокера и commit БД.
            for attempt in range(1, len(requests) + 1):
                await rabbit.publish(
                    {"payment_id": str(payment_id), "attempt": attempt},
                    routing_key=NEW.name,
                    persist=True,
                    mandatory=True,
                )
            await asyncio.wait_for(replays_processed.wait(), timeout=5)
            pay.assert_awaited_once()
            assert len(requests) == (1 if webhook_status == http_codes.HTTP_204_NO_CONTENT else 3)
            stop.set()
            await publisher_task
            await rabbit.stop()
    finally:
        stop.set()
        if publisher_task:
            await publisher_task
        await rabbit.stop()
        server.close()
        await server.wait_closed()


async def test_real_broker_recovery_and_publish_crash_window(client, sessions, rabbit):
    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_202_ACCEPTED
    now = datetime.now(UTC) + timedelta(seconds=1)
    publisher = OutboxPublisher(sessions, rabbit, now=lambda: now)
    # Без соединения с реальным брокером событие не должно отмечаться опубликованным.
    await rabbit.stop()
    assert await publisher.publish_one()
    async with sessions() as session:
        row = await session.scalar(select(Outbox))
        assert row.published_at is None and row.publish_attempts == 1
    await rabbit.connect()
    await declare_queues(rabbit)
    publisher = OutboxPublisher(sessions, rabbit, now=lambda: now + timedelta(seconds=3))
    assert await publisher.publish_one()
    queue = await rabbit.declare_queue(NEW)
    first = await queue.get(timeout=3, fail=False)
    assert first is not None
    await first.ack()
    # Воспроизводим состояние после ACK брокера, но до commit записи outbox.
    async with sessions.begin() as session:
        await session.execute(update(Outbox).values(published_at=None))
    assert await publisher.publish_one()
    second = await queue.get(timeout=3, fail=False)
    assert second is not None
    assert first.message_id == second.message_id
    assert first.body == second.body
    await second.ack()


async def test_real_manual_ack_redelivery_after_disconnect(rabbit):
    entered = asyncio.Event()
    release = asyncio.Event()
    received = asyncio.Event()
    original_id = str(uuid4())

    @rabbit.subscriber(NEW, ack_policy=AckPolicy.MANUAL)
    async def unacked(body: dict, message: RabbitMessage):
        entered.set()
        await release.wait()
        # Возвращаемся без ack: режим MANUAL не должен подтверждать сообщение автоматически.

    await rabbit.start()
    await rabbit.publish(
        {"payment_id": original_id, "attempt": 1},
        routing_key=NEW.name,
        persist=True,
        mandatory=True,
    )
    await asyncio.wait_for(entered.wait(), timeout=3)
    release.set()
    await rabbit.stop()
    restarted = make_broker(os.environ["TEST_RABBITMQ_URL"])

    @restarted.subscriber(NEW, ack_policy=AckPolicy.MANUAL)
    async def redelivered(body: dict, message: RabbitMessage):
        assert body["payment_id"] == original_id
        assert message.raw_message.redelivered
        await message.ack()
        received.set()

    try:
        await restarted.start()
        await asyncio.wait_for(received.wait(), timeout=5)
    finally:
        await restarted.stop()


@pytest.mark.parametrize("body", [b"[]", b'"text"', b"{broken", b"\xff"])
async def test_real_poison_message_does_not_block_next_delivery(rabbit, body):
    import base64

    dead = asyncio.Event()
    processed = asyncio.Event()
    dlq = []
    processor = AsyncMock()
    stop = asyncio.Event()

    async def process(event):
        processed.set()

    processor.process.side_effect = process
    register_handler(rabbit, processor, stop)

    @rabbit.subscriber(DLQ, ack_policy=AckPolicy.MANUAL)
    async def dead_letter(payload: dict, message: RabbitMessage):
        dlq.append(payload)
        await message.ack()
        dead.set()

    try:
        await rabbit.start()
        await rabbit.publish(
            body,
            routing_key=NEW.name,
            content_type="application/json",
            persist=True,
            mandatory=True,
        )
        await rabbit.publish(
            {"payment_id": str(uuid4()), "attempt": 1},
            routing_key=NEW.name,
            persist=True,
            mandatory=True,
        )
        await asyncio.wait_for(dead.wait(), timeout=5)
        await asyncio.wait_for(processed.wait(), timeout=5)
        assert base64.b64decode(dlq[0]["original_base64"]) == body
        assert dlq[0]["attempts"] == 0
        processor.process.assert_awaited_once()
        assert not stop.is_set()
    finally:
        stop.set()
        await rabbit.stop()


async def test_real_disconnect_after_commit_redelivers_without_repayment(client, sessions, rabbit):
    from app.models import Payment

    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_202_ACCEPTED
    payment_id = UUID(response.json()["payment_id"])
    committed = asyncio.Event()
    release_before_ack = asyncio.Event()
    handler_exited = asyncio.Event()
    redelivered = asyncio.Event()
    stop = asyncio.Event()
    pay = AsyncMock(return_value="succeeded")
    requests = []
    deliveries = 0
    first_handler_task = None
    received = []
    connection = await rabbit.connect()
    publisher_broker = make_broker(os.environ["TEST_RABBITMQ_URL"])
    restarted = make_broker(os.environ["TEST_RABBITMQ_URL"])

    class ObserveRedelivery(BaseMiddleware):
        async def consume_scope(self, call_next, message):
            result = await call_next(message)
            # Проверяем реальный признак redelivery и завершение ack штатным обработчиком.
            received.append(
                (message.raw_message.redelivered, message.message_id, message.raw_message.processed)
            )
            redelivered.set()
            return result

    restarted.add_middleware(ObserveRedelivery)

    async def receive(request):
        requests.append(request)
        return httpx.Response(http_codes.HTTP_204_NO_CONTENT)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        processor = Processor(sessions, http, pay=pay)
        process = processor.process

        async def hold_after_commit(event):
            nonlocal deliveries, first_handler_task
            await process(event)
            deliveries += 1
            if deliveries == 1:
                first_handler_task = asyncio.current_task()
                committed.set()
                try:
                    # Барьер открывает только тест после закрытия соединения.
                    # Отмена при разрыве распространяется наружу; ack недостижим до разрыва.
                    await release_before_ack.wait()
                finally:
                    handler_exited.set()

        processor.process = hold_after_commit
        register_handler(rabbit, processor, stop)
        register_handler(restarted, processor, stop)
        try:
            async with asyncio.timeout(5):
                await rabbit.start()
                await publisher_broker.connect()
                await declare_queues(publisher_broker)
                assert await OutboxPublisher(sessions, publisher_broker).publish_one()
                await committed.wait()
                async with sessions() as session:
                    payment = await session.get(Payment, payment_id)
                    saved = (
                        payment.status,
                        payment.processed_at,
                        payment.webhook_event_id,
                        payment.webhook_delivered_at,
                        payment.completed_attempts,
                    )
                    outbox = await session.scalar(select(Outbox))
                    message_id = str(outbox.id)
                assert saved[0] == "succeeded" and saved[1] is not None
                assert saved[3] is not None

            # Разрыв инициирует coroutine теста, а не отменяемый обработчик соединения.
            await asyncio.wait_for(connection.close(), timeout=5)
            assert connection.is_closed
            release_before_ack.set()
            await asyncio.wait_for(handler_exited.wait(), timeout=5)
            await asyncio.wait_for(rabbit.stop(), timeout=5)
            assert not stop.is_set()
            async with asyncio.timeout(5):
                await restarted.start()
                await redelivered.wait()
                pay.assert_awaited_once()
                assert received == [(True, message_id, True)]
                # Доставка webhook уже зафиксирована до разрыва: повторять её не нужно.
                assert len(requests) == 1 and deliveries == 2
                assert requests[0].headers["Idempotency-Key"] == str(saved[2])
                assert json.loads(requests[0].content) == {
                    "event_id": str(saved[2]),
                    "payment_id": str(payment_id),
                    "status": saved[0],
                    "processed_at": saved[1].isoformat(),
                }
                async with sessions() as session:
                    payment = await session.get(Payment, payment_id)
                    assert (
                        payment.status,
                        payment.processed_at,
                        payment.webhook_event_id,
                        payment.webhook_delivered_at,
                        payment.completed_attempts,
                    ) == saved
        finally:
            stop.set()
            try:
                # При ошибке теста отменяем удерживаемый обработчик, не разрешая ранний ack.
                if first_handler_task is not None:
                    if not first_handler_task.done():
                        first_handler_task.cancel()
                    await asyncio.wait_for(
                        asyncio.gather(first_handler_task, return_exceptions=True), timeout=5
                    )
            finally:
                results = await asyncio.gather(
                    *(
                        asyncio.wait_for(broker.stop(), timeout=30)
                        for broker in (restarted, publisher_broker, rabbit)
                    ),
                    return_exceptions=True,
                )
                assert all(result is None for result in results), results
