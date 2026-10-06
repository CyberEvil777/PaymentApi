import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import status as http_codes
from pamqp.commands import Basic
from sqlalchemy import event, func, select, text

from app.messaging import OutboxPublisher
from app.models import Outbox, Payment
from app.processing import PaymentEvent, Processor
from tests.conftest import BODY, HEADERS

pytestmark = pytest.mark.integration


async def create(client, **patch):
    response = await client.post("/api/v1/payments", json=BODY | patch, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_202_ACCEPTED, response.text
    return UUID(response.json()["payment_id"])


async def test_create_get_repeat_conflict(client, sessions):
    payment_id = await create(client)
    response = await client.get(f"/api/v1/payments/{payment_id}", headers=HEADERS)
    assert response.status_code == http_codes.HTTP_200_OK
    data = response.json()
    assert data["amount"] == "12.34" and data["metadata"] == BODY["metadata"]
    assert data["status"] == "pending" and data["processed_at"] is None
    assert datetime.fromisoformat(data["created_at"]).tzinfo is not None
    assert await create(client) == payment_id
    response = await client.post("/api/v1/payments", json=BODY | {"amount": "99"}, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_409_CONFLICT
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)
    assert response.status_code == http_codes.HTTP_404_NOT_FOUND
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_same_key(client, sessions):
    ids = await asyncio.gather(*(create(client) for _ in range(16)))
    assert len(set(ids)) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_conflict(client):
    responses = await asyncio.gather(
        *(
            client.post("/api/v1/payments", json=BODY | {"amount": str(i + 1)}, headers=HEADERS)
            for i in range(10)
        )
    )
    assert (
        sorted(r.status_code for r in responses)
        == [http_codes.HTTP_202_ACCEPTED] + [http_codes.HTTP_409_CONFLICT] * 9
    )


async def test_payment_and_outbox_atomic(client, sessions):
    # Вызываем сбой при SQL INSERT в outbox, после успешной вставки платежа.
    engine = sessions.kw["bind"].sync_engine

    def fail_outbox(conn, cursor, statement, params, context, many):
        if statement.startswith("INSERT INTO outbox"):
            raise RuntimeError("Injected outbox insert failure")

    event.listen(engine, "before_cursor_execute", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="Injected"):
            await create(client)
    finally:
        event.remove(engine, "before_cursor_execute", fail_outbox)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
    await create(client)


async def test_outbox_failure_and_restart(client, sessions):
    payment_id = await create(client)
    now = datetime.now(UTC) + timedelta(seconds=1)
    broken = AsyncMock()
    broken.publish.side_effect = ConnectionError("broker down")
    assert await OutboxPublisher(sessions, broken, now=lambda: now).publish_one()
    async with sessions() as session:
        row = await session.scalar(select(Outbox))
        assert row.published_at is None and row.publish_attempts == 1
    healthy = AsyncMock()
    healthy.publish.return_value = Basic.Ack()
    restarted = OutboxPublisher(sessions, healthy, now=lambda: now + timedelta(seconds=3))
    assert await restarted.publish_one()
    assert not await restarted.publish_one()
    async with sessions() as session:
        row = await session.scalar(select(Outbox))
        assert row.payment_id == payment_id and row.published_at is not None
        assert row.publish_attempts == 2


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_result_webhook_and_duplicate(client, sessions, status):
    payment_id = await create(client)
    pay = AsyncMock(return_value=status)
    calls = []

    async def receive(request):
        calls.append(request)
        # Отдельное соединение с БД подтверждает, что результат шлюза уже зафиксирован.
        async with sessions() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == status and payment.processed_at is not None
        return httpx.Response(http_codes.HTTP_204_NO_CONTENT)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        processor = Processor(sessions, http, pay=pay)
        evt = PaymentEvent(payment_id=payment_id, attempt=1)
        await asyncio.gather(processor.process(evt), processor.process(evt))
        await Processor(sessions, http, pay=pay).process(evt)
    pay.assert_awaited_once()
    assert len(calls) == 1
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert calls[0].headers["Idempotency-Key"] == str(payment.webhook_event_id)
        assert payment.webhook_delivered_at is not None


@pytest.mark.parametrize(
    "http_status", [http_codes.HTTP_302_FOUND, http_codes.HTTP_500_INTERNAL_SERVER_ERROR]
)
async def test_webhook_retry_three_attempts_dlq(client, sessions, http_status):
    payment_id = await create(client)
    pay = AsyncMock(return_value="succeeded")
    calls = []
    now = datetime.now(UTC)

    async def receive(request):
        calls.append(request)
        return httpx.Response(http_status, headers={"Location": "http://receiver.test/other"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        for attempt in range(1, 4):
            # Новый экземпляр для каждой попытки: счётчик не хранится в памяти.
            processor = Processor(sessions, http, pay=pay, now=lambda: now)
            evt = PaymentEvent(payment_id=payment_id, attempt=attempt)
            await processor.process(evt)
            await processor.process(evt)  # Дубль не должен расходовать ещё одну попытку
            async with sessions() as session:
                payment = await session.get(Payment, payment_id)
                assert payment.status == "succeeded" and payment.completed_attempts == attempt
                key = f"{payment_id}:{'dlq' if attempt == 3 else attempt + 1}"
                outbox = await session.scalar(select(Outbox).where(Outbox.event_key == key))
                if attempt < 3:
                    assert outbox.available_at == now + timedelta(seconds=2**attempt)
                else:
                    assert outbox.routing_key == "payments.dlq"
                    assert outbox.payload["attempts"] == 3
                    assert str(http_status) in outbox.payload["error"]
    pay.assert_awaited_once()
    assert len(calls) == 3
    assert len({r.headers["Idempotency-Key"] for r in calls}) == 1


async def test_webhook_retry_recovers_without_repayment(client, sessions):
    payment_id = await create(client)
    pay = AsyncMock(return_value="failed")
    requests = []

    async def receive(request):
        requests.append(request)
        return httpx.Response(
            http_codes.HTTP_503_SERVICE_UNAVAILABLE
            if len(requests) == 1
            else http_codes.HTTP_200_OK
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        for attempt in (1, 2, 1, 2):
            await Processor(sessions, http, pay=pay).process(
                PaymentEvent(payment_id=payment_id, attempt=attempt)
            )
    pay.assert_awaited_once()
    assert len(requests) == 2
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "failed" and payment.webhook_delivered_at is not None


async def test_gateway_technical_error_retries(client, sessions):
    payment_id = await create(client)
    pay = AsyncMock(side_effect=TimeoutError())
    http = AsyncMock()
    processor = Processor(sessions, http, pay=pay)
    for attempt in (1, 2, 3):
        await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    assert pay.await_count == 3
    http.stream.assert_not_called()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "pending" and payment.completed_attempts == 3
        assert (
            await session.scalar(select(Outbox).where(Outbox.routing_key == "payments.dlq"))
        ).payload["error"] == "TimeoutError"


async def test_migrations_match_models(sessions):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from app.models import Base

    async with sessions.kw["bind"].connect() as connection:
        differences = await connection.run_sync(
            lambda conn: compare_metadata(MigrationContext.configure(conn), Base.metadata)
        )
    assert differences == []


async def test_skip_locked_publishers(client, sessions):
    await create(client)
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    results = await asyncio.gather(
        *(OutboxPublisher(sessions, broker).publish_one() for _ in range(5))
    )
    assert sum(results) == 1
    broker.publish.assert_awaited_once()


async def test_service_owns_commit_and_can_be_called_without_http(sessions):
    from app.payments import create_payment
    from app.schemas import PaymentCreate

    async with sessions() as session:
        payment = await create_payment(session, PaymentCreate(**BODY), "service-key")
        assert not session.in_transaction()
        payment_id = payment.id
    async with sessions() as observer:
        assert await observer.get(Payment, payment_id) is not None
        assert await observer.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize(
    "patch",
    [
        {"currency": "EUR"},
        {"description": "different"},
        {"metadata": {"order": True}},
        {"webhook_url": "http://receiver.test/another"},
    ],
)
async def test_idempotency_compares_all_fields(client, sessions, patch):
    await create(client)
    response = await client.post("/api/v1/payments", json=BODY | patch, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_409_CONFLICT
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_idempotency_preserves_json_types_and_normalization(client, sessions):
    payment_id = await create(client, amount="12.3", metadata={"flag": True, "order": 42})
    assert await create(client, amount="12.30", metadata={"order": 42, "flag": True}) == payment_id
    response = await client.post(
        "/api/v1/payments",
        headers=HEADERS,
        json=BODY
        | {
            "amount": "12.30",
            "metadata": {"flag": 1, "order": 42},
        },
    )
    assert response.status_code == http_codes.HTTP_409_CONFLICT
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_failed_commit_rolls_back_both_rows(client, sessions):
    from sqlalchemy.orm import Session

    def fail_commit(session):
        # Выполняем обе вставки до искусственной ошибки перед commit.
        session.flush()
        raise RuntimeError("Injected commit failure")

    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="Injected commit failure"):
            await create(client)
    finally:
        event.remove(Session, "before_commit", fail_commit)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
    await create(client)  # После rollback тот же ключ снова доступен.


async def test_waiting_creator_succeeds_after_first_creator_rolls_back(client, sessions):
    from sqlalchemy.util import await_only

    first_inserted = asyncio.Event()
    second_inserting = asyncio.Event()
    release_first = asyncio.Event()
    async_engine = sessions.kw["bind"]
    engine = async_engine.sync_engine
    first_pid: int | None = None
    second_pid: int | None = None
    observer_pid: int | None = None
    blockers: list[int] = []
    stage = "подключение наблюдателя"

    def hold_and_fail(conn, cursor, statement, params, context, many):
        nonlocal first_pid, second_pid
        if statement.startswith("INSERT INTO outbox") and not first_inserted.is_set():
            first_pid = conn.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            first_inserted.set()
            # SQLAlchemy вызывает обработчик в greenlet-контексте асинхронной операции.
            await_only(release_first.wait())
            raise RuntimeError("Rollback first creator")
        if statement.startswith("INSERT INTO payments") and first_inserted.is_set():
            second_pid = conn.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            second_inserting.set()

    event.listen(engine, "before_cursor_execute", hold_and_fail)
    tasks = []
    try:
        async with asyncio.timeout(5):
            # Удерживаем третье соединение: оно не участвует в транзакциях создателей.
            async with async_engine.connect() as observer:
                observer_pid = await observer.scalar(text("SELECT pg_backend_pid()"))
                stage = "вставка первого платежа"
                tasks.append(asyncio.create_task(create(client)))
                await first_inserted.wait()
                stage = "запуск второго INSERT"
                tasks.append(asyncio.create_task(create(client, amount="99")))
                await second_inserting.wait()
                assert None not in (first_pid, second_pid, observer_pid)
                assert len({first_pid, second_pid, observer_pid}) == 3

                stage = "блокировка второго INSERT первой транзакцией"
                while True:
                    blockers = await observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": second_pid}
                    )
                    if first_pid in blockers:
                        break
                    # Пауза лишь ограничивает частоту опроса; условие проверяет PostgreSQL.
                    await asyncio.sleep(0.01)

                stage = "rollback первой транзакции и завершение второго INSERT"
                release_first.set()
                failed, payment_id = await asyncio.gather(*tasks, return_exceptions=True)
                assert isinstance(failed, RuntimeError)
                assert str(failed) == "Rollback first creator"
                assert isinstance(payment_id, UUID)
    except TimeoutError:
        pytest.fail(
            f"Сценарий не завершился за 5 с: {stage}; "
            f"first_pid={first_pid}, second_pid={second_pid}, observer_pid={observer_pid}, "
            f"последние pg_blocking_pids(second_pid)={blockers}",
            pytrace=False,
        )
    finally:
        release_first.set()
        try:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            event.remove(engine, "before_cursor_execute", hold_and_fail)
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.amount == 99
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
        outbox = await session.scalar(select(Outbox))
        assert outbox.payment_id == payment_id
        assert outbox.payload == {"payment_id": str(payment_id), "attempt": 1}


async def test_total_webhook_timeout_retries_without_repayment(client, sessions):
    payment_id = await create(client)
    pay = AsyncMock(return_value="succeeded")

    async def stalled_headers(request):
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(stalled_headers)) as http:
        for attempt in (1, 2, 3):
            await Processor(sessions, http, pay=pay, webhook_timeout_seconds=0.01).process(
                PaymentEvent(payment_id=payment_id, attempt=attempt)
            )
    pay.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded" and payment.completed_attempts == 3
        assert payment.webhook_delivered_at is None
        dead = await session.scalar(select(Outbox).where(Outbox.routing_key == "payments.dlq"))
        assert dead.payload["error"] == "TimeoutError" and dead.payload["attempts"] == 3
