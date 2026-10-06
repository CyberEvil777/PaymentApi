import json
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """Существующий ключ относится к другому нормализованному запросу."""


async def create_payment(
    session: AsyncSession, body: PaymentCreate, idempotency_key: str
) -> Payment:
    """Атомарно сохраняет платёж и outbox или возвращает совпадающий платёж.

    Ожидает новую сессию без активной транзакции. ON CONFLICT ждёт завершения
    конкурентного создания; следующий SELECT видит его результат при READ COMMITTED.
    """
    canonical = body.canonical()
    async with session.begin():
        payment_id = uuid4()
        inserted = await session.scalar(
            insert(Payment)
            .values(
                id=payment_id,
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                payment_metadata=body.metadata,
                webhook_url=str(body.webhook_url),
                request_body=canonical,
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        payment = (
            await session.scalars(select(Payment).where(Payment.idempotency_key == idempotency_key))
        ).one()
        # Сравнение словарей Python ошибочно приравняло бы JSON true к 1.
        if json.dumps(payment.request_body, sort_keys=True) != json.dumps(
            canonical, sort_keys=True
        ):
            raise IdempotencyConflict
        if inserted is not None:
            session.add(
                Outbox(
                    payment_id=payment.id,
                    event_key=f"{payment.id}:1",
                    routing_key="payments.new",
                    payload={"payment_id": str(payment.id), "attempt": 1},
                )
            )
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
