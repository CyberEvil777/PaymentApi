import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Outbox, Payment

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(UTC)


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: Annotated[int, Field(strict=True, ge=1, le=3)]


async def gateway(sleep=asyncio.sleep, uniform=random.uniform, draw=random.random) -> str:
    await sleep(uniform(2, 5))
    return "succeeded" if draw() < 0.9 else "failed"


def error_summary(exc: Exception) -> str:
    # Не сохраняем URL, учётные данные, тела ответов и произвольные тексты исключений.
    if isinstance(exc, httpx.HTTPStatusError):
        return f"Webhook HTTP {exc.response.status_code}"
    return type(exc).__name__


class InvalidPaymentEvent(ValueError):
    """Событие ссылается на неизвестный платёж или недопустимую попытку."""


class Processor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        http: httpx.AsyncClient,
        pay: Callable[[], Awaitable[str]] = gateway,
        now: Callable[[], datetime] = utcnow,
        webhook_timeout_seconds: float = 10,
    ) -> None:
        self.session_factory = session_factory
        self.http = http
        self.pay = pay
        self.now = now
        self.webhook_timeout_seconds = webhook_timeout_seconds

    async def locked_payment(self, session: AsyncSession, event: PaymentEvent) -> Payment | None:
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise InvalidPaymentEvent("Unknown payment")
        if payment.webhook_delivered_at or event.attempt <= payment.completed_attempts:
            return None
        if event.attempt != payment.completed_attempts + 1:
            raise InvalidPaymentEvent("Out-of-order attempt")
        return payment

    def record_failed_attempt(
        self, session: AsyncSession, payment: Payment, event: PaymentEvent, exc: Exception
    ) -> None:
        payment.completed_attempts = event.attempt
        payment.last_error = error_summary(exc)
        terminal = event.attempt == 3
        payload = {"payment_id": str(payment.id), "attempt": event.attempt + 1}
        if terminal:
            payload = {
                "payment_id": str(payment.id),
                "attempts": 3,
                "error": payment.last_error,
                "status": payment.status,
                "webhook_event_id": str(payment.webhook_event_id),
                "failed_at": self.now().isoformat(),
            }
        session.add(
            Outbox(
                payment_id=payment.id,
                event_key=f"{payment.id}:{'dlq' if terminal else event.attempt + 1}",
                routing_key="payments.dlq" if terminal else "payments.new",
                payload=payload,
                available_at=self.now() + timedelta(seconds=0 if terminal else 2**event.attempt),
            )
        )
        log.warning(
            "payment_id=%s attempt=%s error=%s destination=%s",
            payment.id,
            event.attempt,
            payment.last_error,
            "dlq" if terminal else "retry",
        )

    async def process(self, event: PaymentEvent) -> None:
        log.info("payment_id=%s attempt=%s processing", event.payment_id, event.attempt)
        # Обрабатываем дубли последовательно. Фиксируем результат шлюза до HTTP-запросов.
        async with self.session_factory.begin() as session:
            payment = await self.locked_payment(session, event)
            if payment is None:
                return
            if payment.status == "pending":
                try:
                    result = await self.pay()
                    if result not in {"succeeded", "failed"}:
                        raise ValueError("Invalid gateway result")
                except Exception as exc:
                    self.record_failed_attempt(session, payment, event, exc)
                    return
                payment.status = result
                payment.processed_at = self.now()

        async with self.session_factory.begin() as session:
            payment = await self.locked_payment(session, event)
            if payment is None:
                return
            try:
                await self.deliver_webhook(payment)
            except Exception as exc:
                self.record_failed_attempt(session, payment, event, exc)
            else:
                payment.webhook_delivered_at = self.now()
                payment.completed_attempts = event.attempt
                payment.last_error = None
                log.info(
                    "payment_id=%s attempt=%s webhook_delivered status=%s",
                    payment.id,
                    event.attempt,
                    payment.status,
                )

    async def deliver_webhook(self, payment: Payment) -> None:
        # Таймаут чтения не ограничивает поток с медленной непрерывной передачей данных.
        # Нужен только статус: потоковый режим исключает буферизацию всего тела ответа.
        async with asyncio.timeout(self.webhook_timeout_seconds):
            async with self.http.stream(
                "POST",
                payment.webhook_url,
                json={
                    "event_id": str(payment.webhook_event_id),
                    "payment_id": str(payment.id),
                    "status": payment.status,
                    "processed_at": payment.processed_at.isoformat(),
                },
                headers={"Idempotency-Key": str(payment.webhook_event_id)},
                follow_redirects=False,
            ) as response:
                response.raise_for_status()
