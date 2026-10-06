import asyncio
import base64
import logging
import signal
from pathlib import Path

import httpx
from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError, DeliveryError
from faststream import AckPolicy
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app.config import Settings
from app.db import database
from app.messaging import NEW, OutboxPublisher, confirmed_publish, declare_queues, make_broker
from app.processing import InvalidPaymentEvent, PaymentEvent, Processor

log = logging.getLogger(__name__)


async def decode_raw_body(message: StreamMessage) -> bytes:
    """Откладывает разбор и валидацию JSON до обработчика с ручным подтверждением."""
    return message.body


def transient_database_error(exc: DBAPIError) -> bool:
    sqlstate = getattr(exc.orig, "sqlstate", "") or ""
    return (
        exc.connection_invalidated
        or sqlstate.startswith("08")
        or sqlstate
        in {
            "40001",
            "40P01",
            "53300",
            "57P01",
            "57P02",
            "57P03",
        }
    )


async def pause_until_retry(stop: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=5)
    except TimeoutError:
        pass


def register_handler(
    broker: RabbitBroker,
    processor: Processor,
    stop: asyncio.Event,
    *,
    failed: asyncio.Event | None = None,
):
    def fail_worker(exc: Exception, event: PaymentEvent | None) -> None:
        # Не пишем в лог данные сообщения и тексты исключений: они могут содержать секреты.
        log.error(
            "payment_id=%s attempt=%s fatal_worker_error=%s; delivery unacked",
            event.payment_id if event else None,
            event.attempt if event else None,
            type(exc).__name__,
        )
        if failed is not None:
            failed.set()
        stop.set()

    @broker.subscriber(NEW, ack_policy=AckPolicy.MANUAL, no_reply=True, decoder=decode_raw_body)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        event = None
        try:
            event = PaymentEvent.model_validate_json(body)
        except ValidationError:
            # Base64 сохраняет даже некорректные UTF-8 и JSON, не добавляя NaN
            # или двоичные значения в JSON-документ для DLQ.
            pass

        while not stop.is_set():
            try:
                # Сообщение привязано к исходному каналу, а не созданному при переподключении.
                # RabbitMQ повторно доставит его по новому каналу.
                _ = message.raw_message.channel
            except ChannelInvalidStateError:
                return

            try:
                try:
                    if event is None:
                        raise InvalidPaymentEvent("Invalid event envelope")
                    await processor.process(event)
                except InvalidPaymentEvent:
                    await confirmed_publish(
                        broker,
                        {
                            "original_base64": base64.b64encode(body).decode("ascii"),
                            "error": "InvalidPaymentEvent",
                            "attempts": 0,
                        },
                        "payments.dlq",
                        message.message_id,
                    )
            except DBAPIError as exc:
                if not transient_database_error(exc):
                    fail_worker(exc, event)
                    return
                log.warning(
                    "payment_id=%s attempt=%s database_unavailable",
                    event.payment_id if event else None,
                    event.attempt if event else None,
                )
                await pause_until_retry(stop)
                continue
            except (
                ConnectionError,
                OSError,
                TimeoutError,
                PoolTimeoutError,
                AMQPConnectionError,
                ChannelInvalidStateError,
                DeliveryError,
            ) as exc:
                log.warning(
                    "payment_id=%s attempt=%s transport_error=%s; delivery unacked",
                    event.payment_id if event else None,
                    event.attempt if event else None,
                    type(exc).__name__,
                )
                await pause_until_retry(stop)
                continue
            except Exception as exc:
                # При постоянной ошибке останавливаем worker вместо бесконечных повторов.
                # Закрытие соединения с брокером сохраняет возможность повторной доставки.
                fail_worker(exc, event)
                return

            # Транзакция или публикация в DLQ уже надёжно зафиксирована.
            # Ошибка подтверждения не должна запускать обработку заново.
            try:
                await message.ack()
            except ChannelInvalidStateError:
                log.info(
                    "payment_id=%s ack_channel_closed; awaiting redelivery",
                    event.payment_id if event else None,
                )
            except Exception as exc:
                fail_worker(exc, event)
            return

    return handle


async def heartbeat(stop, broker, sessions):
    from sqlalchemy import text

    path = Path("/tmp/payment-consumer-health")
    try:
        while not stop.is_set():
            try:
                async with sessions() as session:
                    await session.execute(text("SELECT 1"))
                if await broker.ping(timeout=2):
                    await asyncio.to_thread(path.touch)
            except Exception:
                log.warning("Consumer dependency health check failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except TimeoutError:
                pass
    finally:
        await asyncio.to_thread(path.unlink, missing_ok=True)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    engine, sessions = database(settings.database_url)
    broker = make_broker(settings.rabbitmq_url)
    stop = asyncio.Event()
    failed = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    tasks = []
    try:
        async with httpx.AsyncClient(
            timeout=settings.webhook_timeout_seconds, follow_redirects=False, trust_env=False
        ) as http:
            try:
                processor = Processor(
                    sessions, http, webhook_timeout_seconds=settings.webhook_timeout_seconds
                )
                register_handler(broker, processor, stop, failed=failed)
                await broker.connect()
                await declare_queues(broker)
                await broker.start()
                tasks = [
                    asyncio.create_task(
                        OutboxPublisher(sessions, broker).run(stop, settings.outbox_poll_seconds)
                    ),
                    asyncio.create_task(heartbeat(stop, broker, sessions)),
                    asyncio.create_task(stop.wait()),
                ]
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if not stop.is_set():
                    failed.set()
                    log.error("Consumer background task stopped unexpectedly")
                for task in done:
                    task.result()
            finally:
                stop.set()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=12
                    )
                except TimeoutError:
                    log.warning("Consumer background tasks cancelled during shutdown")
                finally:
                    # Закрываем соединение с брокером и при ошибке запуска или фоновой задачи.
                    await broker.stop()
    finally:
        await engine.dispose()
    if failed.is_set():
        raise RuntimeError("Consumer stopped after an unrecoverable error")


if __name__ == "__main__":
    asyncio.run(main())
