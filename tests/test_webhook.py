import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi import status as http_codes

from app.models import Payment
from app.processing import Processor


@pytest.fixture
def payment():
    return Payment(
        id=uuid4(),
        webhook_event_id=uuid4(),
        webhook_url="http://receiver.test/hook",
        status="succeeded",
        processed_at=datetime.now(UTC),
    )


async def test_total_deadline_cancels_slow_response_headers(payment):
    cancelled = asyncio.Event()

    async def slow_headers(request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_headers)) as http:
        processor = Processor(None, http, webhook_timeout_seconds=0.01)
        with pytest.raises(TimeoutError):
            await processor.deliver_webhook(payment)
    assert cancelled.is_set()


@pytest.mark.parametrize(
    "status",
    [
        http_codes.HTTP_200_OK,
        http_codes.HTTP_204_NO_CONTENT,
        http_codes.HTTP_302_FOUND,
        http_codes.HTTP_500_INTERNAL_SERVER_ERROR,
    ],
)
async def test_response_body_is_not_read_and_redirects_are_not_followed(payment, status):
    class UnboundedBody(httpx.AsyncByteStream):
        read = False
        closed = False

        async def __aiter__(self):
            self.read = True
            raise AssertionError("Webhook response body must not be loaded")
            yield b""  # Протокол асинхронного генератора

        async def aclose(self):
            self.closed = True

    body = UnboundedBody()
    requests = []

    async def receiver(request):
        requests.append(request)
        return httpx.Response(
            status, stream=body, headers={"Location": "http://receiver.test/other"}
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receiver), follow_redirects=True
    ) as http:
        processor = Processor(None, http)
        if status < http_codes.HTTP_300_MULTIPLE_CHOICES:
            await processor.deliver_webhook(payment)
        else:
            with pytest.raises(httpx.HTTPStatusError):
                await processor.deliver_webhook(payment)
    assert body.closed and not body.read
    assert len(requests) == 1
    assert requests[0].headers["Idempotency-Key"] == str(payment.webhook_event_id)
