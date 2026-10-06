import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi import status as http_codes
from sqlalchemy.ext.asyncio import AsyncSession

from app import payments
from app.api import create_app
from app.config import Settings
from app.models import Payment
from tests.conftest import API_KEY, BODY, HEADERS


@pytest.fixture
async def api_client():
    session = AsyncMock(spec=AsyncSession)
    context = AsyncMock()
    context.__aenter__.return_value = session
    factory = MagicMock(return_value=context)
    app = create_app(Settings(api_key=API_KEY), factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    ) as client:
        yield client, session, app


@pytest.mark.parametrize(
    "raw",
    [
        '{"amount":NaN}',
        '{"amount":Infinity}',
        '{"amount":-Infinity}',
        '{"metadata":{"value":1e1000}}',
        '{"metadata":{"nested":[NaN]}}',
    ],
)
async def test_nonfinite_validation_is_http_422(api_client, raw):
    client, session, _ = api_client
    body = json.dumps(BODY | json.loads(raw))
    response = await client.post(
        "/api/v1/payments", content=body, headers=HEADERS | {"Content-Type": "application/json"}
    )
    assert response.status_code == http_codes.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json()["detail"][0]["loc"][0] == "body"
    # Ответ должен быть корректным JSON даже при нестандартном JSON на входе.
    json.dumps(response.json(), allow_nan=False)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


async def test_normal_validation_error_shape_is_preserved(api_client):
    client, _, _ = api_client
    response = await client.post("/api/v1/payments", json=BODY | {"amount": "0"}, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_422_UNPROCESSABLE_CONTENT
    error = response.json()["detail"][0]
    assert error["type"] == "greater_than" and error["input"] == "0"
    assert error["ctx"] == {"gt": 0}


async def test_openapi_matches_auth_and_amount_contract(api_client):
    _, _, app = api_client
    schema = app.openapi()
    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    post = schema["paths"]["/api/v1/payments"]["post"]
    assert post["security"] == [{"APIKeyHeader": []}]
    assert {"202", "401", "409", "422"} <= post["responses"].keys()
    amount = schema["components"]["schemas"]["PaymentCreate"]["properties"]["amount"]
    assert {option["type"] for option in amount["anyOf"]} == {"integer", "string"}


async def test_route_maps_conflict_and_not_found(api_client, monkeypatch):
    client, session, _ = api_client
    create = AsyncMock(side_effect=payments.IdempotencyConflict)
    monkeypatch.setattr(payments, "create_payment", create)
    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_409_CONFLICT
    assert response.json() == {"detail": "Idempotency key was used with a different body"}
    assert create.call_args.args[0] is session
    session.get.return_value = None
    assert (
        await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)
    ).status_code == http_codes.HTTP_404_NOT_FOUND


async def test_post_and_get_response_contract(api_client, monkeypatch):
    from decimal import Decimal

    client, session, _ = api_client
    payment = Payment(
        id=uuid4(),
        amount=Decimal("12.34"),
        currency="RUB",
        description="Test payment",
        payment_metadata={"order": "42"},
        status="succeeded",
        webhook_url=BODY["webhook_url"],
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        processed_at=datetime(2026, 1, 2, tzinfo=UTC),
        idempotency_key="not-exposed",
        request_body=BODY,
        webhook_event_id=uuid4(),
    )
    monkeypatch.setattr(payments, "create_payment", AsyncMock(return_value=payment))
    session.get.return_value = payment
    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_202_ACCEPTED
    assert response.json() == {
        "payment_id": str(payment.id),
        "status": "succeeded",
        "created_at": "2026-01-01T00:00:00Z",
    }
    detail = (await client.get(f"/api/v1/payments/{payment.id}", headers=HEADERS)).json()
    assert detail == response.json() | {
        "amount": "12.34",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order": "42"},
        "webhook_url": BODY["webhook_url"],
        "processed_at": "2026-01-02T00:00:00Z",
    }


async def test_unexpected_service_error_is_not_a_conflict(api_client, monkeypatch):
    from sqlalchemy.exc import IntegrityError

    client, _, _ = api_client
    monkeypatch.setattr(
        payments,
        "create_payment",
        AsyncMock(side_effect=IntegrityError("INSERT", {}, ValueError("unexpected constraint"))),
    )
    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == http_codes.HTTP_500_INTERNAL_SERVER_ERROR
    assert "constraint" not in response.text
