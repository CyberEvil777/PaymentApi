import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: Annotated[
        Decimal,
        Field(
            gt=0,
            max_digits=18,
            decimal_places=2,
            description=(
                "Decimal string or integer, 0.01..9999999999999999.99; up to 2 decimal places"
            ),
        ),
    ]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=2000)
    metadata: dict
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount", mode="before", json_schema_input_type=str | int)
    @classmethod
    def no_float(cls, value):
        if isinstance(value, (float, bool)):
            raise ValueError("Send amount as a decimal string or integer, never a JSON float")
        return value

    @field_validator("metadata")
    @classmethod
    def finite_json(cls, value):
        json.dumps(value, allow_nan=False)
        return value

    def canonical(self) -> dict:
        data = self.model_dump(mode="json")
        data["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        return data


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict
    webhook_url: str
    processed_at: datetime | None
