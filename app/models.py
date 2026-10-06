from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, Numeric, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="valid_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="valid_status"),
        CheckConstraint("completed_attempts BETWEEN 0 AND 3", name="valid_attempts"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(2000))
    payment_metadata: Mapped[dict] = mapped_column("metadata", JSON)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(200), unique=True)
    request_body: Mapped[dict] = mapped_column(JSON)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_event_id: Mapped[UUID] = mapped_column(default=uuid4, unique=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_attempts: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str | None] = mapped_column(String(1000))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_pending", "published_at", "available_at"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    # Уникальное событие для каждой попытки или DLQ предотвращает дубли повторов.
    event_key: Mapped[str] = mapped_column(String(100), unique=True)
    routing_key: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str | None] = mapped_column(String(1000))
