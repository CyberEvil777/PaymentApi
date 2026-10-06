"""Платежи и транзакционная таблица outbox."""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(2000), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=False, unique=True),
        sa.Column("request_body", sa.JSON(), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_event_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("completed_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(1000)),
        sa.CheckConstraint("amount > 0", name="positive_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="valid_currency"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="valid_status"),
        sa.CheckConstraint("completed_attempts BETWEEN 0 AND 3", name="valid_attempts"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("event_key", sa.String(100), nullable=False, unique=True),
        sa.Column("routing_key", sa.String(50), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(1000)),
    )
    op.create_index("ix_outbox_pending", "outbox", ["published_at", "available_at"])


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
