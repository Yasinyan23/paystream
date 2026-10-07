"""initial schema: payments, outbox, webhook_attempts

Revision ID: 0001
Revises:
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

currency = postgresql.ENUM("RUB", "USD", "EUR", name="currency", create_type=False)
payment_status = postgresql.ENUM("pending", "succeeded", "failed", name="payment_status", create_type=False)
outbox_status = postgresql.ENUM("pending", "published", name="outbox_status", create_type=False)


def upgrade() -> None:
    bind = op.get_bind()
    for enum in (currency, payment_status, outbox_status):
        enum.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", currency, nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", payment_status, server_default="pending", nullable=False),
        sa.Column("failure_reason", sa.String(64), nullable=True),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_amount_positive")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "headers", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("status", outbox_status, server_default="pending", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_pending_due",
        "outbox",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )

    op.create_table(
        "webhook_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.String(64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name=op.f("fk_webhook_attempts_payment_id_payments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_webhook_attempts")),
    )
    op.create_index(op.f("ix_webhook_attempts_payment_id"), "webhook_attempts", ["payment_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_webhook_attempts_payment_id"), table_name="webhook_attempts")
    op.drop_table("webhook_attempts")
    op.drop_index("ix_outbox_pending_due", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
    bind = op.get_bind()
    for enum in (outbox_status, payment_status, currency):
        enum.drop(bind, checkfirst=True)
