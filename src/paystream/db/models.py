from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid7

from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, Index, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from paystream.db.base import Base
from paystream.domain.payment import Currency, OutboxStatus, PaymentStatus


def _pg_enum(enum: type[StrEnum], name: str) -> Enum:
    return Enum(enum, name=name, values_callable=lambda members: [m.value for m in members])


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_pg_enum(Currency, "currency"))
    description: Mapped[str | None] = mapped_column(Text)
    # `metadata` is reserved on declarative classes, so the attribute is `metadata_`.
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    status: Mapped[PaymentStatus] = mapped_column(
        _pg_enum(PaymentStatus, "payment_status"),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    failure_reason: Mapped[str | None] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str | None] = mapped_column(Text)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_pending_due", "next_attempt_at", postgresql_where=text("status = 'pending'")),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    aggregate_id: Mapped[UUID]
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    headers: Mapped[dict[str, str]] = mapped_column(JSONB, default=dict, server_default=text("'{}'::jsonb"))
    status: Mapped[OutboxStatus] = mapped_column(
        _pg_enum(OutboxStatus, "outbox_status"),
        default=OutboxStatus.PENDING,
        server_default=OutboxStatus.PENDING.value,
    )
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WebhookAttempt(Base):
    __tablename__ = "webhook_attempts"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id", ondelete="CASCADE"), index=True)
    event_id: Mapped[str] = mapped_column(String(64))
    attempt: Mapped[int]
    status_code: Mapped[int | None]
    error: Mapped[str | None] = mapped_column(Text)
    duration_ms: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
