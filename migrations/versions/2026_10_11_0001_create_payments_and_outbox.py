"""create payments and outbox

Revision ID: 0001
Revises:
Create Date: 2026-10-11

"""
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

currency = postgresql.ENUM("RUB", "USD", "EUR", name="currency")
payment_status = postgresql.ENUM("pending", "succeeded", "failed", name="payment_status")


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", currency, nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("status", payment_status, nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status", "payments", ["status"])

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_status", table_name="payments")
    op.drop_table("payments")
    payment_status.drop(op.get_bind(), checkfirst=True)
    currency.drop(op.get_bind(), checkfirst=True)
