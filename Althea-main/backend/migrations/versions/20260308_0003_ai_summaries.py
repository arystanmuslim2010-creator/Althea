"""add ai summaries table

Revision ID: 20260308_0003
Revises: 20260308_0002
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260308_0003"
down_revision = "20260308_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ai_summaries",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=128), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=True),
        sa.Column("actor", sa.String(length=128), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_ai_summaries_tenant_id", "ai_summaries", ["tenant_id"])
    op.create_index("ix_ai_summaries_entity_type", "ai_summaries", ["entity_type"])
    op.create_index("ix_ai_summaries_entity_id", "ai_summaries", ["entity_id"])
    op.create_index("ix_ai_summaries_ts", "ai_summaries", ["ts"])


def downgrade() -> None:
    op.drop_index("ix_ai_summaries_ts", table_name="ai_summaries")
    op.drop_index("ix_ai_summaries_entity_id", table_name="ai_summaries")
    op.drop_index("ix_ai_summaries_entity_type", table_name="ai_summaries")
    op.drop_index("ix_ai_summaries_tenant_id", table_name="ai_summaries")
    op.drop_table("ai_summaries")

