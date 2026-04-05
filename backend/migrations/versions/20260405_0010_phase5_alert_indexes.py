"""phase 5 alert index hardening

Revision ID: 20260405_0010
Revises: 20260405_0009
Create Date: 2026-04-05
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260405_0010"
down_revision = "20260405_0009"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in set(inspector.get_table_names())


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in set(inspector.get_table_names()):
        return False
    return any(str(col.get("name") or "") == column_name for col in inspector.get_columns(table_name))


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in set(inspector.get_table_names()):
        return False
    return any(str(item.get("name") or "") == index_name for item in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _has_table("alerts"):
        return

    if _has_column("alerts", "ingestion_run_id") and not _has_index("alerts", "ix_alerts_ingestion_run_id"):
        op.create_index("ix_alerts_ingestion_run_id", "alerts", ["ingestion_run_id"], unique=False)
    if _has_column("alerts", "source_system") and not _has_index("alerts", "ix_alerts_source_system"):
        op.create_index("ix_alerts_source_system", "alerts", ["source_system"], unique=False)
    if (
        _has_column("alerts", "tenant_id")
        and _has_column("alerts", "run_id")
        and _has_column("alerts", "created_at")
        and not _has_index("alerts", "ix_alerts_tenant_run_id_created_at")
    ):
        op.create_index(
            "ix_alerts_tenant_run_id_created_at",
            "alerts",
            ["tenant_id", "run_id", "created_at"],
            unique=False,
        )


def downgrade() -> None:
    if not _has_table("alerts"):
        return

    for idx_name in (
        "ix_alerts_tenant_run_id_created_at",
        "ix_alerts_source_system",
        "ix_alerts_ingestion_run_id",
    ):
        if _has_index("alerts", idx_name):
            op.drop_index(idx_name, table_name="alerts")
