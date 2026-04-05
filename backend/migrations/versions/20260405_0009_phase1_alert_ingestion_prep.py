"""phase 1 alert-centric ingestion preparation

Revision ID: 20260405_0009
Revises: 20260402_0008
Create Date: 2026-04-05
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260405_0009"
down_revision = "20260402_0008"
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


NEW_ALERT_COLUMNS: list[tuple[str, sa.types.TypeEngine]] = [
    ("raw_payload_json", sa.JSON()),
    ("source_system", sa.String(length=128)),
    ("ingestion_run_id", sa.String(length=128)),
    ("schema_version", sa.String(length=64)),
    ("evaluation_label_is_sar", sa.Boolean()),
    ("ingestion_metadata_json", sa.JSON()),
]


def upgrade() -> None:
    if not _has_table("alerts"):
        return
    for column_name, column_type in NEW_ALERT_COLUMNS:
        if _has_column("alerts", column_name):
            continue
        op.add_column("alerts", sa.Column(column_name, column_type, nullable=True))


def downgrade() -> None:
    if not _has_table("alerts"):
        return
    for column_name, _ in reversed(NEW_ALERT_COLUMNS):
        if _has_column("alerts", column_name):
            op.drop_column("alerts", column_name)
