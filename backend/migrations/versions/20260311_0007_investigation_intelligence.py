"""investigation intelligence: alert_outcomes, global_pattern_signals

Revision ID: 20260311_0007
Revises: 20260310_0006
Create Date: 2026-03-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260311_0007"
down_revision = "20260310_0006"
branch_labels = None
depends_on = None

NEW_TABLES = [
    "alert_outcomes",
    "global_pattern_signals",
]


def _is_postgres() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in set(inspector.get_table_names())


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in set(inspector.get_table_names()):
        return False
    return any(str(i.get("name") or "") == index_name for i in inspector.get_indexes(table_name))


def _create_table_if_missing(table_name: str, columns: list, *constraints) -> None:
    if _has_table(table_name):
        return
    op.create_table(table_name, *columns, *constraints)


def upgrade() -> None:
    _create_table_if_missing(
        "alert_outcomes",
        [
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("tenant_id", sa.String(128), nullable=False),
            sa.Column("alert_id", sa.String(128), nullable=False),
            sa.Column("analyst_decision", sa.String(64), nullable=False),
            sa.Column("decision_reason", sa.Text(), nullable=True),
            sa.Column("analyst_id", sa.String(128), nullable=True),
            sa.Column("model_version", sa.String(128), nullable=True),
            sa.Column("risk_score_at_decision", sa.Float(), nullable=True),
            sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "alert_id", name="uq_alert_outcomes_tenant_alert"),
    )

    _create_table_if_missing(
        "global_pattern_signals",
        [
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("signal_type", sa.String(64), nullable=False),
            sa.Column("signal_hash", sa.String(256), nullable=False),
            sa.Column("tenant_count", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("alert_count", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        ],
        sa.UniqueConstraint("signal_type", "signal_hash", name="uq_global_pattern_signals_type_hash"),
    )

    index_specs = [
        ("alert_outcomes", "ix_alert_outcomes_tenant_alert", ["tenant_id", "alert_id"]),
        ("alert_outcomes", "ix_alert_outcomes_tenant_decision", ["tenant_id", "analyst_decision"]),
        ("global_pattern_signals", "ix_global_pattern_signals_type", ["signal_type"]),
        ("global_pattern_signals", "ix_global_pattern_signals_hash", ["signal_hash"]),
    ]
    for table_name, index_name, cols in index_specs:
        if _has_table(table_name) and not _has_index(table_name, index_name):
            op.create_index(index_name, table_name, cols, unique=False)

    if _is_postgres():
        # alert_outcomes is tenant-scoped; enable RLS
        if _has_table("alert_outcomes"):
            op.execute("ALTER TABLE alert_outcomes ENABLE ROW LEVEL SECURITY")
            op.execute("ALTER TABLE alert_outcomes FORCE ROW LEVEL SECURITY")
            op.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON alert_outcomes")
            op.execute(
                """
                CREATE POLICY tenant_isolation_policy ON alert_outcomes
                USING (tenant_id::text = current_setting('app.tenant_id', true))
                WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
                """
            )
        # global_pattern_signals is cross-tenant — no RLS, admin-visible only


def downgrade() -> None:
    for table_name, index_name in [
        ("global_pattern_signals", "ix_global_pattern_signals_hash"),
        ("global_pattern_signals", "ix_global_pattern_signals_type"),
        ("alert_outcomes", "ix_alert_outcomes_tenant_decision"),
        ("alert_outcomes", "ix_alert_outcomes_tenant_alert"),
    ]:
        if _has_table(table_name) and _has_index(table_name, index_name):
            op.drop_index(index_name, table_name=table_name)

    if _is_postgres():
        if _has_table("alert_outcomes"):
            op.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON alert_outcomes")
            op.execute("ALTER TABLE alert_outcomes NO FORCE ROW LEVEL SECURITY")
            op.execute("ALTER TABLE alert_outcomes DISABLE ROW LEVEL SECURITY")

    for table_name in reversed(NEW_TABLES):
        if _has_table(table_name):
            op.drop_table(table_name)
