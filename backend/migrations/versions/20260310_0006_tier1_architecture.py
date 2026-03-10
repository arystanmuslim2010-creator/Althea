"""tier-1 enterprise aml architecture: streaming feature/governance/workflow schemas

Revision ID: 20260310_0006
Revises: 20260310_0005
Create Date: 2026-03-10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260310_0006"
down_revision = "20260310_0005"
branch_labels = None
depends_on = None


NEW_TABLES = [
    "features",
    "feature_versions",
    "feature_dependencies",
    "offline_feature_store",
    "model_governance_lifecycle",
    "model_governance_approvals",
    "model_governance_monitoring",
    "workflow_state_transitions",
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
    indexes = inspector.get_indexes(table_name)
    return any(str(item.get("name") or "") == index_name for item in indexes)


def _create_table_if_missing(table_name: str, columns: list[sa.Column], *constraints: sa.Constraint) -> None:
    if _has_table(table_name):
        return
    op.create_table(table_name, *columns, *constraints)


def upgrade() -> None:
    _create_table_if_missing(
        "features",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("feature_name", sa.String(length=128), nullable=False),
            sa.Column("feature_type", sa.String(length=64), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("owner", sa.String(length=128), nullable=True),
            sa.Column("source_system", sa.String(length=128), nullable=True),
            sa.Column("tags_json", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "feature_name", name="uq_features_tenant_name"),
    )

    _create_table_if_missing(
        "feature_versions",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("feature_name", sa.String(length=128), nullable=False),
            sa.Column("version", sa.String(length=64), nullable=False),
            sa.Column("transformation_sql", sa.Text(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "feature_name", "version", name="uq_feature_versions_tenant_name_version"),
    )

    _create_table_if_missing(
        "feature_dependencies",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("feature_name", sa.String(length=128), nullable=False),
            sa.Column("depends_on", sa.String(length=128), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "feature_name", "depends_on", name="uq_feature_dependencies_tenant_feature_dep"),
    )

    _create_table_if_missing(
        "offline_feature_store",
        [
            sa.Column("id", sa.String(length=256), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("run_id", sa.String(length=128), nullable=False),
            sa.Column("alert_id", sa.String(length=128), nullable=False),
            sa.Column("feature_version", sa.String(length=64), nullable=False),
            sa.Column("features_json", sa.JSON(), nullable=False),
            sa.Column("parquet_uri", sa.String(length=1024), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
    )

    _create_table_if_missing(
        "model_governance_lifecycle",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("model_version", sa.String(length=128), nullable=False),
            sa.Column("lifecycle_state", sa.String(length=32), nullable=False),
            sa.Column("actor_role", sa.String(length=64), nullable=True),
            sa.Column("actor_id", sa.String(length=128), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
    )

    _create_table_if_missing(
        "model_governance_approvals",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("model_version", sa.String(length=128), nullable=False),
            sa.Column("stage", sa.String(length=64), nullable=False),
            sa.Column("actor_id", sa.String(length=128), nullable=False),
            sa.Column("decision", sa.String(length=32), nullable=False),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
    )

    _create_table_if_missing(
        "model_governance_monitoring",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("model_version", sa.String(length=128), nullable=False),
            sa.Column("drift_metric", sa.Float(), nullable=False, server_default="0"),
            sa.Column("score_shift_metric", sa.Float(), nullable=False, server_default="0"),
            sa.Column("feedback_outcome_rate", sa.Float(), nullable=False, server_default="0"),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
    )

    _create_table_if_missing(
        "workflow_state_transitions",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("case_id", sa.String(length=128), nullable=False),
            sa.Column("from_state", sa.String(length=32), nullable=False),
            sa.Column("to_state", sa.String(length=32), nullable=False),
            sa.Column("actor_id", sa.String(length=128), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
    )

    index_specs = [
        ("features", "ix_features_tenant_name", ["tenant_id", "feature_name"]),
        ("feature_versions", "ix_feature_versions_tenant_name", ["tenant_id", "feature_name"]),
        ("feature_dependencies", "ix_feature_dependencies_tenant_name", ["tenant_id", "feature_name"]),
        ("offline_feature_store", "ix_offline_feature_store_tenant_run", ["tenant_id", "run_id"]),
        ("model_governance_lifecycle", "ix_model_governance_lifecycle_tenant_model", ["tenant_id", "model_version"]),
        ("model_governance_approvals", "ix_model_governance_approvals_tenant_model", ["tenant_id", "model_version"]),
        ("model_governance_monitoring", "ix_model_governance_monitoring_tenant_model", ["tenant_id", "model_version"]),
        ("workflow_state_transitions", "ix_workflow_state_transitions_tenant_case", ["tenant_id", "case_id"]),
    ]
    for table_name, index_name, cols in index_specs:
        if _has_table(table_name) and not _has_index(table_name, index_name):
            op.create_index(index_name, table_name, cols, unique=False)

    if _is_postgres():
        for table_name in NEW_TABLES:
            if not _has_table(table_name):
                continue
            op.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY")
            op.execute(f"ALTER TABLE {table_name} FORCE ROW LEVEL SECURITY")
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation_policy ON {table_name}")
            op.execute(
                f"""
                CREATE POLICY tenant_isolation_policy
                ON {table_name}
                USING (tenant_id::text = current_setting('app.tenant_id', true))
                WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
                """
            )


def downgrade() -> None:
    for table_name, index_name in [
        ("workflow_state_transitions", "ix_workflow_state_transitions_tenant_case"),
        ("model_governance_monitoring", "ix_model_governance_monitoring_tenant_model"),
        ("model_governance_approvals", "ix_model_governance_approvals_tenant_model"),
        ("model_governance_lifecycle", "ix_model_governance_lifecycle_tenant_model"),
        ("offline_feature_store", "ix_offline_feature_store_tenant_run"),
        ("feature_dependencies", "ix_feature_dependencies_tenant_name"),
        ("feature_versions", "ix_feature_versions_tenant_name"),
        ("features", "ix_features_tenant_name"),
    ]:
        if _has_table(table_name) and _has_index(table_name, index_name):
            op.drop_index(index_name, table_name=table_name)

    if _is_postgres():
        for table_name in NEW_TABLES:
            if not _has_table(table_name):
                continue
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation_policy ON {table_name}")
            op.execute(f"ALTER TABLE {table_name} NO FORCE ROW LEVEL SECURITY")
            op.execute(f"ALTER TABLE {table_name} DISABLE ROW LEVEL SECURITY")

    for table_name in reversed(NEW_TABLES):
        if _has_table(table_name):
            op.drop_table(table_name)
