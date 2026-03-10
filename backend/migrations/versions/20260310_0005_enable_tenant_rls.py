"""enable tenant rls, enterprise rbac schema, and pipeline checkpoints

Revision ID: 20260310_0005
Revises: 20260309_0004
Create Date: 2026-03-10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260310_0005"
down_revision = "20260309_0004"
branch_labels = None
depends_on = None


TENANT_TABLES = [
    "alerts",
    "cases",
    "users",
    "feature_store",
    "investigation_logs",
    "model_versions",
    "pipeline_runs",
    "runtime_contexts",
    "alert_notes",
    "alerts_assignments",
    "user_sessions",
    "rbac_roles",
    "user_roles",
    "identity_provider",
    "external_identity",
    "auth_audit_logs",
    "pipeline_job_checkpoints",
    "role_permissions",
]


def _is_postgres() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in set(inspector.get_table_names())


def _has_unique_constraint(table_name: str, constraint_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in set(inspector.get_table_names()):
        return False
    constraints = inspector.get_unique_constraints(table_name)
    return any(str(item.get("name") or "") == constraint_name for item in constraints)


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
    # Tenant-scoped uniqueness for enterprise identity isolation.
    if _has_table("model_versions") and not _has_unique_constraint("model_versions", "uq_model_versions_tenant_model_version"):
        op.create_unique_constraint(
            "uq_model_versions_tenant_model_version",
            "model_versions",
            ["tenant_id", "model_version"],
        )

    if _has_table("alerts") and not _has_unique_constraint("alerts", "uq_alerts_tenant_alert_id"):
        op.create_unique_constraint(
            "uq_alerts_tenant_alert_id",
            "alerts",
            ["tenant_id", "alert_id"],
        )

    # Canonical RBAC metadata tables for enterprise role/permission management.
    _create_table_if_missing(
        "roles",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("name", sa.String(length=64), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("is_system", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "name", name="uq_roles_tenant_name"),
    )
    if _has_table("roles"):
        if not _has_index("roles", "ix_roles_tenant_id"):
            op.create_index("ix_roles_tenant_id", "roles", ["tenant_id"], unique=False)
        if not _has_index("roles", "ix_roles_name"):
            op.create_index("ix_roles_name", "roles", ["name"], unique=False)

    _create_table_if_missing(
        "permissions",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("name", sa.String(length=128), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("name", name="uq_permissions_name"),
    )

    _create_table_if_missing(
        "role_permissions",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("role_name", sa.String(length=64), nullable=False),
            sa.Column("permission_name", sa.String(length=128), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "role_name", "permission_name", name="uq_role_permissions_tenant_role_perm"),
    )
    if _has_table("role_permissions"):
        if not _has_index("role_permissions", "ix_role_permissions_tenant_id"):
            op.create_index("ix_role_permissions_tenant_id", "role_permissions", ["tenant_id"], unique=False)
        if not _has_index("role_permissions", "ix_role_permissions_role_name"):
            op.create_index("ix_role_permissions_role_name", "role_permissions", ["role_name"], unique=False)

    # Audit trail for privileged auth lifecycle changes.
    _create_table_if_missing(
        "auth_audit_logs",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("user_id", sa.String(length=128), nullable=True),
            sa.Column("actor_id", sa.String(length=128), nullable=True),
            sa.Column("action", sa.String(length=128), nullable=False),
            sa.Column("old_role", sa.String(length=64), nullable=True),
            sa.Column("new_role", sa.String(length=64), nullable=True),
            sa.Column("details_json", sa.JSON(), nullable=False),
            sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        ],
    )
    if _has_table("auth_audit_logs"):
        if not _has_index("auth_audit_logs", "ix_auth_audit_logs_tenant_id"):
            op.create_index("ix_auth_audit_logs_tenant_id", "auth_audit_logs", ["tenant_id"], unique=False)
        if not _has_index("auth_audit_logs", "ix_auth_audit_logs_timestamp"):
            op.create_index("ix_auth_audit_logs_timestamp", "auth_audit_logs", ["timestamp"], unique=False)

    # Identity provider placeholders for OIDC/SAML federation support.
    _create_table_if_missing(
        "identity_provider",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("provider_type", sa.String(length=32), nullable=False),
            sa.Column("name", sa.String(length=128), nullable=False),
            sa.Column("config_json", sa.JSON(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "name", name="uq_identity_provider_tenant_name"),
    )
    if _has_table("identity_provider"):
        if not _has_index("identity_provider", "ix_identity_provider_tenant_id"):
            op.create_index("ix_identity_provider_tenant_id", "identity_provider", ["tenant_id"], unique=False)

    _create_table_if_missing(
        "external_identity",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("user_id", sa.String(length=128), nullable=False),
            sa.Column("provider_id", sa.String(length=128), nullable=False),
            sa.Column("external_subject", sa.String(length=512), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "provider_id", "external_subject", name="uq_external_identity_subject"),
    )
    if _has_table("external_identity"):
        if not _has_index("external_identity", "ix_external_identity_tenant_user"):
            op.create_index("ix_external_identity_tenant_user", "external_identity", ["tenant_id", "user_id"], unique=False)

    # Pipeline checkpoints support idempotent resume semantics for large datasets.
    _create_table_if_missing(
        "pipeline_job_checkpoints",
        [
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("job_id", sa.String(length=128), nullable=False),
            sa.Column("run_id", sa.String(length=128), nullable=True),
            sa.Column("chunk_index", sa.Integer(), nullable=False),
            sa.Column("processed_rows", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="completed"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        ],
        sa.UniqueConstraint("tenant_id", "job_id", "chunk_index", name="uq_pipeline_checkpoint_chunk"),
    )
    if _has_table("pipeline_job_checkpoints"):
        if not _has_index("pipeline_job_checkpoints", "ix_pipeline_checkpoints_tenant_job"):
            op.create_index("ix_pipeline_checkpoints_tenant_job", "pipeline_job_checkpoints", ["tenant_id", "job_id"], unique=False)

    # PostgreSQL RLS hardening using per-request app.tenant_id context.
    if _is_postgres():
        for table_name in TENANT_TABLES:
            if not _has_table(table_name):
                continue
            op.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY")
            op.execute(f"ALTER TABLE {table_name} FORCE ROW LEVEL SECURITY")
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table_name}")
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
    if _is_postgres():
        for table_name in TENANT_TABLES:
            if not _has_table(table_name):
                continue
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation_policy ON {table_name}")
            op.execute(f"ALTER TABLE {table_name} NO FORCE ROW LEVEL SECURITY")
            op.execute(f"ALTER TABLE {table_name} DISABLE ROW LEVEL SECURITY")

    for table_name, index_name in [
        ("pipeline_job_checkpoints", "ix_pipeline_checkpoints_tenant_job"),
        ("external_identity", "ix_external_identity_tenant_user"),
        ("identity_provider", "ix_identity_provider_tenant_id"),
        ("auth_audit_logs", "ix_auth_audit_logs_timestamp"),
        ("auth_audit_logs", "ix_auth_audit_logs_tenant_id"),
        ("role_permissions", "ix_role_permissions_role_name"),
        ("role_permissions", "ix_role_permissions_tenant_id"),
        ("roles", "ix_roles_name"),
        ("roles", "ix_roles_tenant_id"),
    ]:
        if _has_table(table_name) and _has_index(table_name, index_name):
            op.drop_index(index_name, table_name=table_name)

    for table_name in ["pipeline_job_checkpoints", "external_identity", "identity_provider", "auth_audit_logs", "role_permissions", "permissions", "roles"]:
        if _has_table(table_name):
            op.drop_table(table_name)

    if _has_table("alerts") and _has_unique_constraint("alerts", "uq_alerts_tenant_alert_id"):
        op.drop_constraint("uq_alerts_tenant_alert_id", "alerts", type_="unique")

    if _has_table("model_versions") and _has_unique_constraint("model_versions", "uq_model_versions_tenant_model_version"):
        op.drop_constraint("uq_model_versions_tenant_model_version", "model_versions", type_="unique")
