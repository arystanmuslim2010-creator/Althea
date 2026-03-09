"""enterprise isolation, rbac, and scalable alert schema

Revision ID: 20260309_0004
Revises: 20260308_0003
Create Date: 2026-03-09
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260309_0004"
down_revision = "20260308_0003"
branch_labels = None
depends_on = None


def _is_postgres() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def upgrade() -> None:
    op.add_column("alerts", sa.Column("alert_id", sa.String(length=128), nullable=True))
    op.add_column("alerts", sa.Column("risk_score", sa.Float(), nullable=False, server_default="0"))
    op.add_column("alerts", sa.Column("risk_band", sa.String(length=32), nullable=True))
    op.add_column("alerts", sa.Column("priority", sa.String(length=32), nullable=True))
    op.add_column("alerts", sa.Column("status", sa.String(length=64), nullable=False, server_default="new"))
    op.add_column("alerts", sa.Column("raw_payload", sa.JSON(), nullable=True))
    op.add_column("alerts", sa.Column("explainability_data", sa.JSON(), nullable=True))

    op.create_index("ix_alerts_alert_id", "alerts", ["alert_id"])
    op.create_index("ix_alerts_risk_score", "alerts", ["risk_score"])
    op.create_index("ix_alerts_status", "alerts", ["status"])
    op.create_index("ix_alerts_created_at", "alerts", ["created_at"])

    if _is_postgres():
        op.execute(
            """
            UPDATE alerts
            SET
              alert_id = COALESCE(payload_json->>'alert_id', split_part(id, ':', 2), id),
              risk_score = COALESCE(NULLIF(payload_json->>'risk_score', '')::double precision, 0.0),
              risk_band = NULLIF(payload_json->>'risk_band', ''),
              priority = COALESCE(NULLIF(payload_json->>'priority', ''), NULLIF(payload_json->>'risk_band', ''), 'low'),
              status = COALESCE(NULLIF(payload_json->>'status', ''), NULLIF(payload_json->>'governance_status', ''), 'new'),
              raw_payload = COALESCE(payload_json, '{}'::jsonb),
              explainability_data = jsonb_strip_nulls(
                jsonb_build_object(
                  'risk_explain_json', payload_json->'risk_explain_json',
                  'top_feature_contributions_json', payload_json->'top_feature_contributions_json',
                  'top_features_json', payload_json->'top_features_json',
                  'ml_service_explain_json', payload_json->'ml_service_explain_json'
                )
              )
            WHERE payload_json IS NOT NULL;
            """
        )
    else:
        op.execute(
            """
            UPDATE alerts
            SET
              alert_id = COALESCE(id, ''),
              status = COALESCE(status, 'new')
            """
        )

    op.create_table(
        "rbac_roles",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("role_name", sa.String(length=64), nullable=False),
        sa.Column("permissions_json", sa.JSON(), nullable=False),
        sa.Column("is_system", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("tenant_id", "role_name", name="uq_rbac_roles_tenant_role"),
    )
    op.create_index("ix_rbac_roles_tenant_id", "rbac_roles", ["tenant_id"])
    op.create_index("ix_rbac_roles_role_name", "rbac_roles", ["role_name"])

    op.create_table(
        "user_roles",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("role_name", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("tenant_id", "user_id", "role_name", name="uq_user_roles_tenant_user_role"),
    )
    op.create_index("ix_user_roles_tenant_id", "user_roles", ["tenant_id"])
    op.create_index("ix_user_roles_user_id", "user_roles", ["user_id"])
    op.create_index("ix_user_roles_role_name", "user_roles", ["role_name"])

    op.execute("DROP INDEX IF EXISTS ix_users_email")
    if _is_postgres():
        op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_email_key")
    op.create_index("uq_users_tenant_email", "users", ["tenant_id", "email"], unique=True)

    if _is_postgres():
        for table_name in ["alerts", "cases", "feature_store", "users", "investigation_logs", "model_versions"]:
            op.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY")
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table_name}")
            op.execute(
                f"""
                CREATE POLICY tenant_isolation ON {table_name}
                USING (tenant_id::text = current_setting('app.tenant_id', true))
                WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
                """
            )


def downgrade() -> None:
    if _is_postgres():
        for table_name in ["alerts", "cases", "feature_store", "users", "investigation_logs", "model_versions"]:
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table_name}")
            op.execute(f"ALTER TABLE {table_name} DISABLE ROW LEVEL SECURITY")

    op.drop_index("uq_users_tenant_email", table_name="users")
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.drop_index("ix_user_roles_role_name", table_name="user_roles")
    op.drop_index("ix_user_roles_user_id", table_name="user_roles")
    op.drop_index("ix_user_roles_tenant_id", table_name="user_roles")
    op.drop_table("user_roles")

    op.drop_index("ix_rbac_roles_role_name", table_name="rbac_roles")
    op.drop_index("ix_rbac_roles_tenant_id", table_name="rbac_roles")
    op.drop_table("rbac_roles")

    op.drop_index("ix_alerts_created_at", table_name="alerts")
    op.drop_index("ix_alerts_status", table_name="alerts")
    op.drop_index("ix_alerts_risk_score", table_name="alerts")
    op.drop_index("ix_alerts_alert_id", table_name="alerts")

    op.drop_column("alerts", "explainability_data")
    op.drop_column("alerts", "raw_payload")
    op.drop_column("alerts", "status")
    op.drop_column("alerts", "priority")
    op.drop_column("alerts", "risk_band")
    op.drop_column("alerts", "risk_score")
    op.drop_column("alerts", "alert_id")
