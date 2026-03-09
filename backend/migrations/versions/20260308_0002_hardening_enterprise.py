"""enterprise hardening schema

Revision ID: 20260308_0002
Revises: 20260308_0001
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260308_0002"
down_revision = "20260308_0001"
branch_labels = None
depends_on = None


def _is_postgres() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def upgrade() -> None:
    op.create_table(
        "feature_store",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("alert_id", sa.String(length=128), nullable=False),
        sa.Column("feature_schema_hash", sa.String(length=256), nullable=False),
        sa.Column("features_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_feature_store_tenant_id", "feature_store", ["tenant_id"])
    op.create_index("ix_feature_store_run_id", "feature_store", ["run_id"])
    op.create_index("ix_feature_store_alert_id", "feature_store", ["alert_id"])
    op.create_index("ix_feature_store_created_at", "feature_store", ["created_at"])

    op.create_table(
        "model_monitoring",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("psi_score", sa.Float()),
        sa.Column("drift_score", sa.Float()),
        sa.Column("degradation_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("metrics_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_model_monitoring_tenant_id", "model_monitoring", ["tenant_id"])
    op.create_index("ix_model_monitoring_run_id", "model_monitoring", ["run_id"])
    op.create_index("ix_model_monitoring_model_version", "model_monitoring", ["model_version"])
    op.create_index("ix_model_monitoring_created_at", "model_monitoring", ["created_at"])

    op.add_column("model_versions", sa.Column("training_metadata_json", sa.JSON(), nullable=True))
    op.add_column("model_versions", sa.Column("approved_by", sa.String(length=128), nullable=True))
    op.add_column("model_versions", sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True))

    op.create_index("ix_alerts_tenant_run_created", "alerts", ["tenant_id", "run_id", "created_at"])
    op.create_index("ix_pipeline_runs_tenant_status_created", "pipeline_runs", ["tenant_id", "status", "created_at"])

    if _is_postgres():
        op.execute(
            """
            DO $$
            BEGIN
                IF to_regclass('public.alerts_default') IS NULL THEN
                    -- Partition-ready fallback: keep existing table unchanged, create default partition table for new installs.
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'alerts') THEN
                        -- Create an enterprise partitioned sibling table used by future migrations without breaking existing API contracts.
                        CREATE TABLE IF NOT EXISTS alerts_partitioned (
                            id VARCHAR(128) NOT NULL,
                            tenant_id VARCHAR(128) NOT NULL,
                            run_id VARCHAR(128),
                            payload_json JSONB NOT NULL,
                            created_at TIMESTAMPTZ,
                            PRIMARY KEY (id, created_at)
                        ) PARTITION BY RANGE (created_at);
                        CREATE TABLE IF NOT EXISTS alerts_default PARTITION OF alerts_partitioned DEFAULT;
                        CREATE INDEX IF NOT EXISTS ix_alerts_partitioned_tenant_run_created
                            ON alerts_partitioned (tenant_id, run_id, created_at DESC);
                    END IF;
                END IF;
            END$$;
            """
        )


def downgrade() -> None:
    if _is_postgres():
        op.execute("DROP TABLE IF EXISTS alerts_default")
        op.execute("DROP TABLE IF EXISTS alerts_partitioned")

    op.drop_index("ix_pipeline_runs_tenant_status_created", table_name="pipeline_runs")
    op.drop_index("ix_alerts_tenant_run_created", table_name="alerts")

    op.drop_column("model_versions", "approved_at")
    op.drop_column("model_versions", "approved_by")
    op.drop_column("model_versions", "training_metadata_json")

    op.drop_index("ix_model_monitoring_created_at", table_name="model_monitoring")
    op.drop_index("ix_model_monitoring_model_version", table_name="model_monitoring")
    op.drop_index("ix_model_monitoring_run_id", table_name="model_monitoring")
    op.drop_index("ix_model_monitoring_tenant_id", table_name="model_monitoring")
    op.drop_table("model_monitoring")

    op.drop_index("ix_feature_store_created_at", table_name="feature_store")
    op.drop_index("ix_feature_store_alert_id", table_name="feature_store")
    op.drop_index("ix_feature_store_run_id", table_name="feature_store")
    op.drop_index("ix_feature_store_tenant_id", table_name="feature_store")
    op.drop_table("feature_store")
