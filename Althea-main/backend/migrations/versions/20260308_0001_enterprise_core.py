"""enterprise core schema

Revision ID: 20260308_0001
Revises:
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260308_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runtime_contexts",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("user_scope", sa.String(length=128), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("active_run_id", sa.String(length=128)),
        sa.Column("run_source", sa.String(length=64)),
        sa.Column("dataset_hash", sa.String(length=128)),
        sa.Column("dataset_artifact_uri", sa.String(length=512)),
        sa.Column("raw_artifact_uri", sa.String(length=512)),
        sa.Column("row_count", sa.Integer()),
        sa.Column("active_job_id", sa.String(length=128)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_runtime_contexts_tenant_id", "runtime_contexts", ["tenant_id"])
    op.create_index("ix_runtime_contexts_user_scope", "runtime_contexts", ["user_scope"])

    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("initiated_by", sa.String(length=128)),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("dataset_hash", sa.String(length=128), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("artifact_uri", sa.String(length=512)),
        sa.Column("raw_artifact_uri", sa.String(length=512)),
        sa.Column("notes", sa.Text()),
        sa.Column("run_id", sa.String(length=128)),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_pipeline_runs_tenant_id", "pipeline_runs", ["tenant_id"])

    for table_name, primary in [
        ("alerts", "id"),
        ("cases", "case_id"),
        ("users", "id"),
        ("alerts_assignments", "id"),
        ("alert_notes", "id"),
        ("investigation_logs", "id"),
        ("model_versions", "id"),
        ("user_sessions", "session_id"),
    ]:
        pass

    op.create_table(
        "alerts",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=128)),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_alerts_tenant_id", "alerts", ["tenant_id"])

    op.create_table(
        "cases",
        sa.Column("case_id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=128)),
        sa.Column("assigned_to", sa.String(length=128)),
        sa.Column("alert_id", sa.String(length=128)),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("immutable_timeline_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_cases_tenant_id", "cases", ["tenant_id"])

    op.create_table(
        "users",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("email", sa.String(length=256), nullable=False),
        sa.Column("password_hash", sa.String(length=512), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("team", sa.String(length=128), nullable=False),
        sa.Column("idp_provider", sa.String(length=64)),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_users_tenant_id", "users", ["tenant_id"])
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.create_table(
        "alerts_assignments",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("alert_id", sa.String(length=128), nullable=False),
        sa.Column("assigned_to", sa.String(length=128), nullable=False),
        sa.Column("assigned_by", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_alerts_assignments_tenant_id", "alerts_assignments", ["tenant_id"])
    op.create_index("ix_alerts_assignments_alert_id", "alerts_assignments", ["alert_id"])

    op.create_table(
        "alert_notes",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("alert_id", sa.String(length=128), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("note_text", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_alert_notes_tenant_id", "alert_notes", ["tenant_id"])
    op.create_index("ix_alert_notes_alert_id", "alert_notes", ["alert_id"])

    op.create_table(
        "investigation_logs",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("case_id", sa.String(length=128)),
        sa.Column("alert_id", sa.String(length=128)),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("performed_by", sa.String(length=128), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_investigation_logs_tenant_id", "investigation_logs", ["tenant_id"])
    op.create_index("ix_investigation_logs_case_id", "investigation_logs", ["case_id"])
    op.create_index("ix_investigation_logs_alert_id", "investigation_logs", ["alert_id"])

    op.create_table(
        "user_sessions",
        sa.Column("session_id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=256), nullable=False),
        sa.Column("revoked", sa.Boolean(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_user_sessions_tenant_id", "user_sessions", ["tenant_id"])
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"])

    op.create_table(
        "model_versions",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("training_dataset_hash", sa.String(length=256), nullable=False),
        sa.Column("feature_schema_uri", sa.String(length=512), nullable=False),
        sa.Column("feature_schema_hash", sa.String(length=256), nullable=False),
        sa.Column("metrics_uri", sa.String(length=512), nullable=False),
        sa.Column("artifact_uri", sa.String(length=512), nullable=False),
        sa.Column("approval_status", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_model_versions_tenant_id", "model_versions", ["tenant_id"])
    op.create_index("ix_model_versions_model_version", "model_versions", ["model_version"])


def downgrade() -> None:
    for table_name in [
        "model_versions",
        "user_sessions",
        "investigation_logs",
        "alert_notes",
        "alerts_assignments",
        "users",
        "cases",
        "alerts",
        "pipeline_runs",
        "runtime_contexts",
    ]:
        op.drop_table(table_name)
