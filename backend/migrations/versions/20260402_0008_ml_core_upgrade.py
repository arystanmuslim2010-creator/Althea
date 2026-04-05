"""ML core upgrade: training_runs, feature_snapshots, decision_audit, richer outcomes

Revision ID: 20260402_0008
Revises: 20260311_0007
Create Date: 2026-04-02
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260402_0008"
down_revision = "20260311_0007"
branch_labels = None
depends_on = None

NEW_TABLES = [
    "training_runs",
    "feature_snapshots",
    "decision_audit",
]

# Columns added to alert_outcomes in this migration
ALERT_OUTCOMES_NEW_COLUMNS = [
    ("sar_filed_flag", sa.Boolean(), False),
    ("qa_override", sa.Boolean(), False),
    ("investigation_start_time", sa.DateTime(timezone=True), True),
    ("investigation_end_time", sa.DateTime(timezone=True), True),
    ("resolution_hours", sa.Float(), True),
    ("touch_count", sa.Integer(), True),
    ("notes_count", sa.Integer(), True),
    ("final_label_status", sa.String(32), True),
    ("final_label_timestamp", sa.DateTime(timezone=True), True),
]


def _is_postgres() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _has_table(name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return name in set(inspector.get_table_names())


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table not in set(inspector.get_table_names()):
        return False
    return any(c["name"] == column for c in inspector.get_columns(table))


def _has_index(table: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table not in set(inspector.get_table_names()):
        return False
    return any(str(i.get("name") or "") == index_name for i in inspector.get_indexes(table))


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. training_runs — records every training run for audit / lineage
    # ------------------------------------------------------------------
    if not _has_table("training_runs"):
        op.create_table(
            "training_runs",
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("tenant_id", sa.String(128), nullable=False),
            sa.Column("training_run_id", sa.String(128), nullable=False),
            sa.Column("snapshot_id", sa.String(128), nullable=True),
            sa.Column("status", sa.String(32), nullable=False, server_default="running"),
            sa.Column("initiated_by", sa.String(128), nullable=True),
            sa.Column("dataset_hash", sa.String(256), nullable=True),
            sa.Column("row_count", sa.Integer(), nullable=True),
            sa.Column("feature_schema_version", sa.String(32), nullable=True),
            sa.Column("cutoff_timestamp", sa.DateTime(timezone=True), nullable=True),
            sa.Column("escalation_model_version", sa.String(128), nullable=True),
            sa.Column("time_model_version", sa.String(128), nullable=True),
            sa.Column("pr_auc", sa.Float(), nullable=True),
            sa.Column("roc_auc", sa.Float(), nullable=True),
            sa.Column("suspicious_capture_top_20pct", sa.Float(), nullable=True),
            sa.Column("ece_after_calibration", sa.Float(), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=True),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("captured_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        )

    for idx_name, cols in [
        ("ix_training_runs_tenant_id", ["tenant_id"]),
        ("ix_training_runs_tenant_run_id", ["tenant_id", "training_run_id"]),
        ("ix_training_runs_status", ["status"]),
    ]:
        if _has_table("training_runs") and not _has_index("training_runs", idx_name):
            op.create_index(idx_name, "training_runs", cols)

    if _is_postgres() and _has_table("training_runs"):
        op.execute("ALTER TABLE training_runs ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE training_runs FORCE ROW LEVEL SECURITY")
        op.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON training_runs")
        op.execute(
            """
            CREATE POLICY tenant_isolation_policy ON training_runs
            USING (tenant_id::text = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
            """
        )

    # ------------------------------------------------------------------
    # 2. feature_snapshots — frozen feature schema per training run
    # ------------------------------------------------------------------
    if not _has_table("feature_snapshots"):
        op.create_table(
            "feature_snapshots",
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("tenant_id", sa.String(128), nullable=False),
            sa.Column("snapshot_id", sa.String(128), nullable=False),
            sa.Column("training_run_id", sa.String(128), nullable=True),
            sa.Column("dataset_hash", sa.String(256), nullable=True),
            sa.Column("feature_schema_version", sa.String(32), nullable=True),
            sa.Column("feature_schema_hash", sa.String(64), nullable=True),
            sa.Column("feature_count", sa.Integer(), nullable=True),
            sa.Column("row_count", sa.Integer(), nullable=True),
            sa.Column("positive_rate", sa.Float(), nullable=True),
            sa.Column("cutoff_timestamp", sa.DateTime(timezone=True), nullable=True),
            sa.Column("schema_uri", sa.String(512), nullable=True),
            sa.Column("manifest_uri", sa.String(512), nullable=True),
            sa.Column("captured_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=True),
        )

    for idx_name, cols in [
        ("ix_feature_snapshots_tenant_id", ["tenant_id"]),
        ("ix_feature_snapshots_snapshot_id", ["snapshot_id"]),
        ("ix_feature_snapshots_training_run", ["tenant_id", "training_run_id"]),
    ]:
        if _has_table("feature_snapshots") and not _has_index("feature_snapshots", idx_name):
            op.create_index(idx_name, "feature_snapshots", cols)

    if _is_postgres() and _has_table("feature_snapshots"):
        op.execute("ALTER TABLE feature_snapshots ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE feature_snapshots FORCE ROW LEVEL SECURITY")
        op.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON feature_snapshots")
        op.execute(
            """
            CREATE POLICY tenant_isolation_policy ON feature_snapshots
            USING (tenant_id::text = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
            """
        )

    # ------------------------------------------------------------------
    # 3. decision_audit — immutable record of every priority + queue decision
    # ------------------------------------------------------------------
    if not _has_table("decision_audit"):
        op.create_table(
            "decision_audit",
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("tenant_id", sa.String(128), nullable=False),
            sa.Column("alert_id", sa.String(128), nullable=False),
            sa.Column("run_id", sa.String(128), nullable=True),
            sa.Column("model_version", sa.String(128), nullable=True),
            sa.Column("priority_score", sa.Float(), nullable=True),
            sa.Column("escalation_prob", sa.Float(), nullable=True),
            sa.Column("graph_risk_score", sa.Float(), nullable=True),
            sa.Column("similar_suspicious_strength", sa.Float(), nullable=True),
            sa.Column("p50_hours", sa.Float(), nullable=True),
            sa.Column("p90_hours", sa.Float(), nullable=True),
            sa.Column("governance_status", sa.String(64), nullable=True),
            sa.Column("queue_action", sa.String(64), nullable=True),
            sa.Column("priority_bucket", sa.String(32), nullable=True),
            sa.Column("compliance_flags_json", sa.JSON(), nullable=True),
            sa.Column("signals_json", sa.JSON(), nullable=True),
            sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        )

    for idx_name, cols in [
        ("ix_decision_audit_tenant_alert", ["tenant_id", "alert_id"]),
        ("ix_decision_audit_tenant_run", ["tenant_id", "run_id"]),
        ("ix_decision_audit_queue_action", ["queue_action"]),
    ]:
        if _has_table("decision_audit") and not _has_index("decision_audit", idx_name):
            op.create_index(idx_name, "decision_audit", cols)

    if _is_postgres() and _has_table("decision_audit"):
        op.execute("ALTER TABLE decision_audit ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE decision_audit FORCE ROW LEVEL SECURITY")
        op.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON decision_audit")
        op.execute(
            """
            CREATE POLICY tenant_isolation_policy ON decision_audit
            USING (tenant_id::text = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id::text = current_setting('app.tenant_id', true))
            """
        )

    # ------------------------------------------------------------------
    # 4. Extend alert_outcomes with richer investigation tracking columns
    # ------------------------------------------------------------------
    if _has_table("alert_outcomes"):
        for col_name, col_type, nullable in ALERT_OUTCOMES_NEW_COLUMNS:
            if not _has_column("alert_outcomes", col_name):
                op.add_column(
                    "alert_outcomes",
                    sa.Column(
                        col_name,
                        col_type,
                        nullable=nullable,
                        server_default="FALSE" if isinstance(col_type, sa.Boolean) else None,
                    ),
                )

        # Add index on final_label_status for retraining scheduler queries
        if not _has_index("alert_outcomes", "ix_alert_outcomes_label_status"):
            op.create_index(
                "ix_alert_outcomes_label_status",
                "alert_outcomes",
                ["tenant_id", "final_label_status"],
            )


def downgrade() -> None:
    # Remove added columns from alert_outcomes
    if _has_table("alert_outcomes"):
        if _has_index("alert_outcomes", "ix_alert_outcomes_label_status"):
            op.drop_index("ix_alert_outcomes_label_status", table_name="alert_outcomes")
        for col_name, _, _ in reversed(ALERT_OUTCOMES_NEW_COLUMNS):
            if _has_column("alert_outcomes", col_name):
                op.drop_column("alert_outcomes", col_name)

    # Drop new tables in reverse order
    for table_name in reversed(NEW_TABLES):
        if _is_postgres() and _has_table(table_name):
            op.execute(f"DROP POLICY IF EXISTS tenant_isolation_policy ON {table_name}")
        if _has_table(table_name):
            for idx_name, cols in [
                (f"ix_{table_name}_tenant_id", ["tenant_id"]),
            ]:
                if _has_index(table_name, idx_name):
                    op.drop_index(idx_name, table_name=table_name)
            op.drop_table(table_name)
