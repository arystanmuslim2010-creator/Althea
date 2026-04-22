"""enrichment plane tables

Revision ID: 20260422_0011
Revises: 20260405_0010
Create Date: 2026-04-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260422_0011"
down_revision = "20260405_0010"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in set(inspector.get_table_names())


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in set(inspector.get_table_names()):
        return False
    return any(str(item.get("name") or "") == index_name for item in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _has_table("enrichment_account_events"):
        op.create_table(
            "enrichment_account_events",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("source_record_id", sa.String(length=256), nullable=False),
            sa.Column("entity_id", sa.String(length=128), nullable=True),
            sa.Column("account_id", sa.String(length=128), nullable=True),
            sa.Column("counterparty_id", sa.String(length=128), nullable=True),
            sa.Column("counterparty_account_id", sa.String(length=128), nullable=True),
            sa.Column("bank_id", sa.String(length=128), nullable=True),
            sa.Column("counterparty_bank_id", sa.String(length=128), nullable=True),
            sa.Column("event_type", sa.String(length=64), nullable=False),
            sa.Column("direction", sa.String(length=32), nullable=True),
            sa.Column("amount", sa.Float(), nullable=False),
            sa.Column("currency", sa.String(length=16), nullable=True),
            sa.Column("country", sa.String(length=32), nullable=True),
            sa.Column("channel", sa.String(length=64), nullable=True),
            sa.Column("payment_type", sa.String(length=64), nullable=True),
            sa.Column("is_cross_border", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("raw_ref", sa.String(length=512), nullable=True),
            sa.Column("raw_payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint("tenant_id", "source_name", "source_record_id", name="uq_enrichment_account_events_tenant_source_record"),
        )
    if not _has_index("enrichment_account_events", "ix_enrichment_account_events_tenant_entity_time"):
        op.create_index("ix_enrichment_account_events_tenant_entity_time", "enrichment_account_events", ["tenant_id", "entity_id", "event_time"], unique=False)
    if not _has_index("enrichment_account_events", "ix_enrichment_account_events_tenant_account_time"):
        op.create_index("ix_enrichment_account_events_tenant_account_time", "enrichment_account_events", ["tenant_id", "account_id", "event_time"], unique=False)
    if not _has_index("enrichment_account_events", "ix_enrichment_account_events_tenant_counterparty_time"):
        op.create_index("ix_enrichment_account_events_tenant_counterparty_time", "enrichment_account_events", ["tenant_id", "counterparty_id", "event_time"], unique=False)

    if not _has_table("enrichment_alert_outcomes"):
        op.create_table(
            "enrichment_alert_outcomes",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("alert_id", sa.String(length=128), nullable=False),
            sa.Column("case_id", sa.String(length=128), nullable=True),
            sa.Column("entity_id", sa.String(length=128), nullable=True),
            sa.Column("decision", sa.String(length=64), nullable=True),
            sa.Column("status", sa.String(length=64), nullable=True),
            sa.Column("reason_code", sa.String(length=128), nullable=True),
            sa.Column("decided_by", sa.String(length=128), nullable=True),
            sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )
    if not _has_index("enrichment_alert_outcomes", "ix_enrichment_alert_outcomes_tenant_alert_time"):
        op.create_index("ix_enrichment_alert_outcomes_tenant_alert_time", "enrichment_alert_outcomes", ["tenant_id", "alert_id", "event_time"], unique=False)
    if not _has_index("enrichment_alert_outcomes", "ix_enrichment_alert_outcomes_tenant_entity_time"):
        op.create_index("ix_enrichment_alert_outcomes_tenant_entity_time", "enrichment_alert_outcomes", ["tenant_id", "entity_id", "event_time"], unique=False)

    if not _has_table("enrichment_case_actions"):
        op.create_table(
            "enrichment_case_actions",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("case_id", sa.String(length=128), nullable=False),
            sa.Column("alert_id", sa.String(length=128), nullable=True),
            sa.Column("entity_id", sa.String(length=128), nullable=True),
            sa.Column("action", sa.String(length=128), nullable=False),
            sa.Column("actor_id", sa.String(length=128), nullable=True),
            sa.Column("actor_role", sa.String(length=64), nullable=True),
            sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("details_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )
    if not _has_index("enrichment_case_actions", "ix_enrichment_case_actions_tenant_case_time"):
        op.create_index("ix_enrichment_case_actions_tenant_case_time", "enrichment_case_actions", ["tenant_id", "case_id", "event_time"], unique=False)
    if not _has_index("enrichment_case_actions", "ix_enrichment_case_actions_tenant_alert_time"):
        op.create_index("ix_enrichment_case_actions_tenant_alert_time", "enrichment_case_actions", ["tenant_id", "alert_id", "event_time"], unique=False)

    if not _has_table("enrichment_sync_state"):
        op.create_table(
            "enrichment_sync_state",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("cursor", sa.String(length=512), nullable=True),
            sa.Column("last_event_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("records_read", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("records_written", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("records_failed", sa.Integer(), nullable=False, server_default="0"),
            sa.UniqueConstraint("tenant_id", "source_name", name="uq_enrichment_sync_state_tenant_source"),
        )

    if not _has_table("enrichment_source_health"):
        op.create_table(
            "enrichment_source_health",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("measured_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("freshness_seconds", sa.Float(), nullable=False, server_default="0"),
            sa.Column("lag_seconds", sa.Float(), nullable=False, server_default="0"),
            sa.Column("coverage_ratio", sa.Float(), nullable=False, server_default="0"),
            sa.Column("error_rate", sa.Float(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("details_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )
    if not _has_index("enrichment_source_health", "ix_enrichment_source_health_tenant_source_measured_at"):
        op.create_index("ix_enrichment_source_health_tenant_source_measured_at", "enrichment_source_health", ["tenant_id", "source_name", "measured_at"], unique=False)

    if not _has_table("master_customers"):
        op.create_table(
            "master_customers",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("customer_id", sa.String(length=128), nullable=False),
            sa.Column("external_customer_id", sa.String(length=256), nullable=True),
            sa.Column("risk_tier", sa.String(length=64), nullable=True),
            sa.Column("segment", sa.String(length=64), nullable=True),
            sa.Column("country", sa.String(length=32), nullable=True),
            sa.Column("pep_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("sanctions_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("kyc_status", sa.String(length=64), nullable=True),
            sa.Column("effective_from", sa.DateTime(timezone=True), nullable=True),
            sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint("tenant_id", "source_name", "customer_id", name="uq_master_customers_tenant_source_customer"),
        )

    if not _has_table("master_accounts"):
        op.create_table(
            "master_accounts",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("account_id", sa.String(length=128), nullable=False),
            sa.Column("external_account_id", sa.String(length=256), nullable=True),
            sa.Column("customer_id", sa.String(length=128), nullable=True),
            sa.Column("bank_id", sa.String(length=128), nullable=True),
            sa.Column("account_type", sa.String(length=64), nullable=True),
            sa.Column("country", sa.String(length=32), nullable=True),
            sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("status", sa.String(length=64), nullable=True),
            sa.Column("effective_from", sa.DateTime(timezone=True), nullable=True),
            sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint("tenant_id", "source_name", "account_id", name="uq_master_accounts_tenant_source_account"),
        )

    if not _has_table("master_counterparties"):
        op.create_table(
            "master_counterparties",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("counterparty_id", sa.String(length=128), nullable=False),
            sa.Column("external_counterparty_id", sa.String(length=256), nullable=True),
            sa.Column("bank_id", sa.String(length=128), nullable=True),
            sa.Column("country", sa.String(length=32), nullable=True),
            sa.Column("entity_type", sa.String(length=64), nullable=True),
            sa.Column("effective_from", sa.DateTime(timezone=True), nullable=True),
            sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint("tenant_id", "source_name", "counterparty_id", name="uq_master_counterparties_tenant_source_counterparty"),
        )

    if not _has_table("entity_aliases"):
        op.create_table(
            "entity_aliases",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("entity_type", sa.String(length=64), nullable=False),
            sa.Column("canonical_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("external_id", sa.String(length=256), nullable=False),
            sa.Column("alias_type", sa.String(length=64), nullable=False),
            sa.Column("confidence", sa.Float(), nullable=False, server_default="1.0"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
            sa.UniqueConstraint("tenant_id", "entity_type", "source_name", "external_id", name="uq_entity_aliases_tenant_entity_source_external"),
        )

    if not _has_table("entity_links"):
        op.create_table(
            "entity_links",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("left_entity_type", sa.String(length=64), nullable=False),
            sa.Column("left_entity_id", sa.String(length=128), nullable=False),
            sa.Column("right_entity_type", sa.String(length=64), nullable=False),
            sa.Column("right_entity_id", sa.String(length=128), nullable=False),
            sa.Column("link_type", sa.String(length=64), nullable=False),
            sa.Column("confidence", sa.Float(), nullable=False, server_default="1.0"),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )

    if not _has_table("master_data_overrides"):
        op.create_table(
            "master_data_overrides",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("override_type", sa.String(length=64), nullable=False),
            sa.Column("left_entity_type", sa.String(length=64), nullable=True),
            sa.Column("left_entity_id", sa.String(length=128), nullable=True),
            sa.Column("right_entity_type", sa.String(length=64), nullable=True),
            sa.Column("right_entity_id", sa.String(length=128), nullable=True),
            sa.Column("target_entity_type", sa.String(length=64), nullable=True),
            sa.Column("target_entity_id", sa.String(length=128), nullable=True),
            sa.Column("source_name", sa.String(length=64), nullable=True),
            sa.Column("external_id", sa.String(length=256), nullable=True),
            sa.Column("created_by", sa.String(length=128), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        )

    if not _has_table("enrichment_audit_log"):
        op.create_table(
            "enrichment_audit_log",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=True),
            sa.Column("action", sa.String(length=128), nullable=False),
            sa.Column("actor_id", sa.String(length=128), nullable=True),
            sa.Column("status", sa.String(length=32), nullable=True),
            sa.Column("details_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        )

    if not _has_table("enrichment_dead_letter"):
        op.create_table(
            "enrichment_dead_letter",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("source_record_id", sa.String(length=256), nullable=True),
            sa.Column("error_code", sa.String(length=128), nullable=False),
            sa.Column("error_message", sa.Text(), nullable=False),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("replay_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=32), nullable=False),
        )

    if not _has_table("enrichment_schema_registry"):
        op.create_table(
            "enrichment_schema_registry",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("schema_version", sa.String(length=128), nullable=False),
            sa.Column("observed_fields_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("drift_status", sa.String(length=32), nullable=False),
            sa.UniqueConstraint("source_name", "schema_version", name="uq_enrichment_schema_registry_source_version"),
        )

    if not _has_table("enrichment_coverage_snapshots"):
        op.create_table(
            "enrichment_coverage_snapshots",
            sa.Column("id", sa.String(length=128), primary_key=True),
            sa.Column("tenant_id", sa.String(length=128), nullable=False),
            sa.Column("source_name", sa.String(length=64), nullable=False),
            sa.Column("alert_type", sa.String(length=64), nullable=False),
            sa.Column("coverage_ratio", sa.Float(), nullable=False, server_default="0"),
            sa.Column("matched_alerts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("total_alerts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("details_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("measured_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    for idx_name, table_name in (
        ("ix_enrichment_source_health_tenant_source_measured_at", "enrichment_source_health"),
        ("ix_enrichment_case_actions_tenant_alert_time", "enrichment_case_actions"),
        ("ix_enrichment_case_actions_tenant_case_time", "enrichment_case_actions"),
        ("ix_enrichment_alert_outcomes_tenant_entity_time", "enrichment_alert_outcomes"),
        ("ix_enrichment_alert_outcomes_tenant_alert_time", "enrichment_alert_outcomes"),
        ("ix_enrichment_account_events_tenant_counterparty_time", "enrichment_account_events"),
        ("ix_enrichment_account_events_tenant_account_time", "enrichment_account_events"),
        ("ix_enrichment_account_events_tenant_entity_time", "enrichment_account_events"),
    ):
        if _has_index(table_name, idx_name):
            op.drop_index(idx_name, table_name=table_name)

    for table_name in (
        "enrichment_coverage_snapshots",
        "enrichment_schema_registry",
        "enrichment_dead_letter",
        "enrichment_audit_log",
        "master_data_overrides",
        "entity_links",
        "entity_aliases",
        "master_counterparties",
        "master_accounts",
        "master_customers",
        "enrichment_source_health",
        "enrichment_sync_state",
        "enrichment_case_actions",
        "enrichment_alert_outcomes",
        "enrichment_account_events",
    ):
        if _has_table(table_name):
            op.drop_table(table_name)
