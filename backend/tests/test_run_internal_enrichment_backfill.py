from __future__ import annotations

import json

from scripts import run_internal_enrichment_backfill as backfill_script


def test_internal_enrichment_backfill_script_runs_and_rebuilds_health(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        backfill_script,
        "get_settings",
        lambda: type("Settings", (), {"default_tenant_id": "tenant-a"})(),
    )

    class _SyncService:
        def backfill_internal_targets(self, tenant_id: str, targets: list[str], actor_id: str) -> dict:
            return {
                "tenant_id": tenant_id,
                "targets": targets,
                "actor_id": actor_id,
                "records_written": 4,
            }

    class _HealthService:
        def rebuild(self, tenant_id: str) -> list[dict]:
            return [{"tenant_id": tenant_id, "status": "healthy"}]

    monkeypatch.setattr(backfill_script, "get_enrichment_sync_service", lambda: _SyncService())
    monkeypatch.setattr(backfill_script, "get_enrichment_health_service", lambda: _HealthService())

    exit_code = backfill_script.main(["--targets", "case_actions", "alert_outcomes"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["tenant_id"] == "tenant-a"
    assert payload["records_written"] == 4
    assert payload["health"][0]["status"] == "healthy"
