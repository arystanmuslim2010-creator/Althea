from __future__ import annotations

from src.services.explain_service import explain_alert as legacy_explain_alert


class ExplainabilityService:
    def explain_alert(self, alert_id: str, run_id: str, storage) -> dict | None:
        return legacy_explain_alert(alert_id, run_id, storage)
