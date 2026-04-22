from __future__ import annotations

import json
import time
from typing import Any

import httpx


class BaseEnrichmentConnector:
    source_name = "base"
    env_prefix = "ALTHEA_BASE"

    def __init__(
        self,
        *,
        base_url: str | None,
        token: str | None,
        timeout_seconds: int = 10,
        retry_max: int = 2,
        cooldown_seconds: int = 30,
    ) -> None:
        self._base_url = str(base_url or "").strip().rstrip("/")
        self._token = str(token or "").strip() or None
        self._timeout_seconds = max(1, int(timeout_seconds or 10))
        self._retry_max = max(0, int(retry_max or 0))
        self._cooldown_seconds = max(0, int(cooldown_seconds or 0))
        self._cooldown_until = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _request_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        now = time.time()
        if self._cooldown_until > now:
            raise RuntimeError(f"{self.source_name} connector is in cooldown")
        last_exc: Exception | None = None
        for attempt in range(self._retry_max + 1):
            try:
                with httpx.Client(timeout=self._timeout_seconds, headers=self._headers()) as client:
                    response = client.get(f"{self._base_url}{path}", params=params)
                    response.raise_for_status()
                    data = response.json()
                    if not isinstance(data, dict):
                        raise RuntimeError(f"{self.source_name} connector returned non-object JSON")
                    return data
            except Exception as exc:  # pragma: no cover - exercised via mock transport in tests
                last_exc = exc
                if attempt >= self._retry_max:
                    if self._cooldown_seconds:
                        self._cooldown_until = time.time() + self._cooldown_seconds
                    raise
        raise RuntimeError(str(last_exc or "connector request failed"))

    @staticmethod
    def sanitize_raw_payload(payload: dict[str, Any]) -> dict[str, Any]:
        blocked = {"authorization", "token", "password", "secret", "api_key", "apikey"}
        out: dict[str, Any] = {}
        for key, value in payload.items():
            clean_key = str(key)
            if any(token in clean_key.lower() for token in blocked):
                continue
            if isinstance(value, dict):
                out[clean_key] = BaseEnrichmentConnector.sanitize_raw_payload(value)
            elif isinstance(value, list):
                out[clean_key] = [BaseEnrichmentConnector.sanitize_raw_payload(item) if isinstance(item, dict) else item for item in value[:200]]
            elif isinstance(value, str):
                out[clean_key] = value[:4096]
            else:
                out[clean_key] = value
        return out

    def fetch_records(self, cursor: str | None = None, batch_size: int = 500, full_backfill: bool = False) -> dict[str, Any]:
        if not self.enabled:
            return {
                "customers": [],
                "accounts": [],
                "counterparties": [],
                "account_events": [],
                "alert_outcomes": [],
                "case_actions": [],
                "next_cursor": cursor,
                "raw_payload": {},
            }
        params = {"cursor": cursor or "", "batch_size": max(1, int(batch_size)), "full_backfill": "true" if full_backfill else "false"}
        payload = self._request_json("/sync", params=params)
        raw_payload = self.sanitize_raw_payload(dict(payload))
        return {
            "customers": list(payload.get("customers") or []),
            "accounts": list(payload.get("accounts") or []),
            "counterparties": list(payload.get("counterparties") or []),
            "account_events": list(payload.get("account_events") or []),
            "alert_outcomes": list(payload.get("alert_outcomes") or []),
            "case_actions": list(payload.get("case_actions") or []),
            "next_cursor": payload.get("next_cursor"),
            "raw_payload": raw_payload,
            "schema_preview": {
                "keys": sorted(payload.keys()),
                "json": json.dumps(raw_payload, sort_keys=True, ensure_ascii=True)[:2048],
            },
        }
