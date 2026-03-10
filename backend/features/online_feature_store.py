from __future__ import annotations

from typing import Any

import pandas as pd

from storage.redis_cache import RedisCache


class OnlineFeatureStore:
    def __init__(self, cache: RedisCache, ttl_seconds: int = 60 * 60 * 24) -> None:
        self._cache = cache
        self._ttl_seconds = int(ttl_seconds)

    @staticmethod
    def _key(tenant_id: str, alert_id: str, version: str) -> str:
        return f"features:online:{tenant_id}:{version}:{alert_id}"

    def put_features(self, tenant_id: str, alert_id: str, version: str, features: dict[str, Any]) -> None:
        self._cache.set_json(
            self._key(tenant_id, alert_id, version),
            dict(features or {}),
            ttl_seconds=self._ttl_seconds,
        )

    def get_features(self, tenant_id: str, alert_id: str, version: str) -> dict[str, Any]:
        return dict(self._cache.get_json(self._key(tenant_id, alert_id, version), default={}) or {})

    def get_many(self, tenant_id: str, alert_ids: list[str], version: str) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for alert_id in alert_ids:
            payload = self.get_features(tenant_id=tenant_id, alert_id=alert_id, version=version)
            if payload:
                rows.append(payload)
        return pd.DataFrame(rows)
