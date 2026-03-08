from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List


def _split_csv(raw: str | None, fallback: List[str]) -> List[str]:
    if not raw:
        return fallback
    values = [item.strip() for item in raw.split(",")]
    return [item for item in values if item]


@dataclass(slots=True)
class Settings:
    app_name: str = "ALTHEA Enterprise AML API"
    app_env: str = os.getenv("ALTHEA_ENV", "development")
    backend_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    default_tenant_id: str = os.getenv("ALTHEA_DEFAULT_TENANT_ID", "default-bank")
    tenant_header: str = os.getenv("ALTHEA_TENANT_HEADER", "X-Tenant-ID")
    database_url: str = os.getenv("ALTHEA_DATABASE_URL", "")
    redis_url: str = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379/0")
    queue_mode: str = os.getenv("ALTHEA_QUEUE_MODE", "inline")
    jwt_secret: str = os.getenv("ALTHEA_JWT_SECRET", "change-me-in-production")
    jwt_algorithm: str = os.getenv("ALTHEA_JWT_ALGORITHM", "HS256")
    access_token_minutes: int = int(os.getenv("ALTHEA_ACCESS_TOKEN_MINUTES", "60"))
    refresh_token_minutes: int = int(os.getenv("ALTHEA_REFRESH_TOKEN_MINUTES", str(60 * 24 * 14)))
    model_selection_strategy: str = os.getenv("ALTHEA_MODEL_SELECTION", "approved_latest")
    legacy_sqlite_name: str = os.getenv("ALTHEA_LEGACY_SQLITE_NAME", "app.db")
    rq_queue_name: str = os.getenv("ALTHEA_RQ_QUEUE", "althea-pipeline")
    object_storage_dirname: str = os.getenv("ALTHEA_OBJECT_STORAGE_DIR", "object_storage")
    reports_dirname: str = os.getenv("ALTHEA_REPORTS_DIR", "reports")
    dead_letter_dirname: str = os.getenv("ALTHEA_DEAD_LETTER_DIR", "dead_letter")
    model_registry_dirname: str = os.getenv("ALTHEA_MODEL_REGISTRY_DIR", "models")
    allowed_origins: List[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("ALTHEA_ALLOWED_ORIGINS"),
            [
                "https://althea-uolo.vercel.app",
                "https://althea-gamma.vercel.app",
                "http://localhost:5173",
                "http://127.0.0.1:5173",
            ],
        )
    )
    oidc_issuer_url: str | None = os.getenv("ALTHEA_OIDC_ISSUER_URL")
    oidc_client_id: str | None = os.getenv("ALTHEA_OIDC_CLIENT_ID")
    saml_metadata_url: str | None = os.getenv("ALTHEA_SAML_METADATA_URL")

    @property
    def project_root(self) -> Path:
        return self.backend_root.parent

    @property
    def data_dir(self) -> Path:
        candidate = self.project_root / "data"
        return candidate if candidate.exists() else self.backend_root / "data"

    @property
    def object_storage_root(self) -> Path:
        return self.data_dir / self.object_storage_dirname

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / self.reports_dirname

    @property
    def dead_letter_dir(self) -> Path:
        return self.data_dir / self.dead_letter_dirname

    @property
    def model_registry_dir(self) -> Path:
        return self.object_storage_root / self.model_registry_dirname

    @property
    def runtime_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        default_db = self.data_dir / "althea_enterprise.db"
        return f"sqlite:///{default_db.as_posix()}"

    @property
    def legacy_sqlite_path(self) -> Path:
        return self.data_dir / self.legacy_sqlite_name


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.object_storage_root.mkdir(parents=True, exist_ok=True)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    settings.dead_letter_dir.mkdir(parents=True, exist_ok=True)
    settings.model_registry_dir.mkdir(parents=True, exist_ok=True)
    return settings
