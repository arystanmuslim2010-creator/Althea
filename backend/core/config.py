from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List

logger = logging.getLogger("althea.config")


def _existing_insecure_artifacts(project_root: Path, backend_root: Path) -> list[str]:
    candidates = [
        backend_root / "TOKEN.txt",
        backend_root / "LOGIN_INFO.json",
        project_root / "tmp_env_dev.txt",
        project_root / "uvicorn.out.log",
        project_root / "uvicorn.err.log",
    ]
    return [str(path) for path in candidates if path.exists()]


def _split_csv(raw: str | None, fallback: List[str]) -> List[str]:
    if not raw:
        return fallback
    values = [item.strip() for item in raw.split(",")]
    return [item for item in values if item]


def _resolve_secret_value(secret_ref: str | None) -> str | None:
    if not secret_ref:
        return None
    ref = secret_ref.strip()
    if not ref:
        return None
    if ref.lower().startswith("env:"):
        return os.getenv(ref.split(":", 1)[1].strip() or "", None)
    path = Path(ref)
    if path.exists() and path.is_file():
        try:
            value = path.read_text(encoding="utf-8").strip()
            return value or None
        except Exception:
            return None
    return None


@dataclass(slots=True)
class Settings:
    app_name: str = "ALTHEA Enterprise AML API"
    app_env: str = os.getenv("ALTHEA_ENV", "development")
    backend_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    default_tenant_id: str = os.getenv("ALTHEA_DEFAULT_TENANT_ID", "default-bank")
    tenant_header: str = os.getenv("ALTHEA_TENANT_HEADER", "X-Tenant-ID")
    database_url: str = os.getenv("ALTHEA_DATABASE_URL", "")
    redis_url: str = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379/0")
    queue_mode: str = os.getenv("ALTHEA_QUEUE_MODE", "rq")
    streaming_provider: str = os.getenv("ALTHEA_STREAMING_PROVIDER", "redis")
    streaming_prefix: str = os.getenv("ALTHEA_STREAMING_PREFIX", "althea.streaming")
    streaming_inline_processing: bool = os.getenv("ALTHEA_STREAMING_INLINE_PROCESSING", "false").lower() in {"1", "true", "yes"}
    feature_online_ttl_seconds: int = int(os.getenv("ALTHEA_FEATURE_ONLINE_TTL_SECONDS", str(60 * 60 * 24)))
    jwt_secret: str = os.getenv("ALTHEA_JWT_SECRET", "change-me-in-production")
    jwt_algorithm: str = os.getenv("ALTHEA_JWT_ALGORITHM", "HS256")
    access_token_minutes: int = int(os.getenv("ALTHEA_ACCESS_TOKEN_MINUTES", "60"))
    refresh_token_minutes: int = int(os.getenv("ALTHEA_REFRESH_TOKEN_MINUTES", str(60 * 24 * 14)))
    model_selection_strategy: str = os.getenv("ALTHEA_MODEL_SELECTION", "active_approved")
    rq_queue_name: str = os.getenv("ALTHEA_RQ_QUEUE", "althea-pipeline")
    rq_job_timeout_seconds: int = int(os.getenv("ALTHEA_RQ_JOB_TIMEOUT_SECONDS", "900"))
    object_storage_dirname: str = os.getenv("ALTHEA_OBJECT_STORAGE_DIR", "object_storage")
    reports_dirname: str = os.getenv("ALTHEA_REPORTS_DIR", "reports")
    dead_letter_dirname: str = os.getenv("ALTHEA_DEAD_LETTER_DIR", "dead_letter")
    model_registry_dirname: str = os.getenv("ALTHEA_MODEL_REGISTRY_DIR", "models")
    worker_concurrency: int = int(os.getenv("ALTHEA_WORKER_CONCURRENCY", "4"))
    pipeline_batch_size: int = int(os.getenv("ALTHEA_PIPELINE_BATCH_SIZE", "20000"))
    require_postgres_in_non_dev: bool = os.getenv("ALTHEA_REQUIRE_POSTGRES", "true").lower() in {"1", "true", "yes"}
    allow_sqlite_in_dev: bool = os.getenv("ALTHEA_ALLOW_SQLITE_IN_DEV", "true").lower() in {"1", "true", "yes"}
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
    oidc_client_secret: str | None = os.getenv("ALTHEA_OIDC_CLIENT_SECRET")
    saml_metadata_url: str | None = os.getenv("ALTHEA_SAML_METADATA_URL")
    azure_ad_tenant_id: str | None = os.getenv("ALTHEA_AZURE_AD_TENANT_ID")
    azure_ad_client_id: str | None = os.getenv("ALTHEA_AZURE_AD_CLIENT_ID")
    okta_domain: str | None = os.getenv("ALTHEA_OKTA_DOMAIN")
    okta_client_id: str | None = os.getenv("ALTHEA_OKTA_CLIENT_ID")
    sso_provisioning_secret: str | None = os.getenv("ALTHEA_SSO_PROVISIONING_SECRET")
    secret_key_ref: str | None = os.getenv("ALTHEA_SECRET_KEY_REF")
    otel_exporter_otlp_endpoint: str | None = os.getenv("ALTHEA_OTEL_EXPORTER_OTLP_ENDPOINT")

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
        if not self.allow_sqlite_in_dev:
            raise RuntimeError("ALTHEA_DATABASE_URL is required when sqlite fallback is disabled.")
        default_db = self.data_dir / "althea_enterprise.db"
        return f"sqlite:///{default_db.as_posix()}"

    @property
    def is_dev(self) -> bool:
        return self.app_env.lower() == "development"

    @property
    def is_non_dev(self) -> bool:
        return not self.is_dev

    def validate(self) -> None:
        queue_mode = (self.queue_mode or "").lower().strip()
        if queue_mode == "inline":
            raise RuntimeError("ALTHEA_QUEUE_MODE=inline is no longer supported. Use 'rq'.")
        if queue_mode != "rq":
            raise RuntimeError(f"Unsupported ALTHEA_QUEUE_MODE '{self.queue_mode}'. Allowed: rq.")
        strategy = (self.model_selection_strategy or "").lower().strip()
        if strategy != "active_approved":
            raise RuntimeError(
                "ALTHEA_MODEL_SELECTION must be 'active_approved' to enforce model governance."
            )

        db_url = self.runtime_database_url
        if self.is_non_dev and self.require_postgres_in_non_dev:
            if db_url.startswith("sqlite"):
                raise RuntimeError("Non-development environments must use PostgreSQL (set ALTHEA_DATABASE_URL).")
            if not (db_url.startswith("postgresql") or db_url.startswith("postgres")):
                raise RuntimeError("ALTHEA_DATABASE_URL must point to PostgreSQL in non-development environments.")

        default_secret = "change-me-in-production"
        if self.is_non_dev and self.jwt_secret.strip() == default_secret:
            raise RuntimeError("ALTHEA_JWT_SECRET must be rotated for non-development environments.")
        if self.is_non_dev and len(self.jwt_secret.strip()) < 32:
            raise RuntimeError("ALTHEA_JWT_SECRET must be at least 32 characters in non-development environments.")

        weak_secret_tokens = {"replace-with-strong-secret", "your-secret-key", "your-32-character-secret-key-here-min32chars"}
        if self.is_non_dev and any(token in (self.jwt_secret or "").lower() for token in weak_secret_tokens):
            raise RuntimeError("ALTHEA_JWT_SECRET uses an insecure placeholder value in non-development environment.")

        insecure_artifacts = _existing_insecure_artifacts(self.project_root, self.backend_root)
        if insecure_artifacts and self.is_non_dev:
            raise RuntimeError(
                "Insecure runtime artifacts detected for non-development environment: "
                + ", ".join(insecure_artifacts)
            )
        if insecure_artifacts and self.is_dev:
            logger.warning(
                "Insecure local artifacts detected. Remove them before committing. artifacts=%s",
                insecure_artifacts,
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    secret_value = _resolve_secret_value(settings.secret_key_ref)
    if secret_value:
        settings.jwt_secret = secret_value
    settings.validate()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.object_storage_root.mkdir(parents=True, exist_ok=True)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    settings.dead_letter_dir.mkdir(parents=True, exist_ok=True)
    settings.model_registry_dir.mkdir(parents=True, exist_ok=True)
    return settings
