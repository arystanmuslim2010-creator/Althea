from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List

logger = logging.getLogger("althea.config")
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_PRIMARY_INGESTION_MODES = {"legacy", "alert_jsonl"}
_COOKIE_SAMESITE_VALUES = {"lax", "strict", "none"}
_RUNTIME_MODES = {"demo", "pilot", "production"}


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


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    normalized = raw.strip().lower()
    if not normalized:
        return bool(default)
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise RuntimeError(
        f"{name} must be a boolean value (accepted: 1/0, true/false, yes/no, on/off)."
    )


def _parse_int_env(name: str, default: int, min_value: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    normalized = raw.strip()
    if not normalized:
        return int(default)
    try:
        value = int(normalized)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer value.") from exc
    if value < int(min_value):
        raise RuntimeError(f"{name} must be >= {int(min_value)}.")
    return value


def _parse_primary_ingestion_mode_env(name: str, default: str) -> str:
    raw = os.getenv(name)
    value = str(raw if raw is not None else default).strip().lower()
    if not value:
        value = str(default).strip().lower() or "alert_jsonl"
    if value not in _PRIMARY_INGESTION_MODES:
        allowed = ", ".join(sorted(_PRIMARY_INGESTION_MODES))
        raise RuntimeError(f"{name} must be one of: {allowed}.")
    return value


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


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_key = key.strip()
            if not env_key:
                continue
            if env_key in os.environ:
                continue
            normalized = value.strip().strip("'").strip('"')
            os.environ[env_key] = normalized
    except Exception:
        logger.warning("Failed to load env file", extra={"path": str(path)})


@dataclass(slots=True)
class Settings:
    app_name: str = "ALTHEA Enterprise AML API"
    app_env: str = os.getenv("ALTHEA_ENV", "development")
    runtime_mode: str = os.getenv("ALTHEA_RUNTIME_MODE", "demo")
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
    jwt_secret: str = os.getenv("ALTHEA_JWT_SECRET", "")
    jwt_algorithm: str = os.getenv("ALTHEA_JWT_ALGORITHM", "HS256")
    access_token_minutes: int = int(os.getenv("ALTHEA_ACCESS_TOKEN_MINUTES", "60"))
    refresh_token_minutes: int = int(os.getenv("ALTHEA_REFRESH_TOKEN_MINUTES", str(60 * 24 * 14)))
    allow_dev_models: bool = os.getenv("ALTHEA_ALLOW_DEV_MODELS", "").lower() in {"1", "true", "yes"}
    model_selection_strategy: str = os.getenv("ALTHEA_MODEL_SELECTION", "active_approved")
    rq_queue_name: str = os.getenv("ALTHEA_RQ_QUEUE", "althea-pipeline")
    enrichment_rq_queue_name: str = os.getenv("ALTHEA_ENRICHMENT_RQ_QUEUE", "althea-enrichment")
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
    allowed_hosts: List[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("ALTHEA_ALLOWED_HOSTS"),
            [
                "localhost",
                "127.0.0.1",
                "testserver",
            ],
        )
    )
    cors_allow_origin_regex: str | None = os.getenv("ALTHEA_CORS_ALLOW_ORIGIN_REGEX")
    cors_allow_credentials: bool = os.getenv("ALTHEA_CORS_ALLOW_CREDENTIALS", "true").lower() in {"1", "true", "yes"}
    trusted_proxy_headers: bool = os.getenv("ALTHEA_TRUST_PROXY_HEADERS", "false").lower() in {"1", "true", "yes"}
    security_headers_enabled: bool = os.getenv("ALTHEA_SECURITY_HEADERS_ENABLED", "true").lower() in {"1", "true", "yes"}
    login_rate_limit_max_attempts: int = int(os.getenv("ALTHEA_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "5"))
    login_rate_limit_window_seconds: int = int(os.getenv("ALTHEA_LOGIN_RATE_LIMIT_WINDOW_SECONDS", "60"))
    refresh_rate_limit_max_attempts: int = int(os.getenv("ALTHEA_REFRESH_RATE_LIMIT_MAX_ATTEMPTS", "30"))
    refresh_rate_limit_window_seconds: int = int(os.getenv("ALTHEA_REFRESH_RATE_LIMIT_WINDOW_SECONDS", "60"))
    enable_public_tenant_bootstrap: bool = os.getenv("ALTHEA_ENABLE_PUBLIC_TENANT_BOOTSTRAP", "").lower() in {"1", "true", "yes"}
    bootstrap_provisioning_secret: str | None = os.getenv("ALTHEA_BOOTSTRAP_PROVISIONING_SECRET")
    refresh_cookie_name: str = os.getenv("ALTHEA_REFRESH_COOKIE_NAME", "althea_rt")
    refresh_cookie_path: str = os.getenv("ALTHEA_REFRESH_COOKIE_PATH", "/api/auth")
    refresh_cookie_domain: str | None = os.getenv("ALTHEA_REFRESH_COOKIE_DOMAIN")
    refresh_cookie_secure: bool = os.getenv("ALTHEA_REFRESH_COOKIE_SECURE", "").lower() in {"1", "true", "yes"}
    refresh_cookie_samesite: str = os.getenv("ALTHEA_REFRESH_COOKIE_SAMESITE", "strict")
    allow_refresh_token_in_body: bool = os.getenv("ALTHEA_ALLOW_REFRESH_TOKEN_IN_BODY", "").lower() in {"1", "true", "yes"}
    expose_refresh_token_in_response: bool = os.getenv("ALTHEA_EXPOSE_REFRESH_TOKEN_IN_RESPONSE", "").lower() in {"1", "true", "yes"}
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
    enable_alert_jsonl_ingestion: bool = True
    # Finalization stage: legacy ingestion is hard-disabled by default.
    # This flag remains as an emergency-only override during stabilization.
    enable_legacy_ingestion: bool = False
    enable_ibm_amlsim_import: bool = False
    enable_human_interpretation: bool = True
    strict_ingestion_validation: bool = False
    alert_jsonl_max_upload_rows: int = 1000
    ingestion_max_upload_bytes: int = 10 * 1024 * 1024
    primary_ingestion_mode: str = "alert_jsonl"
    enrichment_enabled: bool = True
    enrichment_sources_enabled: List[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("ALTHEA_ENRICHMENT_SOURCES_ENABLED"),
            ["internal_case", "internal_outcome"],
        )
    )
    enrichment_sync_batch_size: int = int(os.getenv("ALTHEA_ENRICHMENT_SYNC_BATCH_SIZE", "500"))
    enrichment_health_stale_seconds: int = int(os.getenv("ALTHEA_ENRICHMENT_HEALTH_STALE_SECONDS", "3600"))
    enrichment_sync_max_retries: int = int(os.getenv("ALTHEA_ENRICHMENT_SYNC_MAX_RETRIES", "2"))
    enrichment_connector_timeout_seconds: int = int(os.getenv("ALTHEA_ENRICHMENT_CONNECTOR_TIMEOUT_SECONDS", "10"))
    enrichment_connector_retry_max: int = int(os.getenv("ALTHEA_ENRICHMENT_CONNECTOR_RETRY_MAX", "2"))
    enrichment_connector_cooldown_seconds: int = int(os.getenv("ALTHEA_ENRICHMENT_CONNECTOR_COOLDOWN_SECONDS", "30"))
    kyc_base_url: str | None = os.getenv("ALTHEA_KYC_BASE_URL")
    kyc_token: str | None = os.getenv("ALTHEA_KYC_TOKEN")
    watchlist_base_url: str | None = os.getenv("ALTHEA_WATCHLIST_BASE_URL")
    watchlist_token: str | None = os.getenv("ALTHEA_WATCHLIST_TOKEN")
    device_base_url: str | None = os.getenv("ALTHEA_DEVICE_BASE_URL")
    device_token: str | None = os.getenv("ALTHEA_DEVICE_TOKEN")
    channel_base_url: str | None = os.getenv("ALTHEA_CHANNEL_BASE_URL")
    channel_token: str | None = os.getenv("ALTHEA_CHANNEL_TOKEN")

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

    def is_demo_mode(self) -> bool:
        return str(self.runtime_mode or "").lower().strip() == "demo"

    def is_pilot_mode(self) -> bool:
        return str(self.runtime_mode or "").lower().strip() == "pilot"

    def is_production_mode(self) -> bool:
        return str(self.runtime_mode or "").lower().strip() == "production"

    def demo_features_enabled(self) -> bool:
        return self.is_demo_mode()

    def validate(self) -> None:
        self.runtime_mode = str(os.getenv("ALTHEA_RUNTIME_MODE", self.runtime_mode) or "demo").strip().lower()
        if self.runtime_mode not in _RUNTIME_MODES:
            allowed = ", ".join(sorted(_RUNTIME_MODES))
            raise RuntimeError(f"ALTHEA_RUNTIME_MODE must be one of: {allowed}.")
        self.enable_alert_jsonl_ingestion = _parse_bool_env(
            "ALTHEA_ENABLE_ALERT_JSONL_INGESTION", self.enable_alert_jsonl_ingestion
        )
        self.enable_legacy_ingestion = _parse_bool_env(
            "ALTHEA_ENABLE_LEGACY_INGESTION", self.enable_legacy_ingestion
        )
        self.enable_ibm_amlsim_import = _parse_bool_env(
            "ALTHEA_ENABLE_IBM_AMLSIM_IMPORT", self.enable_ibm_amlsim_import
        )
        self.enable_human_interpretation = _parse_bool_env(
            "ALTHEA_ENABLE_HUMAN_INTERPRETATION", self.enable_human_interpretation
        )
        self.strict_ingestion_validation = _parse_bool_env(
            "ALTHEA_STRICT_INGESTION_VALIDATION", self.strict_ingestion_validation
        )
        self.alert_jsonl_max_upload_rows = _parse_int_env(
            "ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS",
            self.alert_jsonl_max_upload_rows,
            min_value=1,
        )
        self.ingestion_max_upload_bytes = _parse_int_env(
            "ALTHEA_INGESTION_MAX_UPLOAD_BYTES",
            self.ingestion_max_upload_bytes,
            min_value=1024,
        )
        self.primary_ingestion_mode = _parse_primary_ingestion_mode_env(
            "ALTHEA_PRIMARY_INGESTION_MODE",
            self.primary_ingestion_mode,
        )
        self.enrichment_enabled = _parse_bool_env(
            "ALTHEA_ENRICHMENT_ENABLED",
            self.enrichment_enabled,
        )
        self.enrichment_sync_batch_size = _parse_int_env(
            "ALTHEA_ENRICHMENT_SYNC_BATCH_SIZE",
            self.enrichment_sync_batch_size,
            min_value=1,
        )
        self.enrichment_health_stale_seconds = _parse_int_env(
            "ALTHEA_ENRICHMENT_HEALTH_STALE_SECONDS",
            self.enrichment_health_stale_seconds,
            min_value=1,
        )
        self.enrichment_sync_max_retries = _parse_int_env(
            "ALTHEA_ENRICHMENT_SYNC_MAX_RETRIES",
            self.enrichment_sync_max_retries,
            min_value=0,
        )
        self.enrichment_connector_timeout_seconds = _parse_int_env(
            "ALTHEA_ENRICHMENT_CONNECTOR_TIMEOUT_SECONDS",
            self.enrichment_connector_timeout_seconds,
            min_value=1,
        )
        self.enrichment_connector_retry_max = _parse_int_env(
            "ALTHEA_ENRICHMENT_CONNECTOR_RETRY_MAX",
            self.enrichment_connector_retry_max,
            min_value=0,
        )
        self.enrichment_connector_cooldown_seconds = _parse_int_env(
            "ALTHEA_ENRICHMENT_CONNECTOR_COOLDOWN_SECONDS",
            self.enrichment_connector_cooldown_seconds,
            min_value=0,
        )
        self.cors_allow_credentials = _parse_bool_env("ALTHEA_CORS_ALLOW_CREDENTIALS", self.cors_allow_credentials)
        self.trusted_proxy_headers = _parse_bool_env("ALTHEA_TRUST_PROXY_HEADERS", self.trusted_proxy_headers)
        self.security_headers_enabled = _parse_bool_env("ALTHEA_SECURITY_HEADERS_ENABLED", self.security_headers_enabled)
        self.login_rate_limit_max_attempts = _parse_int_env(
            "ALTHEA_LOGIN_RATE_LIMIT_MAX_ATTEMPTS",
            self.login_rate_limit_max_attempts,
            min_value=1,
        )
        self.login_rate_limit_window_seconds = _parse_int_env(
            "ALTHEA_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
            self.login_rate_limit_window_seconds,
            min_value=1,
        )
        self.refresh_rate_limit_max_attempts = _parse_int_env(
            "ALTHEA_REFRESH_RATE_LIMIT_MAX_ATTEMPTS",
            self.refresh_rate_limit_max_attempts,
            min_value=1,
        )
        self.refresh_rate_limit_window_seconds = _parse_int_env(
            "ALTHEA_REFRESH_RATE_LIMIT_WINDOW_SECONDS",
            self.refresh_rate_limit_window_seconds,
            min_value=1,
        )
        self.enable_public_tenant_bootstrap = _parse_bool_env(
            "ALTHEA_ENABLE_PUBLIC_TENANT_BOOTSTRAP",
            self.enable_public_tenant_bootstrap,
        )
        self.allow_refresh_token_in_body = _parse_bool_env(
            "ALTHEA_ALLOW_REFRESH_TOKEN_IN_BODY",
            self.allow_refresh_token_in_body,
        )
        self.expose_refresh_token_in_response = _parse_bool_env(
            "ALTHEA_EXPOSE_REFRESH_TOKEN_IN_RESPONSE",
            self.expose_refresh_token_in_response,
        )
        self.refresh_cookie_secure = _parse_bool_env("ALTHEA_REFRESH_COOKIE_SECURE", self.refresh_cookie_secure)
        self.refresh_cookie_samesite = str(self.refresh_cookie_samesite or "strict").strip().lower()
        if self.refresh_cookie_samesite not in _COOKIE_SAMESITE_VALUES:
            allowed = ", ".join(sorted(_COOKIE_SAMESITE_VALUES))
            raise RuntimeError(f"ALTHEA_REFRESH_COOKIE_SAMESITE must be one of: {allowed}.")
        if self.refresh_cookie_samesite == "none" and not self.refresh_cookie_secure:
            raise RuntimeError("ALTHEA_REFRESH_COOKIE_SECURE must be true when ALTHEA_REFRESH_COOKIE_SAMESITE=none.")
        if self.is_non_dev and not self.refresh_cookie_secure:
            raise RuntimeError("ALTHEA_REFRESH_COOKIE_SECURE must be true in non-development environments.")
        self.refresh_cookie_name = str(self.refresh_cookie_name or "").strip()
        if not self.refresh_cookie_name:
            raise RuntimeError("ALTHEA_REFRESH_COOKIE_NAME must be non-empty.")
        self.refresh_cookie_path = str(self.refresh_cookie_path or "/api/auth").strip() or "/api/auth"
        if not self.refresh_cookie_path.startswith("/"):
            raise RuntimeError("ALTHEA_REFRESH_COOKIE_PATH must start with '/'.")
        self.refresh_cookie_domain = (str(self.refresh_cookie_domain or "").strip() or None)

        if os.getenv("ALTHEA_ALLOW_DEV_MODELS") is None:
            # Safe default: allow local bootstrap only in development.
            self.allow_dev_models = bool(self.is_dev)
        if self.is_pilot_mode() or self.is_production_mode():
            self.allow_dev_models = False
        if os.getenv("ALTHEA_ENABLE_PUBLIC_TENANT_BOOTSTRAP") is None:
            self.enable_public_tenant_bootstrap = False
        if os.getenv("ALTHEA_ALLOW_REFRESH_TOKEN_IN_BODY") is None:
            # Safer default: allow body refresh tokens in local development only.
            self.allow_refresh_token_in_body = bool(self.is_dev)
        if os.getenv("ALTHEA_EXPOSE_REFRESH_TOKEN_IN_RESPONSE") is None:
            self.expose_refresh_token_in_response = False
        # Refresh tokens must never be exposed to browser JavaScript/API response bodies.
        self.expose_refresh_token_in_response = False
        if os.getenv("ALTHEA_REFRESH_COOKIE_SECURE") is None:
            self.refresh_cookie_secure = bool(self.is_non_dev)

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

        if self.is_non_dev:
            if not self.allowed_origins:
                raise RuntimeError("ALTHEA_ALLOWED_ORIGINS must define at least one trusted origin in non-development environments.")
            for origin in self.allowed_origins:
                normalized = str(origin or "").strip().lower()
                if not normalized:
                    continue
                if normalized == "*" or "localhost" in normalized or normalized.startswith("http://"):
                    raise RuntimeError(
                        "ALTHEA_ALLOWED_ORIGINS must not contain wildcard, localhost, or insecure HTTP origins in non-development environments."
                    )
            if any(str(host or "").strip() in {"*", "0.0.0.0"} for host in self.allowed_hosts):
                raise RuntimeError("ALTHEA_ALLOWED_HOSTS must not contain wildcard hosts in non-development environments.")
            if self.cors_allow_origin_regex and "vercel" in str(self.cors_allow_origin_regex).lower():
                raise RuntimeError(
                    "ALTHEA_CORS_ALLOW_ORIGIN_REGEX must not use broad shared-host patterns in non-development environments."
                )

        default_secret = "change-me-in-production"
        if not self.jwt_secret or self.jwt_secret.strip() == default_secret:
            raise RuntimeError("ALTHEA_JWT_SECRET must be securely set")
        if self.is_non_dev and len(self.jwt_secret.strip()) < 32:
            raise RuntimeError("ALTHEA_JWT_SECRET must be at least 32 characters in non-development environments.")

        weak_secret_tokens = {"replace-with-strong-secret", "your-secret-key", "your-32-character-secret-key-here-min32chars"}
        if self.is_non_dev and any(token in (self.jwt_secret or "").lower() for token in weak_secret_tokens):
            raise RuntimeError("ALTHEA_JWT_SECRET uses an insecure placeholder value in non-development environment.")

        if self.enable_public_tenant_bootstrap and self.is_non_dev:
            bootstrap_secret = (self.bootstrap_provisioning_secret or "").strip()
            if not bootstrap_secret:
                raise RuntimeError(
                    "ALTHEA_BOOTSTRAP_PROVISIONING_SECRET must be set when ALTHEA_ENABLE_PUBLIC_TENANT_BOOTSTRAP is enabled."
                )
        if not self.tenant_header or not str(self.tenant_header).strip():
            raise RuntimeError("ALTHEA_TENANT_HEADER must be configured.")

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
    backend_root = Path(__file__).resolve().parent.parent
    project_root = backend_root.parent
    _load_env_file(project_root / ".env")
    _load_env_file(backend_root / ".env")

    settings = Settings()
    secret_value = _resolve_secret_value(settings.secret_key_ref)
    if secret_value:
        settings.jwt_secret = secret_value
    settings.validate()
    logger.info(
            "Phase 5 ingestion flags",
        extra={
            "runtime_mode": settings.runtime_mode,
            "enable_alert_jsonl_ingestion": settings.enable_alert_jsonl_ingestion,
            "enable_legacy_ingestion": settings.enable_legacy_ingestion,
            "enable_ibm_amlsim_import": settings.enable_ibm_amlsim_import,
            "enable_human_interpretation": settings.enable_human_interpretation,
            "strict_ingestion_validation": settings.strict_ingestion_validation,
            "alert_jsonl_max_upload_rows": settings.alert_jsonl_max_upload_rows,
            "ingestion_max_upload_bytes": settings.ingestion_max_upload_bytes,
            "primary_ingestion_mode": settings.primary_ingestion_mode,
            "alert_jsonl_path": "enabled" if settings.enable_alert_jsonl_ingestion else "disabled",
            "legacy_ingestion_mode": "emergency_override_only",
            "public_tenant_bootstrap": settings.enable_public_tenant_bootstrap,
            "refresh_cookie_secure": settings.refresh_cookie_secure,
            "refresh_cookie_samesite": settings.refresh_cookie_samesite,
            "allow_refresh_token_in_body": settings.allow_refresh_token_in_body,
            "cors_allow_origin_regex": bool(settings.cors_allow_origin_regex),
            "trusted_proxy_headers": settings.trusted_proxy_headers,
            "login_rate_limit_max_attempts": settings.login_rate_limit_max_attempts,
            "login_rate_limit_window_seconds": settings.login_rate_limit_window_seconds,
            "refresh_rate_limit_max_attempts": settings.refresh_rate_limit_max_attempts,
            "refresh_rate_limit_window_seconds": settings.refresh_rate_limit_window_seconds,
            "enrichment_enabled": settings.enrichment_enabled,
            "enrichment_sources_enabled": settings.enrichment_sources_enabled,
            "enrichment_rq_queue_name": settings.enrichment_rq_queue_name,
            "enrichment_sync_batch_size": settings.enrichment_sync_batch_size,
            "enrichment_health_stale_seconds": settings.enrichment_health_stale_seconds,
            "enrichment_sync_max_retries": settings.enrichment_sync_max_retries,
            "enrichment_connector_timeout_seconds": settings.enrichment_connector_timeout_seconds,
            "enrichment_connector_retry_max": settings.enrichment_connector_retry_max,
            "enrichment_connector_cooldown_seconds": settings.enrichment_connector_cooldown_seconds,
        },
    )
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.object_storage_root.mkdir(parents=True, exist_ok=True)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    settings.dead_letter_dir.mkdir(parents=True, exist_ok=True)
    settings.model_registry_dir.mkdir(parents=True, exist_ok=True)
    return settings
