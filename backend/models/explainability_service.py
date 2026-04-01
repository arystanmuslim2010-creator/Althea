"""
Unified model explainability service for runtime and governance paths.

The service attempts real model attribution (SHAP) first and only falls back to
heuristics with explicit metadata. Fallback output is never labeled as SHAP.
"""
from __future__ import annotations

import importlib
import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import numpy as np
import pandas as pd

from core.observability import (
    record_explanation_failure,
    record_explanation_fallback,
    record_explanation_generation,
)

logger = logging.getLogger("althea.explainability")


class ExplanationMethod(str, Enum):
    SHAP = "shap"
    TREE_SHAP = "tree_shap"
    NUMERIC_FALLBACK = "numeric_fallback"
    UNAVAILABLE = "unavailable"


class ExplanationStatus(str, Enum):
    OK = "ok"
    FALLBACK = "fallback"
    UNAVAILABLE = "unavailable"
    PARTIAL = "partial"


class ExplanationWarningCode(str, Enum):
    SHAP_NOT_INSTALLED = "shap_not_installed"
    UNSUPPORTED_MODEL = "unsupported_model"
    EXPLAINER_RUNTIME_ERROR = "explainer_runtime_error"
    FEATURE_FRAME_INCOMPATIBLE = "feature_frame_incompatible"
    MODEL_ARTIFACT_UNAVAILABLE = "model_artifact_unavailable"
    NO_FEATURE_DATA = "no_feature_data"
    NO_NUMERIC_FEATURES = "no_numeric_features"
    UNKNOWN = "unknown"


@dataclass
class ExplanationResult:
    feature_attribution: list[dict[str, Any]]
    risk_reason_codes: list[str]
    explanation_method: ExplanationMethod
    explanation_status: ExplanationStatus
    explanation_warning: Optional[str]
    explanation_warning_code: Optional[str]
    model_version: str
    shap_values: Optional[np.ndarray] = None

    def to_dict(self) -> dict[str, Any]:
        attribution_kind = (
            "model_shap"
            if self.explanation_method in {ExplanationMethod.SHAP, ExplanationMethod.TREE_SHAP}
            else "heuristic_feature_importance"
        )
        return {
            "feature_attribution": self.feature_attribution,
            "risk_reason_codes": self.risk_reason_codes,
            "explanation_method": self.explanation_method.value,
            "attribution_kind": attribution_kind,
            "explanation_status": self.explanation_status.value,
            "explanation_warning": self.explanation_warning,
            "explanation_warning_code": self.explanation_warning_code,
            "model_version": self.model_version,
        }

    def to_legacy_dict(self) -> dict[str, Any]:
        top_features = [item["feature"] for item in self.feature_attribution[:5] if item.get("feature")]
        top_contributions = self.feature_attribution[:5]

        risk_explain = {
            "feature_attribution": self.feature_attribution,
            "risk_reason_codes": self.risk_reason_codes,
            "explanation_method": self.explanation_method.value,
            "attribution_kind": (
                "model_shap"
                if self.explanation_method in {ExplanationMethod.SHAP, ExplanationMethod.TREE_SHAP}
                else "heuristic_feature_importance"
            ),
            "explanation_status": self.explanation_status.value,
            "explanation_warning": self.explanation_warning,
            "explanation_warning_code": self.explanation_warning_code,
        }

        return {
            "top_features": top_features,
            "top_feature_contributions": top_contributions,
            "risk_explain_json": risk_explain,
            "ml_service_explain_json": {
                "top_features": top_contributions,
                "model_version": self.model_version,
                "explanation_method": self.explanation_method.value,
                "explanation_status": self.explanation_status.value,
                "explanation_warning": self.explanation_warning,
                "explanation_warning_code": self.explanation_warning_code,
            },
        }


class ExplainabilityService:
    _shap_module: Optional[Any] = None
    _shap_checked: bool = False

    _SUPPORTED_MODEL_PREFIXES = (
        "sklearn.",
        "lightgbm.",
        "xgboost.",
    )
    _FEATURE_COMPATIBILITY_HINTS = (
        "shape",
        "feature",
        "column",
        "dim",
        "dtype",
        "categor",
    )

    @classmethod
    def _get_shap(cls):
        if cls._shap_checked:
            return cls._shap_module
        cls._shap_checked = True
        try:
            cls._shap_module = importlib.import_module("shap")
            logger.info("SHAP library loaded successfully")
        except Exception:
            cls._shap_module = None
            logger.warning("SHAP library is unavailable; runtime explainability will use fallback metadata.")
        return cls._shap_module

    @classmethod
    def reset_shap_cache(cls) -> None:
        cls._shap_module = None
        cls._shap_checked = False

    def _is_model_supported(self, model: Any) -> bool:
        class_path = f"{model.__class__.__module__}.{model.__class__.__name__}"
        return class_path.startswith(self._SUPPORTED_MODEL_PREFIXES)

    @staticmethod
    def _reason_codes(attribution: list[dict[str, Any]]) -> list[str]:
        codes: list[str] = []
        for item in attribution[:5]:
            feature = str(item.get("feature") or "unknown")
            value = float(item.get("value", 0.0) or 0.0)
            direction = "increase" if value >= 0 else "decrease"
            codes.append(f"{feature}:{direction}")
        return codes

    @staticmethod
    def _warning_text(code: str, detail: str | None = None) -> str:
        messages = {
            ExplanationWarningCode.SHAP_NOT_INSTALLED.value: (
                "Model-based attribution unavailable because SHAP is not installed; "
                "returned heuristic feature highlights only."
            ),
            ExplanationWarningCode.UNSUPPORTED_MODEL.value: (
                "Model type is not supported for SHAP attribution; returned heuristic feature highlights only."
            ),
            ExplanationWarningCode.EXPLAINER_RUNTIME_ERROR.value: (
                "Model-based attribution failed at runtime; returned heuristic feature highlights only."
            ),
            ExplanationWarningCode.FEATURE_FRAME_INCOMPATIBLE.value: (
                "Feature frame is incompatible with the explainer; returned heuristic feature highlights only."
            ),
            ExplanationWarningCode.MODEL_ARTIFACT_UNAVAILABLE.value: (
                "Model-based attribution unavailable because model artifact is not available in this path; "
                "returned heuristic or cached highlights only."
            ),
            ExplanationWarningCode.NO_FEATURE_DATA.value: "No feature data available for explanation.",
            ExplanationWarningCode.NO_NUMERIC_FEATURES.value: "No numeric features available for explanation fallback.",
            ExplanationWarningCode.UNKNOWN.value: "Model-based attribution unavailable.",
        }
        message = messages.get(code, messages[ExplanationWarningCode.UNKNOWN.value])
        if detail:
            return f"{message} Detail: {detail}"
        return message

    @staticmethod
    def _normalize_contributions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            feature = str(item.get("feature") or item.get("name") or "").strip()
            if not feature:
                continue
            raw_value = item.get("value", item.get("contribution", item.get("shap_value", 0.0)))
            try:
                value = float(raw_value or 0.0)
            except Exception:
                value = 0.0
            shap_raw = item.get("shap_value", item.get("contribution"))
            try:
                shap_value = None if shap_raw is None else float(shap_raw)
            except Exception:
                shap_value = None
            normalized.append(
                {
                    "feature": feature,
                    "value": value,
                    "magnitude": abs(value),
                    "shap_value": shap_value,
                }
            )
        return normalized[:5]

    def _numeric_fallback(
        self,
        feature_frame: pd.DataFrame,
        model_version: str = "unknown",
        warning_code: str = ExplanationWarningCode.UNKNOWN.value,
        warning_text: str | None = None,
        fallback_contributions: Optional[list[dict[str, Any]]] = None,
    ) -> ExplanationResult:
        fallback_list = list(fallback_contributions or [])
        if fallback_list:
            attribution = self._normalize_contributions(fallback_list)
            return ExplanationResult(
                feature_attribution=attribution,
                risk_reason_codes=self._reason_codes(attribution),
                explanation_method=ExplanationMethod.NUMERIC_FALLBACK,
                explanation_status=ExplanationStatus.FALLBACK,
                explanation_warning=warning_text or self._warning_text(warning_code),
                explanation_warning_code=warning_code,
                model_version=model_version,
            )

        if feature_frame is None or feature_frame.empty:
            code = ExplanationWarningCode.NO_FEATURE_DATA.value
            return ExplanationResult(
                feature_attribution=[],
                risk_reason_codes=[],
                explanation_method=ExplanationMethod.UNAVAILABLE,
                explanation_status=ExplanationStatus.UNAVAILABLE,
                explanation_warning=self._warning_text(code),
                explanation_warning_code=code,
                model_version=model_version,
            )

        numeric = feature_frame.select_dtypes(include=["number"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if numeric.empty:
            code = ExplanationWarningCode.NO_NUMERIC_FEATURES.value
            return ExplanationResult(
                feature_attribution=[],
                risk_reason_codes=[],
                explanation_method=ExplanationMethod.UNAVAILABLE,
                explanation_status=ExplanationStatus.UNAVAILABLE,
                explanation_warning=self._warning_text(code),
                explanation_warning_code=code,
                model_version=model_version,
            )

        row = numeric.iloc[0]
        ordered_features = row.abs().sort_values(ascending=False).head(5).index.tolist()
        attribution = [
            {
                "feature": str(name),
                "value": float(row[name]),
                "magnitude": float(abs(row[name])),
                "shap_value": None,
            }
            for name in ordered_features
        ]

        return ExplanationResult(
            feature_attribution=attribution,
            risk_reason_codes=self._reason_codes(attribution),
            explanation_method=ExplanationMethod.NUMERIC_FALLBACK,
            explanation_status=ExplanationStatus.FALLBACK,
            explanation_warning=warning_text or self._warning_text(warning_code),
            explanation_warning_code=warning_code,
            model_version=model_version,
        )

    def _classify_explainer_exception(self, exc: Exception) -> str:
        msg = str(exc).lower()
        if any(token in msg for token in self._FEATURE_COMPATIBILITY_HINTS):
            return ExplanationWarningCode.FEATURE_FRAME_INCOMPATIBLE.value
        return ExplanationWarningCode.EXPLAINER_RUNTIME_ERROR.value

    @staticmethod
    def _extract_shap_vector(shap_module: Any, shap_values: Any) -> np.ndarray:
        if isinstance(shap_values, list):
            # TreeExplainer for classifiers can return a list by class.
            arr = np.asarray(shap_values[-1])
            if arr.ndim == 1:
                return arr.astype(float)
            if arr.ndim >= 2:
                return arr[0].astype(float)

        if hasattr(shap_module, "Explanation") and isinstance(shap_values, shap_module.Explanation):
            arr = np.asarray(shap_values.values)
        else:
            arr = np.asarray(getattr(shap_values, "values", shap_values))

        if arr.ndim == 1:
            return arr.astype(float)
        if arr.ndim == 2:
            return arr[0].astype(float)
        if arr.ndim == 3:
            class_index = 1 if arr.shape[2] > 1 else 0
            return arr[0, :, class_index].astype(float)

        raise ValueError(f"Unsupported SHAP output dimensions: {arr.ndim}")

    def _shap_explanation(
        self,
        model: Any,
        feature_frame: pd.DataFrame,
        model_version: str = "unknown",
    ) -> ExplanationResult:
        shap_module = self._get_shap()
        if shap_module is None:
            return self._numeric_fallback(
                feature_frame=feature_frame,
                model_version=model_version,
                warning_code=ExplanationWarningCode.SHAP_NOT_INSTALLED.value,
            )

        try:
            model_class = f"{model.__class__.__module__}.{model.__class__.__name__}"
            is_tree_model = model_class.startswith(("lightgbm.", "xgboost.", "sklearn.ensemble."))
            method = ExplanationMethod.TREE_SHAP if is_tree_model else ExplanationMethod.SHAP

            if is_tree_model:
                explainer = shap_module.TreeExplainer(model)
                shap_values = explainer.shap_values(feature_frame)
            else:
                explainer = shap_module.Explainer(model, feature_frame)
                shap_values = explainer(feature_frame)

            vals = self._extract_shap_vector(shap_module, shap_values)
            order = np.argsort(np.abs(vals))[::-1][:5]
            feature_names = feature_frame.columns.tolist()

            attribution = [
                {
                    "feature": str(feature_names[idx]),
                    "value": float(vals[idx]),
                    "magnitude": float(abs(vals[idx])),
                    "shap_value": float(vals[idx]),
                }
                for idx in order
                if idx < len(feature_names)
            ]
            return ExplanationResult(
                feature_attribution=attribution,
                risk_reason_codes=self._reason_codes(attribution),
                explanation_method=method,
                explanation_status=ExplanationStatus.OK,
                explanation_warning=None,
                explanation_warning_code=None,
                model_version=model_version,
                shap_values=vals,
            )

        except Exception as exc:
            warning_code = self._classify_explainer_exception(exc)
            return self._numeric_fallback(
                feature_frame=feature_frame,
                model_version=model_version,
                warning_code=warning_code,
                warning_text=self._warning_text(warning_code, detail=str(exc)),
            )

    def _record_observability(
        self,
        result: ExplanationResult,
        duration_seconds: float,
        tenant_id: str | None,
        alert_id: str | None,
        model_version: str,
        feature_schema_version: str | None,
    ) -> None:
        method = result.explanation_method.value
        status = result.explanation_status.value
        warning_code = result.explanation_warning_code or ExplanationWarningCode.UNKNOWN.value

        record_explanation_generation(method=method, status=status, duration_seconds=duration_seconds)
        if status in {ExplanationStatus.FALLBACK.value, ExplanationStatus.UNAVAILABLE.value}:
            record_explanation_fallback(method=method, reason=warning_code)
            record_explanation_failure(reason=warning_code)

        event = {
            "event": "explanation_generated",
            "tenant_id": tenant_id,
            "alert_id": alert_id,
            "model_version": model_version,
            "feature_schema_version": feature_schema_version,
            "explanation_method": method,
            "explanation_status": status,
            "explanation_warning_code": result.explanation_warning_code,
            "duration_ms": round(duration_seconds * 1000.0, 3),
        }
        log = logger.info if status == ExplanationStatus.OK.value else logger.warning
        log(json.dumps(event, ensure_ascii=True))

    def generate_explanation(
        self,
        model: Optional[Any],
        feature_frame: pd.DataFrame,
        model_version: str = "unknown",
        fallback_contributions: Optional[list[dict[str, Any]]] = None,
        tenant_id: str | None = None,
        alert_id: str | None = None,
        feature_schema_version: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()

        if feature_frame is None or feature_frame.empty:
            result = self._numeric_fallback(
                feature_frame=feature_frame,
                model_version=model_version,
                warning_code=ExplanationWarningCode.NO_FEATURE_DATA.value,
                fallback_contributions=fallback_contributions,
            )
            self._record_observability(
                result=result,
                duration_seconds=time.perf_counter() - started,
                tenant_id=tenant_id,
                alert_id=alert_id,
                model_version=model_version,
                feature_schema_version=feature_schema_version,
            )
            return result.to_dict()

        if model is None:
            warning_text = None
            fallback_list = list(fallback_contributions or [])
            if fallback_list:
                warning_text = (
                    "Model-based attribution unavailable in this path; "
                    "returning stored feature highlights from prior scoring."
                )
            result = self._numeric_fallback(
                feature_frame=feature_frame,
                model_version=model_version,
                warning_code=ExplanationWarningCode.MODEL_ARTIFACT_UNAVAILABLE.value,
                warning_text=warning_text,
                fallback_contributions=fallback_list,
            )
            self._record_observability(
                result=result,
                duration_seconds=time.perf_counter() - started,
                tenant_id=tenant_id,
                alert_id=alert_id,
                model_version=model_version,
                feature_schema_version=feature_schema_version,
            )
            return result.to_dict()

        if not self._is_model_supported(model):
            result = self._numeric_fallback(
                feature_frame=feature_frame,
                model_version=model_version,
                warning_code=ExplanationWarningCode.UNSUPPORTED_MODEL.value,
            )
            self._record_observability(
                result=result,
                duration_seconds=time.perf_counter() - started,
                tenant_id=tenant_id,
                alert_id=alert_id,
                model_version=model_version,
                feature_schema_version=feature_schema_version,
            )
            return result.to_dict()

        result = self._shap_explanation(model, feature_frame, model_version)
        self._record_observability(
            result=result,
            duration_seconds=time.perf_counter() - started,
            tenant_id=tenant_id,
            alert_id=alert_id,
            model_version=model_version,
            feature_schema_version=feature_schema_version,
        )
        return result.to_dict()

    def generate_explanation_result(
        self,
        model: Optional[Any],
        feature_frame: pd.DataFrame,
        model_version: str = "unknown",
    ) -> ExplanationResult:
        payload = self.generate_explanation(
            model=model,
            feature_frame=feature_frame,
            model_version=model_version,
        )
        method_value = str(payload.get("explanation_method") or ExplanationMethod.UNAVAILABLE.value)
        status_value = str(payload.get("explanation_status") or ExplanationStatus.UNAVAILABLE.value)
        try:
            method = ExplanationMethod(method_value)
        except ValueError:
            method = ExplanationMethod.UNAVAILABLE
        try:
            status = ExplanationStatus(status_value)
        except ValueError:
            status = ExplanationStatus.UNAVAILABLE
        return ExplanationResult(
            feature_attribution=list(payload.get("feature_attribution") or []),
            risk_reason_codes=list(payload.get("risk_reason_codes") or []),
            explanation_method=method,
            explanation_status=status,
            explanation_warning=payload.get("explanation_warning"),
            explanation_warning_code=payload.get("explanation_warning_code"),
            model_version=str(payload.get("model_version") or model_version),
            shap_values=None,
        )

    def merge_into_alert_metadata(
        self,
        alert_payload: dict[str, Any],
        explanation: dict[str, Any],
    ) -> dict[str, Any]:
        out = dict(alert_payload or {})

        existing = out.get("risk_explain_json")
        if isinstance(existing, str):
            try:
                existing = json.loads(existing)
            except Exception:
                existing = {}
        if not isinstance(existing, dict):
            existing = {}

        merged = {
            **existing,
            "feature_attribution": explanation.get("feature_attribution", []),
            "risk_reason_codes": explanation.get("risk_reason_codes", []),
            "explanation_method": explanation.get("explanation_method", "unknown"),
            "explanation_status": explanation.get("explanation_status", "unknown"),
            "explanation_warning": explanation.get("explanation_warning"),
            "explanation_warning_code": explanation.get("explanation_warning_code"),
        }

        out["risk_explain_json"] = json.dumps(merged, ensure_ascii=True)
        out["top_feature_contributions_json"] = json.dumps(
            explanation.get("feature_attribution", []),
            ensure_ascii=True,
        )
        out["top_features_json"] = json.dumps(
            [item.get("feature") for item in explanation.get("feature_attribution", []) if isinstance(item, dict)],
            ensure_ascii=True,
        )
        out["ml_service_explain_json"] = json.dumps(
            {
                "top_features": explanation.get("feature_attribution", []),
                "model_version": explanation.get("model_version", out.get("model_version", "unknown")),
                "explanation_method": explanation.get("explanation_method", "unknown"),
                "explanation_status": explanation.get("explanation_status", "unknown"),
                "explanation_warning": explanation.get("explanation_warning"),
                "explanation_warning_code": explanation.get("explanation_warning_code"),
            },
            ensure_ascii=True,
        )
        return out


_explainability_service: Optional[ExplainabilityService] = None


def get_explainability_service() -> ExplainabilityService:
    global _explainability_service
    if _explainability_service is None:
        _explainability_service = ExplainabilityService()
    return _explainability_service
