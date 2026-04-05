"""Training run service — orchestrates the complete ML training pipeline.

Sequence:
    1. Build labeled dataset (TrainingDatasetBuilder)
    2. Assign training labels (LabelBuilder)
    3. Time-based train/val/test split (TimeBasedSplitter)
    4. Train escalation model (EscalationModelTrainer)
    5. Train time model (InvestigationTimeTrainer)
    6. Calibrate escalation probabilities (ProbabilityCalibrator)
    7. Evaluate both models (ModelEvaluator)
    8. Publish artifacts (ModelPublisher)
    9. Persist training run record

The service is intentionally stateless; each call to ``run()`` performs
a full training cycle and returns a summary dict suitable for API responses
and audit log persistence.
"""
from __future__ import annotations

import json
import logging
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from training.calibration import ProbabilityCalibrator
from training.dataset_builder import TrainingDatasetBuilder
from training.evaluator import ModelEvaluator
from training.label_builder import LabelBuilder
from training.publisher import ModelPublisher
from training.splitter import TimeBasedSplitter
from training.train_escalation_model import EscalationModelTrainer
from training.train_time_model import InvestigationTimeTrainer

logger = logging.getLogger("althea.training.run_service")


class TrainingRunService:
    """Orchestrate a complete ML training run for a single tenant.

    Parameters
    ----------
    repository         : EnterpriseRepository instance
    object_storage     : ObjectStorage instance
    model_registry     : ModelRegistry instance
    dataset_builder    : optional override (defaults to TrainingDatasetBuilder)
    label_builder      : optional override (defaults to LabelBuilder)
    splitter           : optional override (defaults to TimeBasedSplitter)
    escalation_trainer : optional override
    time_trainer       : optional override
    calibrator         : optional override
    evaluator          : optional override
    publisher          : optional override
    auto_approve       : publish escalation model in 'approved' state
    auto_activate      : set model as active immediately after approval
    train_time_model   : whether to train the time model in this run
    """

    def __init__(
        self,
        repository,
        object_storage,
        model_registry,
        dataset_builder: TrainingDatasetBuilder | None = None,
        label_builder: LabelBuilder | None = None,
        splitter: TimeBasedSplitter | None = None,
        escalation_trainer: EscalationModelTrainer | None = None,
        time_trainer: InvestigationTimeTrainer | None = None,
        calibrator: ProbabilityCalibrator | None = None,
        evaluator: ModelEvaluator | None = None,
        publisher: ModelPublisher | None = None,
        auto_approve: bool = False,
        auto_activate: bool = False,
        train_time_model: bool = True,
    ) -> None:
        self._repository = repository
        self._dataset_builder = dataset_builder or TrainingDatasetBuilder(repository)
        self._label_builder = label_builder or LabelBuilder()
        self._splitter = splitter or TimeBasedSplitter(entity_col="user_id")
        self._esc_trainer = escalation_trainer or EscalationModelTrainer()
        self._time_trainer = time_trainer or InvestigationTimeTrainer()
        self._calibrator = calibrator or ProbabilityCalibrator(method="isotonic")
        self._evaluator = evaluator or ModelEvaluator()
        self._publisher = publisher or ModelPublisher(
            model_registry=model_registry,
            object_storage=object_storage,
            auto_approve=auto_approve,
            auto_activate=auto_activate,
        )
        self._train_time_model = train_time_model

    def run(
        self,
        tenant_id: str,
        cutoff_timestamp: datetime | None = None,
        escalation_model_version: str | None = None,
        time_model_version: str | None = None,
        initiated_by: str = "system",
    ) -> dict[str, Any]:
        """Execute a complete training run.

        Returns a summary dict containing:
        - training_run_id
        - escalation model version and metrics
        - time model version and metrics (if trained)
        - split statistics
        - calibration metrics
        - errors / warnings if any step failed gracefully
        """
        training_run_id = f"train-{uuid.uuid4().hex[:12]}"
        started_at = datetime.now(timezone.utc)

        logger.info(
            json.dumps(
                {
                    "event": "training_run_start",
                    "training_run_id": training_run_id,
                    "tenant_id": tenant_id,
                    "initiated_by": initiated_by,
                    "started_at": started_at.isoformat(),
                },
                ensure_ascii=True,
            )
        )

        summary: dict[str, Any] = {
            "training_run_id": training_run_id,
            "tenant_id": tenant_id,
            "started_at": started_at.isoformat(),
            "initiated_by": initiated_by,
            "status": "running",
            "escalation_model": None,
            "time_model": None,
            "errors": [],
            "warnings": [],
        }

        try:
            # ----------------------------------------------------------------
            # Step 1 & 2: Dataset + labels for escalation model
            # ----------------------------------------------------------------
            esc_dataset = self._dataset_builder.build_escalation_dataset(
                tenant_id=tenant_id,
                cutoff_timestamp=cutoff_timestamp,
            )
            esc_labeled = self._label_builder.build_escalation_labels(esc_dataset)
            label_summary = self._label_builder.label_summary(esc_labeled)
            summary["label_summary"] = label_summary

            # ----------------------------------------------------------------
            # Step 3: Time-based split
            # ----------------------------------------------------------------
            split = self._splitter.split(esc_labeled)
            summary["split_metadata"] = split.metadata

            if split.train.empty or split.validation.empty:
                raise ValueError(
                    "Train or validation partition is empty after splitting. "
                    "More labeled data is needed."
                )

            # ----------------------------------------------------------------
            # Step 4: Train escalation model
            # ----------------------------------------------------------------
            esc_result = self._esc_trainer.train(
                train_df=split.train,
                val_df=split.validation,
            )

            # ----------------------------------------------------------------
            # Step 6: Calibrate escalation probabilities on validation set
            # ----------------------------------------------------------------
            X_val = split.validation[esc_result.feature_columns].replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
            y_val = split.validation["escalation_label"].astype(int)

            val_probs = self._get_raw_probs(esc_result.model, X_val)
            calib_result = self._calibrator.fit(val_probs, y_val.to_numpy())
            esc_result.training_metadata.update(calib_result.metadata)

            # ----------------------------------------------------------------
            # Step 7a: Evaluate escalation model on test set
            # ----------------------------------------------------------------
            esc_eval_metrics: dict[str, Any] = {}
            if not split.test.empty:
                X_test = split.test[esc_result.feature_columns].replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
                y_test = split.test["escalation_label"].astype(int)
                test_probs_raw = self._get_raw_probs(esc_result.model, X_test)
                test_probs = self._calibrator.transform(test_probs_raw, calib_result.calibrator)
                seg_col = split.test.get("segment") if isinstance(split.test, pd.DataFrame) else None
                typ_col = split.test.get("typology") if isinstance(split.test, pd.DataFrame) else None
                esc_eval_metrics = self._evaluator.evaluate_escalation(
                    y_true=y_test.to_numpy(),
                    y_prob=test_probs,
                    segments=seg_col,
                    typologies=typ_col,
                )
            esc_result.metrics = esc_eval_metrics

            # ----------------------------------------------------------------
            # Step 8a: Publish escalation model
            # ----------------------------------------------------------------
            import hashlib
            dataset_hash = hashlib.sha256(
                str(label_summary.get("rows", 0)).encode()
            ).hexdigest()[:16]

            esc_record = self._publisher.publish_escalation_model(
                tenant_id=tenant_id,
                artifact_bytes=esc_result.artifact_bytes,
                feature_schema=esc_result.feature_schema,
                metrics=esc_eval_metrics,
                training_metadata=esc_result.training_metadata,
                calibration_artifact_bytes=calib_result.artifact_bytes,
                evaluation_report=esc_eval_metrics,
                training_run_id=training_run_id,
                dataset_hash=dataset_hash,
                model_version=escalation_model_version,
            )

            summary["escalation_model"] = {
                "model_version": esc_record.get("model_version"),
                "approval_status": esc_record.get("approval_status"),
                "artifact_format": esc_result.artifact_format,
                "feature_count": len(esc_result.feature_columns),
                "calibration_ece_after": calib_result.calibration_error_after,
                "metrics": {
                    "pr_auc": esc_eval_metrics.get("pr_auc"),
                    "roc_auc": esc_eval_metrics.get("roc_auc"),
                    "suspicious_capture_top_20pct": esc_eval_metrics.get("suspicious_capture_top_20pct"),
                    "lift_top_decile": esc_eval_metrics.get("lift_top_decile"),
                },
            }

        except Exception as exc:
            summary["status"] = "failed"
            summary["errors"].append(
                {"step": "escalation_model_training", "error": str(exc), "trace": traceback.format_exc()}
            )
            logger.exception(
                "Training run %s failed at escalation model step: %s",
                training_run_id,
                exc,
            )
            summary["completed_at"] = datetime.now(timezone.utc).isoformat()
            return summary

        # ----------------------------------------------------------------
        # Step 5 / 7b / 8b: Time model (optional — soft failure)
        # ----------------------------------------------------------------
        if self._train_time_model:
            try:
                time_dataset = self._dataset_builder.build_time_dataset(
                    tenant_id=tenant_id,
                    cutoff_timestamp=cutoff_timestamp,
                )
                time_labeled = self._label_builder.build_time_labels(time_dataset)
                time_split = self._splitter.split(time_labeled)

                time_result = self._time_trainer.train(
                    train_df=time_split.train,
                    val_df=time_split.validation,
                )

                time_eval_metrics: dict[str, Any] = {}
                if not time_split.test.empty and "resolution_hours" in time_split.test.columns:
                    X_test_t = time_split.test[time_result.feature_columns].replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
                    y_pred_log = self._get_regression_pred(time_result.model_p50, X_test_t)
                    time_eval_metrics = self._evaluator.evaluate_time_model(
                        y_true=time_split.test["resolution_hours"].to_numpy(),
                        y_pred_log=y_pred_log,
                    )
                time_result.metrics = time_eval_metrics

                time_record = self._publisher.publish_time_model(
                    tenant_id=tenant_id,
                    artifact_bytes_p50=time_result.artifact_bytes_p50,
                    artifact_bytes_p90=time_result.artifact_bytes_p90,
                    feature_schema=time_result.feature_schema,
                    metrics=time_eval_metrics,
                    training_metadata=time_result.training_metadata,
                    training_run_id=training_run_id,
                    dataset_hash=dataset_hash,
                    model_version=time_model_version,
                )

                summary["time_model"] = {
                    "model_version": time_record.get("model_version"),
                    "metrics": {
                        "mae_hours": time_eval_metrics.get("mae_hours"),
                        "rmse_hours": time_eval_metrics.get("rmse_hours"),
                        "median_ae_hours": time_eval_metrics.get("median_ae_hours"),
                    },
                }

            except Exception as exc:
                summary["warnings"].append(
                    {"step": "time_model_training", "warning": str(exc)}
                )
                logger.warning("Time model training failed (non-fatal): %s", exc)

        # ----------------------------------------------------------------
        # Finalize
        # ----------------------------------------------------------------
        summary["status"] = "completed"
        summary["completed_at"] = datetime.now(timezone.utc).isoformat()

        try:
            self._publisher.publish_training_run_record(
                tenant_id=tenant_id,
                training_run_id=training_run_id,
                run_metadata=summary,
            )
        except Exception as exc:
            summary["warnings"].append({"step": "run_record_publish", "warning": str(exc)})

        logger.info(
            json.dumps(
                {
                    "event": "training_run_complete",
                    "training_run_id": training_run_id,
                    "tenant_id": tenant_id,
                    "status": summary["status"],
                    "escalation_model_version": (summary.get("escalation_model") or {}).get("model_version"),
                    "time_model_version": (summary.get("time_model") or {}).get("model_version"),
                },
                ensure_ascii=True,
            )
        )
        return summary

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_raw_probs(model: Any, X: pd.DataFrame) -> np.ndarray:
        """Extract raw P(class=1) probabilities from a fitted model."""
        if hasattr(model, "predict_proba"):
            pred = np.asarray(model.predict_proba(X))
            if pred.ndim == 2 and pred.shape[1] >= 2:
                return np.clip(pred[:, 1].astype(float), 0.0, 1.0)
            return np.clip(pred.ravel().astype(float), 0.0, 1.0)
        try:
            import lightgbm as lgb
            if hasattr(model, "booster_"):
                return np.clip(model.booster_.predict(X).astype(float), 0.0, 1.0)
        except Exception:
            pass
        return np.clip(np.asarray(model.predict(X), dtype=float), 0.0, 1.0)

    @staticmethod
    def _get_regression_pred(model: Any, X: pd.DataFrame) -> np.ndarray:
        """Get regression predictions from a fitted model."""
        if hasattr(model, "predict"):
            return np.asarray(model.predict(X), dtype=float)
        return np.zeros(len(X), dtype=float)
