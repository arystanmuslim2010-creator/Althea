from __future__ import annotations

from typing import Any

from evaluation.baselines import build_baseline_rankings
from evaluation.metrics import compute_ranking_metrics, validate_binary_labels


class EvaluationService:
    def __init__(self, repository=None) -> None:
        self._repository = repository

    @staticmethod
    def _best_baseline_name(baselines: dict[str, dict[str, Any]]) -> str | None:
        candidates = []
        for name, metrics in baselines.items():
            if name == "random" or not metrics.get("is_valid"):
                continue
            candidates.append(
                (
                    metrics.get("recall_at_top_20_pct") or 0.0,
                    metrics.get("precision_at_top_20_pct") or 0.0,
                    metrics.get("pr_auc") or 0.0,
                    name,
                )
            )
        if not candidates:
            return None
        candidates.sort(reverse=True)
        return str(candidates[0][3])

    @staticmethod
    def _lift(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator is None or denominator <= 0:
            return None
        return round(numerator / denominator, 4)

    def evaluate_records(
        self,
        *,
        dataset_name: str,
        records: list[dict[str, Any]],
        label_field: str = "evaluation_label_is_sar",
        althea_score_field: str = "risk_score",
    ) -> dict[str, Any]:
        clean_records = [dict(row or {}) for row in records]
        validation = validate_binary_labels(clean_records, label_field)
        total_alerts = len(clean_records)
        positive_alerts = int(validation.get("positive_alerts") or 0)
        rankings = build_baseline_rankings(
            clean_records,
            althea_score_field=althea_score_field,
            dataset_name=dataset_name,
        )

        metrics_by_ranking = {
            name: compute_ranking_metrics(rows, label_field=label_field, score_field="ranking_score")
            for name, rows in rankings.items()
        }
        random_metrics = metrics_by_ranking.get("random", {})
        althea_metrics = dict(metrics_by_ranking.get("althea", {}))

        baselines = {
            name: {
                **metrics,
                "lift_over_random_at_top_20_pct": self._lift(
                    metrics.get("recall_at_top_20_pct"),
                    random_metrics.get("recall_at_top_20_pct"),
                ),
            }
            for name, metrics in metrics_by_ranking.items()
            if name != "althea"
        }
        best_baseline = self._best_baseline_name(baselines)
        best_baseline_metrics = baselines.get(best_baseline or "", {})
        lift_over_best = self._lift(
            althea_metrics.get("recall_at_top_20_pct"),
            best_baseline_metrics.get("recall_at_top_20_pct"),
        )
        althea_metrics["lift_over_random_at_top_20_pct"] = self._lift(
            althea_metrics.get("recall_at_top_20_pct"),
            random_metrics.get("recall_at_top_20_pct"),
        )

        if not validation["is_valid"]:
            summary_text = validation["warning"]
        else:
            top20_capture = round((althea_metrics.get("recall_at_top_20_pct") or 0.0) * 100.0)
            if best_baseline and lift_over_best is not None:
                readable_baseline = best_baseline.replace("_", " ")
                summary_text = (
                    f"ALTHEA captured {top20_capture}% of suspicious alerts in the top 20% of the queue, "
                    f"outperforming the {readable_baseline} baseline by {lift_over_best:.2f}x."
                )
            else:
                summary_text = (
                    f"ALTHEA captured {top20_capture}% of suspicious alerts in the top 20% of the queue."
                )

        return {
            "dataset_name": dataset_name,
            "total_alerts": total_alerts,
            "positive_alerts": positive_alerts,
            "label_field": label_field,
            "evaluation_valid": bool(validation["is_valid"]),
            "warning": validation.get("warning"),
            "baselines": baselines,
            "althea_metrics": althea_metrics,
            "best_baseline": best_baseline,
            "lift_over_best_baseline": lift_over_best,
            "summary_text": summary_text,
        }

    def evaluate_run(
        self,
        *,
        tenant_id: str,
        run_id: str,
        dataset_name: str | None = None,
        label_field: str = "evaluation_label_is_sar",
        althea_score_field: str = "risk_score",
    ) -> dict[str, Any]:
        if self._repository is None:
            raise ValueError("Evaluation repository is not configured")
        records = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        return self.evaluate_records(
            dataset_name=dataset_name or run_id,
            records=records,
            label_field=label_field,
            althea_score_field=althea_score_field,
        )
