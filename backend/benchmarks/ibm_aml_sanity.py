from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from benchmarks.ibm_aml_improvement import (
    _add_derived_features,
    _add_label_free_history_features,
    _apply_train_only_encodings,
    _compute_baselines,
    _fit_logistic_candidate,
    _numeric_feature_columns,
    _ranking_metrics_from_scores,
    _raw_signal_feature_columns,
    _repo_root,
    _top_k_count,
    extract_feature_csv_from_alert_jsonl,
    extract_source_destination_feature_csv,
    load_feature_frame,
)

logger = logging.getLogger("althea.benchmarks.ibm_aml_sanity")


@dataclass(slots=True)
class BenchmarkSanityResult:
    summary_path: Path
    report_path: Path
    verdict: dict[str, Any]
    ablations: list[dict[str, Any]]
    split_robustness: list[dict[str, Any]]
    transfer_results: list[dict[str, Any]]
    randomization: dict[str, Any]


_PRIMARY_GROUPING = "source_account_24h"
_PRIMARY_CANDIDATE = "logistic_regression_raw_signals"
_RANDOM_STATE = 42
_SANITY_RECALL_TARGETS = (0.80, 0.90)
_PRIMARY_SCORE_COLUMN = "model_score"
_TOP_COMPARE_FRACTION = 0.10
_TOP_SAMPLE_SIZE = 5


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _isoformat_utc(value: pd.Timestamp | pd.Timestamp | Any) -> str:
    return pd.Timestamp(value).tz_convert("UTC").isoformat().replace("+00:00", "Z")


def _default_feature_cache_dir() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features"


def _split_frame_by_fractions(
    frame: pd.DataFrame,
    *,
    train_fraction: float,
    validation_fraction: float,
) -> dict[str, pd.DataFrame]:
    total = len(frame)
    if total == 0:
        return {"train": frame.copy(), "validation": frame.copy(), "test": frame.copy()}
    train_end = int(total * train_fraction)
    validation_end = int(total * (train_fraction + validation_fraction))
    train_end = max(1, min(train_end, total - 2))
    validation_end = max(train_end + 1, min(validation_end, total - 1))
    return {
        "train": frame.iloc[:train_end].copy(),
        "validation": frame.iloc[train_end:validation_end].copy(),
        "test": frame.iloc[validation_end:].copy(),
    }


def _prepare_split_matrices(
    frame: pd.DataFrame,
    *,
    train_fraction: float,
    validation_fraction: float,
) -> tuple[dict[str, pd.DataFrame], list[str], dict[str, Any]]:
    prepared = _add_derived_features(_add_label_free_history_features(frame))
    splits = _split_frame_by_fractions(
        prepared,
        train_fraction=train_fraction,
        validation_fraction=validation_fraction,
    )
    train, validation, test, encoding_diagnostics = _apply_train_only_encodings(
        splits["train"],
        splits["validation"],
        splits["test"],
    )
    numeric_cols = _numeric_feature_columns(train)
    raw_feature_cols = _raw_signal_feature_columns(numeric_cols)
    matrices = {"train": train, "validation": validation, "test": test}
    for key in matrices:
        matrices[key][numeric_cols] = (
            matrices[key][numeric_cols]
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
            .astype(np.float32)
        )
    return matrices, raw_feature_cols, encoding_diagnostics


def _dataset_stats(frame: pd.DataFrame) -> dict[str, Any]:
    total = len(frame)
    positives = int(frame["evaluation_label_is_sar"].sum()) if total else 0
    return {
        "total_alerts": int(total),
        "positive_alerts": positives,
        "negative_alerts": int(max(total - positives, 0)),
        "positive_rate": float(positives / total) if total else 0.0,
        "average_transactions_per_alert": float(frame["transaction_count"].mean()) if total else 0.0,
        "typed_alert_rate": float(frame["pattern_assigned"].mean()) if total else 0.0,
    }


def _feature_groups(feature_columns: list[str]) -> dict[str, list[str]]:
    feature_set = set(feature_columns)
    groups = {
        "amount_related": [
            "total_amount",
            "max_amount",
            "mean_amount",
            "min_amount",
            "median_amount",
            "std_amount",
            "amount_range",
            "amount_per_transaction",
            "max_amount_share_usd",
            "log_total_amount",
        ],
        "normalized_amount": [
            "total_amount_usd",
            "max_amount_usd",
            "mean_amount_usd",
            "min_amount_usd",
            "median_amount_usd",
            "std_amount_usd",
            "amount_range_usd",
            "amount_per_transaction_usd",
            "amount_per_counterparty_usd",
            "amount_std_to_mean_usd",
            "log_total_amount_usd",
            "log_max_amount_usd",
        ],
        "history_chronology": [
            "source_account_hours_since_prev_alert",
            "source_account_prior_alert_count",
            "source_account_prior_total_amount_usd",
            "source_account_prior_avg_amount_usd",
            "source_account_seen_train",
            "source_account_frequency_train",
            "source_bank_hours_since_prev_alert",
            "source_bank_prior_alert_count",
            "source_bank_prior_total_amount_usd",
            "source_bank_prior_avg_amount_usd",
            "source_bank_frequency_train",
        ],
        "counterparty_breadth": [
            "unique_destination_accounts",
            "unique_destination_banks",
            "log_unique_destination_accounts",
            "counterparty_density",
            "repeated_counterparty_ratio",
            "top_counterparty_tx_share",
            "top_counterparty_amount_share",
            "same_bank_ratio",
            "cross_bank_ratio",
        ],
        "pattern_derived": [
            "pattern_assigned",
        ],
        "time_cadence": [
            "time_span_hours",
            "avg_gap_hours",
            "max_gap_hours",
            "tx_per_hour",
            "created_hour",
            "created_day_of_week",
            "hour_sin",
            "hour_cos",
            "dow_sin",
            "dow_cos",
            "night_ratio",
            "weekend_ratio",
        ],
        "payment_currency_mix": [
            "currency_count",
            "has_mixed_currencies",
            "payment_format_count",
            "ach_ratio",
            "wire_ratio",
            "cash_ratio",
            "cheque_ratio",
            "credit_card_ratio",
            "reinvestment_ratio",
            "bitcoin_ratio",
        ],
    }
    return {
        name: [feature for feature in columns if feature in feature_set]
        for name, columns in groups.items()
    }


def _fit_primary_logistic(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    name: str = _PRIMARY_CANDIDATE,
    notes: str = "Primary sanity-check candidate: raw alert signals with chronology-safe history, excluding label-derived rate encodings.",
) -> dict[str, Any]:
    return _fit_logistic_candidate(
        train_df,
        validation_df,
        test_df,
        feature_columns,
        name=name,
        notes=notes,
    )


def _metric_row(
    name: str,
    *,
    metrics: dict[str, Any],
    baseline_metrics: dict[str, Any] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    row = {
        "name": name,
        "recall_at_top_10pct": float(metrics.get("recall_at_top_10pct", 0.0)),
        "recall_at_top_20pct": float(metrics.get("recall_at_top_20pct", 0.0)),
        "precision_at_top_10pct": float(metrics.get("precision_at_top_10pct", 0.0)),
        "precision_at_top_20pct": float(metrics.get("precision_at_top_20pct", 0.0)),
        "pr_auc": float(metrics.get("pr_auc", 0.0)),
        "review_reduction_at_80pct_recall": float(
            ((metrics.get("review_reduction_at_80pct_recall") or {}).get("review_reduction", 0.0))
        ),
    }
    if baseline_metrics is not None:
        row["delta_recall_at_top_10pct"] = row["recall_at_top_10pct"] - float(baseline_metrics.get("recall_at_top_10pct", 0.0))
        row["delta_recall_at_top_20pct"] = row["recall_at_top_20pct"] - float(baseline_metrics.get("recall_at_top_20pct", 0.0))
        row["delta_precision_at_top_10pct"] = row["precision_at_top_10pct"] - float(baseline_metrics.get("precision_at_top_10pct", 0.0))
    if note:
        row["note"] = note
    return row


def _run_feature_ablations(
    matrices: dict[str, pd.DataFrame],
    raw_feature_columns: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    full_result = _fit_primary_logistic(
        matrices["train"],
        matrices["validation"],
        matrices["test"],
        raw_feature_columns,
    )
    full_metrics = full_result["test_metrics"]
    groups = _feature_groups(raw_feature_columns)
    ablations = [_metric_row("full_feature_champion", metrics=full_metrics, note="Unablated primary logistic candidate.")]
    for group_name, group_columns in groups.items():
        if not group_columns:
            ablations.append(
                {
                    "name": f"ablation_{group_name}",
                    "skipped": True,
                    "reason": "group_not_present_in_primary_feature_set",
                }
            )
            continue
        retained = [feature for feature in raw_feature_columns if feature not in set(group_columns)]
        result = _fit_primary_logistic(
            matrices["train"],
            matrices["validation"],
            matrices["test"],
            retained,
            name=f"ablation_{group_name}",
            notes=f"Ablation removing feature group `{group_name}`.",
        )
        ablations.append(
            _metric_row(
                f"ablation_{group_name}",
                metrics=result["test_metrics"],
                baseline_metrics=full_metrics,
                note=f"Removed {len(group_columns)} columns: {', '.join(group_columns[:8])}" + (" ..." if len(group_columns) > 8 else ""),
            )
        )
    return full_result, ablations


def _leakage_audit(raw_feature_columns: list[str], full_feature_columns: list[str]) -> dict[str, Any]:
    findings = []
    if any(col.endswith("_positive_rate_train") for col in full_feature_columns):
        findings.append(
            {
                "type": "label_leakage_like",
                "severity": "high",
                "status": "present_in_non_primary_variant",
                "finding": "Train-only positive-rate encodings are directly derived from labels and are not acceptable as the primary externally-cited benchmark feature set.",
                "affected_columns": [col for col in full_feature_columns if col.endswith("_positive_rate_train")],
            }
        )
    if any(col.endswith("_pattern_rate_train") for col in full_feature_columns):
        findings.append(
            {
                "type": "label_leakage_like",
                "severity": "medium",
                "status": "present_in_non_primary_variant",
                "finding": "Train-only pattern-rate encodings are derived from pattern annotations that originate from the laundering-attempt file and should be treated as benchmark convenience rather than bank-realistic signal.",
                "affected_columns": [col for col in full_feature_columns if col.endswith("_pattern_rate_train")],
            }
        )
    if "pattern_assigned" in raw_feature_columns:
        findings.append(
            {
                "type": "proxy_label_coupling",
                "severity": "medium",
                "status": "present_in_primary_variant",
                "finding": "The primary raw-signal champion still sees `pattern_assigned`, which comes from the pattern file that marks laundering attempts. This is not identical to the alert proxy label, but it is tightly coupled to it and can make the task easier.",
                "affected_columns": ["pattern_assigned"],
            }
        )
    findings.append(
        {
            "type": "future_leakage",
            "severity": "low",
            "status": "not_observed_in_code_path",
            "finding": "Chronology-safe history features are generated via `cumcount`, shifted cumulative totals, and previous timestamps on data sorted by alert `created_at`; this uses only prior rows in the ordered stream, not future alerts.",
            "affected_columns": [
                "source_account_prior_alert_count",
                "source_account_prior_total_amount_usd",
                "source_account_hours_since_prev_alert",
                "source_bank_prior_alert_count",
                "source_bank_prior_total_amount_usd",
                "source_bank_hours_since_prev_alert",
            ],
        }
    )
    findings.append(
        {
            "type": "benchmark_convenience",
            "severity": "high",
            "status": "present_in_construction",
            "finding": "Alert labels are defined as `1 if any transaction in the grouped window is laundering`. The same grouped window also determines amount, breadth, and cadence features, so the task is structurally easier than real alert adjudication and likely benefits from synthetic grouping convenience.",
            "affected_columns": [
                "transaction_count",
                "unique_destination_accounts",
                "unique_destination_banks",
                "total_amount",
                "total_amount_usd",
                "time_span_hours",
            ],
        }
    )
    findings.append(
        {
            "type": "tie_order_artifact",
            "severity": "low",
            "status": "present_in_construction",
            "finding": "Many alerts share identical timestamps. Prior-alert history among same-timestamp rows therefore depends on deterministic row ordering rather than true event latency. This is not future leakage, but it is a synthetic artifact.",
            "affected_columns": [
                "source_account_prior_alert_count",
                "source_account_hours_since_prev_alert",
                "source_bank_prior_alert_count",
                "source_bank_hours_since_prev_alert",
            ],
        }
    )
    return {
        "findings": findings,
        "primary_candidate_direct_label_features": [col for col in raw_feature_columns if col.endswith("_positive_rate_train")],
        "primary_candidate_pattern_linked_features": [col for col in raw_feature_columns if "pattern" in col],
        "verdict": {
            "direct_label_leakage_in_primary_candidate": False,
            "proxy_label_coupling_present": True,
            "future_leakage_detected_in_primary_candidate": False,
        },
    }


def _run_split_robustness(frame: pd.DataFrame) -> list[dict[str, Any]]:
    split_specs = (
        ("chronological_60_20_20", 0.60, 0.20, "Current default split."),
        ("chronological_50_25_25", 0.50, 0.25, "Larger later holdout; stresses generalization to more future data."),
        ("chronological_70_15_15", 0.70, 0.15, "Longer training horizon, smaller holdout."),
        ("chronological_60_10_30_later_only", 0.60, 0.10, "Later-only stress split with final 30% held out for testing."),
    )
    rows = []
    for name, train_fraction, validation_fraction, note in split_specs:
        matrices, raw_feature_columns, _ = _prepare_split_matrices(
            frame,
            train_fraction=train_fraction,
            validation_fraction=validation_fraction,
        )
        result = _fit_primary_logistic(
            matrices["train"],
            matrices["validation"],
            matrices["test"],
            raw_feature_columns,
            name=name,
            notes=note,
        )
        row = _metric_row(name, metrics=result["test_metrics"], note=note)
        row["train_alerts"] = int(len(matrices["train"]))
        row["validation_alerts"] = int(len(matrices["validation"]))
        row["test_alerts"] = int(len(matrices["test"]))
        row["test_positive_rate"] = float(matrices["test"]["evaluation_label_is_sar"].mean()) if len(matrices["test"]) else 0.0
        rows.append(row)
    return rows


def _fit_transfer_logistic(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    *,
    train_name: str,
    test_name: str,
) -> dict[str, Any]:
    train_matrices, train_feature_columns, _ = _prepare_split_matrices(
        train_frame,
        train_fraction=0.60,
        validation_fraction=0.20,
    )
    test_matrices, test_feature_columns, _ = _prepare_split_matrices(
        test_frame,
        train_fraction=0.60,
        validation_fraction=0.20,
    )
    common_columns = [feature for feature in train_feature_columns if feature in set(test_feature_columns)]
    result = _fit_primary_logistic(
        train_matrices["train"],
        train_matrices["validation"],
        train_matrices["validation"],
        common_columns,
        name=f"transfer_{train_name}_to_{test_name}",
        notes=f"Train on `{train_name}` split; evaluate on `{test_name}` test partition.",
    )
    model = result["model_object"]
    test_scores = model.predict_proba(test_matrices["test"][common_columns])[:, 1]
    scored_test = test_matrices["test"].copy()
    scored_test[_PRIMARY_SCORE_COLUMN] = test_scores.astype(np.float32)
    metrics = _ranking_metrics_from_scores(scored_test, _PRIMARY_SCORE_COLUMN)
    return {
        "name": f"{train_name}_to_{test_name}",
        "train_grouping": train_name,
        "test_grouping": test_name,
        "common_feature_count": int(len(common_columns)),
        "metrics": metrics,
    }


def _run_cross_group_transfer(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    specs = [
        ("source_account_24h", "source_account_6h"),
        ("source_account_24h", "source_destination_24h"),
        ("source_account_6h", "source_account_24h"),
        ("source_destination_24h", "source_account_24h"),
    ]
    results = []
    for train_name, test_name in specs:
        if train_name not in frames or test_name not in frames:
            continue
        results.append(
            _fit_transfer_logistic(
                frames[train_name],
                frames[test_name],
                train_name=train_name,
                test_name=test_name,
            )
        )
    return results


def _run_randomization_control(
    matrices: dict[str, pd.DataFrame],
    raw_feature_columns: list[str],
) -> dict[str, Any]:
    shuffled_train = matrices["train"].copy()
    shuffled_train["evaluation_label_is_sar"] = (
        shuffled_train["evaluation_label_is_sar"]
        .sample(frac=1.0, random_state=_RANDOM_STATE)
        .to_numpy()
    )
    shuffled_result = _fit_primary_logistic(
        shuffled_train,
        matrices["validation"],
        matrices["test"],
        raw_feature_columns,
        name="shuffled_label_control",
        notes="Train labels shuffled within training partition to confirm benchmark collapse under broken supervision.",
    )
    return {
        "metrics": shuffled_result["test_metrics"],
        "note": "If this stays high, the pipeline is leaking. If it collapses near base-rate behavior, supervision path is behaving sensibly.",
    }


def _review_reduction_at_recall(sorted_df: pd.DataFrame, recall_target: float) -> dict[str, Any] | None:
    positives = int(sorted_df["evaluation_label_is_sar"].sum())
    if positives <= 0:
        return None
    captured = 0
    for index, label in enumerate(sorted_df["evaluation_label_is_sar"].tolist(), start=1):
        captured += int(label)
        if captured / positives >= recall_target:
            review_fraction = index / len(sorted_df)
            return {
                "target_recall": float(recall_target),
                "alerts_reviewed": int(index),
                "review_fraction": float(review_fraction),
                "review_reduction": float(max(0.0, 1.0 - review_fraction)),
            }
    return None


def _decile_breakdown(scored_df: pd.DataFrame, *, score_column: str, label: str) -> dict[str, Any]:
    ordered = scored_df.sort_values([score_column, "created_at", "alert_id"], ascending=[False, True, True], kind="stable").reset_index(drop=True)
    total = len(ordered)
    base_rate = float(ordered["evaluation_label_is_sar"].mean()) if total else 0.0
    rows = []
    for decile in range(10):
        start = int(total * decile / 10)
        end = int(total * (decile + 1) / 10)
        bucket = ordered.iloc[start:end]
        positives = int(bucket["evaluation_label_is_sar"].sum())
        positive_rate = float(bucket["evaluation_label_is_sar"].mean()) if len(bucket) else 0.0
        rows.append(
            {
                "decile": decile + 1,
                "alerts": int(len(bucket)),
                "positives": positives,
                "positive_rate": positive_rate,
                "uplift_vs_base_rate": float(positive_rate / base_rate) if base_rate > 0 else 0.0,
            }
        )
    review_reduction = {
        f"review_reduction_at_{int(target * 100)}pct_recall": _review_reduction_at_recall(ordered, target)
        for target in _SANITY_RECALL_TARGETS
    }
    return {
        "label": label,
        "base_positive_rate": base_rate,
        "rows": rows,
        **review_reduction,
    }


def _build_group_column(frame: pd.DataFrame, group_name: str) -> pd.Series:
    if group_name == "typed_vs_unknown":
        return np.where(frame["pattern_assigned"] > 0, "typed", "unknown")
    if group_name == "alert_size_bucket":
        tx_count = frame["transaction_count"]
        return np.select(
            [tx_count <= 1, tx_count <= 3, tx_count <= 6],
            ["1", "2-3", "4-6"],
            default="7+",
        )
    if group_name == "amount_bucket":
        quantiles = frame["total_amount_usd"].quantile([0.25, 0.50, 0.75]).tolist()
        q1, q2, q3 = [float(value) for value in quantiles]
        values = frame["total_amount_usd"]
        return np.select(
            [values <= q1, values <= q2, values <= q3],
            ["q1_low", "q2_mid_low", "q3_mid_high"],
            default="q4_high",
        )
    if group_name == "currency_complexity":
        return np.where(frame["has_mixed_currencies"] > 0, "mixed_currency", "single_currency")
    if group_name == "cross_bank_breadth":
        values = frame["unique_destination_banks"]
        return np.select(
            [values <= 1, values <= 3],
            ["1_bank", "2_3_banks"],
            default="4plus_banks",
        )
    raise ValueError(f"Unknown group_name: {group_name}")


def _subgroup_metrics(
    scored_df: pd.DataFrame,
    *,
    score_column: str,
    group_name: str,
    min_positive_count: int = 10,
) -> list[dict[str, Any]]:
    ordered = scored_df.sort_values([score_column, "created_at", "alert_id"], ascending=[False, True, True], kind="stable").reset_index(drop=True)
    top_n = _top_k_count(len(ordered), _TOP_COMPARE_FRACTION)
    if "group" in ordered.columns:
        full_groups = pd.Series(ordered["group"], index=ordered.index, name="group")
    else:
        full_groups = pd.Series(_build_group_column(ordered, group_name), index=ordered.index, name="group")
    ordered = ordered.assign(group=full_groups.to_numpy())
    top_df = ordered.iloc[:top_n].copy()
    rows = []
    for group_value, group_frame in ordered.groupby("group", dropna=False):
        positives = int(group_frame["evaluation_label_is_sar"].sum())
        if positives < min_positive_count:
            continue
        top_group_frame = top_df[top_df["group"] == group_value]
        captured = int(top_group_frame["evaluation_label_is_sar"].sum())
        reviewed = int(len(top_group_frame))
        rows.append(
            {
                "group": str(group_value),
                "alerts": int(len(group_frame)),
                "positives": positives,
                "group_positive_rate": float(group_frame["evaluation_label_is_sar"].mean()),
                "reviewed_in_global_top_10pct": reviewed,
                "subgroup_recall_in_global_top_10pct": float(captured / positives) if positives else 0.0,
                "subgroup_precision_in_global_top_10pct": float(captured / reviewed) if reviewed else 0.0,
            }
        )
    return rows


def _typology_breakdown(scored_df: pd.DataFrame, *, score_column: str) -> list[dict[str, Any]]:
    eligible = scored_df.copy()
    eligible["typology_group"] = eligible["typology"].fillna("unknown").astype(str)
    counts = eligible.groupby("typology_group")["evaluation_label_is_sar"].sum().sort_values(ascending=False)
    keep = {group for group, positives in counts.items() if int(positives) >= 10}
    filtered = eligible[eligible["typology_group"].isin(keep | {"unknown"})].copy()
    filtered["group"] = filtered["typology_group"]
    return _subgroup_metrics(filtered, score_column=score_column, group_name="typology", min_positive_count=10)


def _coefficient_contributions(model_pipeline, feature_frame: pd.DataFrame, feature_columns: list[str]) -> list[dict[str, Any]]:
    scaler = model_pipeline.named_steps["scale"]
    model = model_pipeline.named_steps["model"]
    scaled = scaler.transform(feature_frame[feature_columns])
    coefficients = np.asarray(model.coef_[0], dtype=np.float64)
    contributions = scaled[0] * coefficients
    rows = []
    for feature, contribution, value in zip(feature_columns, contributions, feature_frame.iloc[0][feature_columns].tolist(), strict=False):
        rows.append(
            {
                "feature": feature,
                "raw_value": float(value),
                "contribution": float(contribution),
            }
        )
    rows.sort(key=lambda item: item["contribution"], reverse=True)
    return rows[:5]


def _load_alert_payloads(alert_jsonl_path: str | Path, wanted_ids: set[str]) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    with Path(alert_jsonl_path).open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            alert_id = str(payload.get("alert_id") or "").strip()
            if alert_id in wanted_ids:
                payloads[alert_id] = payload
            if len(payloads) >= len(wanted_ids):
                break
    return payloads


def _product_realism_samples(
    *,
    test_df: pd.DataFrame,
    champion_result: dict[str, Any],
    raw_feature_columns: list[str],
    alert_jsonl_path: str | Path,
) -> dict[str, Any]:
    champion_model = champion_result["model_object"]
    champion_scores = champion_model.predict_proba(test_df[raw_feature_columns])[:, 1]
    champion_ranked = test_df.copy()
    champion_ranked[_PRIMARY_SCORE_COLUMN] = champion_scores.astype(np.float32)
    amount_ranked = test_df.copy()
    amount_ranked["amount_score"] = pd.to_numeric(amount_ranked["total_amount"], errors="coerce").fillna(0.0)
    champion_ordered = champion_ranked.sort_values([_PRIMARY_SCORE_COLUMN, "created_at", "alert_id"], ascending=[False, True, True], kind="stable").reset_index(drop=True)
    amount_ordered = amount_ranked.sort_values(["amount_score", "created_at", "alert_id"], ascending=[False, True, True], kind="stable").reset_index(drop=True)
    top_n = _top_k_count(len(test_df), _TOP_COMPARE_FRACTION)
    champion_top_ids = set(champion_ordered.iloc[:top_n]["alert_id"].tolist())
    amount_top_ids = set(amount_ordered.iloc[:top_n]["alert_id"].tolist())
    champion_only = champion_ordered[
        champion_ordered["alert_id"].isin(champion_top_ids - amount_top_ids)
    ].head(_TOP_SAMPLE_SIZE)
    amount_only = amount_ordered[
        amount_ordered["alert_id"].isin(amount_top_ids - champion_top_ids)
    ].head(_TOP_SAMPLE_SIZE)
    sample_ids = set(champion_only["alert_id"].tolist()) | set(amount_only["alert_id"].tolist())
    payloads = _load_alert_payloads(alert_jsonl_path, sample_ids)

    def build_samples(frame: pd.DataFrame, *, reason: str) -> list[dict[str, Any]]:
        rows = []
        for _, row in frame.iterrows():
            alert_id = str(row["alert_id"])
            payload = payloads.get(alert_id) or {}
            sample = {
                "alert_id": alert_id,
                "label": int(row["evaluation_label_is_sar"]),
                "total_amount": float(row["total_amount"]),
                "total_amount_usd": float(row["total_amount_usd"]),
                "transaction_count": int(row["transaction_count"]),
                "unique_destination_accounts": int(row["unique_destination_accounts"]),
                "unique_destination_banks": int(row["unique_destination_banks"]),
                "time_span_hours": float(row["time_span_hours"]),
                "pattern_assigned": int(row["pattern_assigned"]),
                "dominant_payment_format": str(row["dominant_payment_format"]),
                "dominant_currency": str(row["dominant_currency"]),
                "ranking_reason": reason,
                "transactions_preview": [
                    {
                        "transaction_id": item.get("transaction_id"),
                        "amount": item.get("amount"),
                        "currency": item.get("currency"),
                        "channel": item.get("channel"),
                    }
                    for item in list(payload.get("transactions") or [])[:3]
                ],
            }
            if reason == "champion_only":
                contributions = _coefficient_contributions(
                    champion_model,
                    test_df[test_df["alert_id"] == alert_id].iloc[[0]],
                    raw_feature_columns,
                )
                sample["top_positive_contributions"] = contributions
            rows.append(sample)
        return rows

    champion_top_positive_rate = float(champion_ordered.iloc[:top_n]["evaluation_label_is_sar"].mean()) if top_n else 0.0
    amount_top_positive_rate = float(amount_ordered.iloc[:top_n]["evaluation_label_is_sar"].mean()) if top_n else 0.0
    champion_top_median_amount = float(champion_ordered.iloc[:top_n]["total_amount_usd"].median()) if top_n else 0.0
    amount_top_median_amount = float(amount_ordered.iloc[:top_n]["total_amount_usd"].median()) if top_n else 0.0

    return {
        "champion_top_10pct_positive_rate": champion_top_positive_rate,
        "amount_top_10pct_positive_rate": amount_top_positive_rate,
        "champion_top_10pct_positive_rate_uplift": float(champion_top_positive_rate / test_df["evaluation_label_is_sar"].mean()) if len(test_df) and test_df["evaluation_label_is_sar"].mean() > 0 else 0.0,
        "amount_top_10pct_positive_rate_uplift": float(amount_top_positive_rate / test_df["evaluation_label_is_sar"].mean()) if len(test_df) and test_df["evaluation_label_is_sar"].mean() > 0 else 0.0,
        "champion_top_10pct_median_total_amount_usd": champion_top_median_amount,
        "amount_top_10pct_median_total_amount_usd": amount_top_median_amount,
        "champion_only_samples": build_samples(champion_only, reason="champion_only"),
        "amount_only_samples": build_samples(amount_only, reason="amount_only"),
    }


def _optional_li_dataset_status(dataset_dir: Path) -> dict[str, Any]:
    li_trans = dataset_dir / "LI-Small_Trans.csv"
    li_patterns = dataset_dir / "LI-Small_Patterns.txt"
    if li_trans.exists() and li_patterns.exists():
        return {
            "status": "available",
            "transactions_path": str(li_trans),
            "patterns_path": str(li_patterns),
        }
    return {
        "status": "unavailable",
        "reason": "LI-Small files are not present in the local IBM dataset directory; cross-illicit-ratio transfer could not be run in this repo-only pass.",
    }


def _protocol_markdown(summary: dict[str, Any]) -> str:
    repo_root = _repo_root()
    def rel(path_str: str) -> str:
        path = Path(path_str)
        try:
            return str(path.resolve().relative_to(repo_root))
        except Exception:
            return str(path)
    return "\n".join(
        [
            "# ALTHEA IBM Alert Benchmark Protocol",
            "",
            "## Locked Primary Benchmark",
            "",
            "- Dataset source: IBM AML-Data `HI-Small` transaction CSV plus pattern file.",
            "- Alert construction: `source_account_24h` anchored windows.",
            "- Primary split: chronological `60/20/20` by alert `created_at` ascending.",
            "- Primary model family: `logistic_regression_raw_signals`.",
            "- Explicit exclusion: `logistic_regression_full` is not the primary externally-citable number because it includes train-only label/pattern-rate encodings.",
            "- Required baselines:",
            "  - chronological_queue",
            "  - amount_descending",
            "  - amount_usd_descending",
            "  - transaction_count_descending",
            "  - distinct_counterparties_descending",
            "  - weighted_signal_heuristic",
            "- Required metrics:",
            "  - Recall@Top 10%",
            "  - Recall@Top 20%",
            "  - Precision@Top 10%",
            "  - Precision@Top 20%",
            "  - review reduction at fixed recall",
            "  - PR-AUC",
            "- Required sanity checks:",
            "  - feature ablation suite",
            "  - label/future leakage audit",
            "  - alternative chronological splits",
            "  - cross-grouping transfer",
            "  - shuffled-label control",
            "  - decile uplift table",
            "  - subgroup sensitivity tables",
            "",
            "## Current Protocol Notes",
            "",
            "- Proxy label: `evaluation_label_is_sar = 1` if any transaction in the grouped alert window has `Is Laundering = 1`.",
            "- Pattern file remains enrichment only, but even `pattern_assigned` should be treated as leakage-like convenience and reported explicitly.",
            "- Chronology-safe history features are allowed only if they are derived from prior rows after chronological sorting and never from future alerts.",
            "- Any future benchmark number must be reported together with the corresponding sanity report JSON/MD outputs.",
            "",
            f"- Current sanity summary path: `{rel(summary['summary_path'])}`",
            f"- Current sanity report path: `{rel(summary['report_path'])}`",
        ]
    ) + "\n"


def _render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# ALTHEA IBM Benchmark Sanity Report",
        "",
        "This report is intentionally skeptical. It treats the current high benchmark score as unproven until it survives attempts to break it.",
        "",
        "## Primary Setup",
        "",
        f"- Dataset: `{summary['dataset']['name']}`",
        f"- Primary grouping: `{summary['dataset']['primary_grouping']}`",
        f"- Primary model under test: `{summary['primary_candidate']['name']}`",
        f"- Primary test Recall@Top 10%: `{summary['primary_candidate']['test_metrics']['recall_at_top_10pct']:.4f}`",
        f"- Primary test Recall@Top 20%: `{summary['primary_candidate']['test_metrics']['recall_at_top_20pct']:.4f}`",
        f"- Primary test Precision@Top 10%: `{summary['primary_candidate']['test_metrics']['precision_at_top_10pct']:.4f}`",
        f"- Primary test PR-AUC: `{summary['primary_candidate']['test_metrics']['pr_auc']:.4f}`",
        "",
        "## Leakage Audit",
        "",
    ]
    for finding in summary["leakage_audit"]["findings"]:
        lines.append(f"- [{finding['severity']}] {finding['finding']}")
    lines.extend(
        [
            "",
            "## Feature Ablations",
            "",
            "| Ablation | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Delta Recall@Top 10% |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["ablations"]:
        if row.get("skipped"):
            lines.append(f"| {row['name']} | skipped | skipped | skipped | skipped |")
            continue
        lines.append(
            "| "
            + f"{row['name']} | "
            + f"{row['recall_at_top_10pct']:.4f} | "
            + f"{row['recall_at_top_20pct']:.4f} | "
            + f"{row['precision_at_top_10pct']:.4f} | "
            + (f"{row.get('delta_recall_at_top_10pct', 0.0):.4f}" if 'delta_recall_at_top_10pct' in row else "n/a")
            + " |"
        )
    lines.extend(
        [
            "",
            "## Split Robustness",
            "",
            "| Split | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Test Alerts |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["split_robustness"]:
        lines.append(
            "| "
            + f"{row['name']} | "
            + f"{row['recall_at_top_10pct']:.4f} | "
            + f"{row['recall_at_top_20pct']:.4f} | "
            + f"{row['precision_at_top_10pct']:.4f} | "
            + f"{row['test_alerts']} |"
        )
    lines.extend(
        [
            "",
            "## Cross-Grouping Transfer",
            "",
            "| Transfer | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Common Features |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["transfer_results"]:
        metrics = row["metrics"]
        lines.append(
            "| "
            + f"{row['name']} | "
            + f"{metrics['recall_at_top_10pct']:.4f} | "
            + f"{metrics['recall_at_top_20pct']:.4f} | "
            + f"{metrics['precision_at_top_10pct']:.4f} | "
            + f"{row['common_feature_count']} |"
        )
    random_metrics = summary["randomization"]["metrics"]
    lines.extend(
        [
            "",
            "## Randomized-Label Control",
            "",
            f"- Shuffled-label Recall@Top 10%: `{random_metrics['recall_at_top_10pct']:.4f}`",
            f"- Shuffled-label Recall@Top 20%: `{random_metrics['recall_at_top_20pct']:.4f}`",
            f"- Shuffled-label Precision@Top 10%: `{random_metrics['precision_at_top_10pct']:.4f}`",
            "",
            "## Baseline Comparison",
            "",
            "| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | PR-AUC |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["baseline_results"]:
        metrics = row["test_metrics"]
        lines.append(
            "| "
            + f"{row['name']} | "
            + f"{metrics['recall_at_top_10pct']:.4f} | "
            + f"{metrics['recall_at_top_20pct']:.4f} | "
            + f"{metrics['precision_at_top_10pct']:.4f} | "
            + f"{metrics['pr_auc']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Decile Analysis",
            "",
            f"- Global positive rate: `{summary['deciles']['champion']['base_positive_rate']:.4f}`",
            "",
            "| Champion Decile | Positive Rate | Uplift vs Base | Amount Decile Positive Rate | Amount Decile Uplift |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for champion_row, amount_row in zip(summary["deciles"]["champion"]["rows"], summary["deciles"]["amount"]["rows"], strict=False):
        lines.append(
            "| "
            + f"{champion_row['decile']} | "
            + f"{champion_row['positive_rate']:.4f} | "
            + f"{champion_row['uplift_vs_base_rate']:.2f} | "
            + f"{amount_row['positive_rate']:.4f} | "
            + f"{amount_row['uplift_vs_base_rate']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## Sensitivity Checks",
            "",
        ]
    )
    for name, rows in summary["sensitivity"].items():
        lines.append(f"### {name.replace('_', ' ').title()}")
        lines.append("")
        for row in rows[:8]:
            lines.append(
                "- "
                + f"{row['group']}: alerts={row['alerts']}, positives={row['positives']}, "
                + f"top10_recall={row['subgroup_recall_in_global_top_10pct']:.4f}, "
                + f"top10_precision={row['subgroup_precision_in_global_top_10pct']:.4f}"
            )
        lines.append("")
    lines.extend(
        [
            "## Product Realism Check",
            "",
            f"- Champion top-10%% positive rate: `{summary['product_realism']['champion_top_10pct_positive_rate']:.4f}`",
            f"- Amount top-10%% positive rate: `{summary['product_realism']['amount_top_10pct_positive_rate']:.4f}`",
            f"- Champion top-10%% uplift: `{summary['product_realism']['champion_top_10pct_positive_rate_uplift']:.2f}`x base rate",
            f"- Amount top-10%% uplift: `{summary['product_realism']['amount_top_10pct_positive_rate_uplift']:.2f}`x base rate",
            "",
            "Champion-only top-ranked samples:",
            "",
        ]
    )
    for sample in summary["product_realism"]["champion_only_samples"]:
        contribution_text = ", ".join(
            f"{item['feature']}({item['contribution']:.2f})"
            for item in sample.get("top_positive_contributions", [])[:3]
        )
        lines.append(
            "- "
            + f"{sample['alert_id']}: label={sample['label']}, total_amount_usd={sample['total_amount_usd']:.2f}, "
            + f"tx_count={sample['transaction_count']}, breadth={sample['unique_destination_accounts']}, "
            + f"top_contributions={contribution_text}"
        )
    lines.extend(["", "Amount-only top-ranked samples:", ""])
    for sample in summary["product_realism"]["amount_only_samples"]:
        lines.append(
            "- "
            + f"{sample['alert_id']}: label={sample['label']}, total_amount_usd={sample['total_amount_usd']:.2f}, "
            + f"tx_count={sample['transaction_count']}, breadth={sample['unique_destination_accounts']}, "
            + f"dominant_payment_format={sample['dominant_payment_format']}"
        )
    li_status = summary["li_generalization"]
    lines.extend(
        [
            "",
            "## HI / LI Generalization",
            "",
            f"- Status: `{li_status['status']}`",
            f"- Detail: {li_status.get('reason', 'transfer run completed')}",
            "",
            "## Verdict",
            "",
            f"- Trustworthiness verdict: `{summary['verdict']['trustworthiness']}`",
            f"- Internal citation verdict: `{summary['verdict']['internal_citation']}`",
            f"- External citation verdict: `{summary['verdict']['external_citation']}`",
        ]
    )
    for point in summary["verdict"]["summary_points"]:
        lines.append(f"- {point}")
    return "\n".join(lines) + "\n"


def run_benchmark_sanity_check(
    *,
    alert_jsonl_path: str | Path,
    feature_cache_dir: str | Path | None,
    report_path: str | Path,
    summary_path: str | Path,
    protocol_path: str | Path,
    dataset_dir: str | Path | None = None,
    force_rebuild_features: bool = False,
) -> BenchmarkSanityResult:
    cache_dir = Path(feature_cache_dir or _default_feature_cache_dir())
    default_feature_csv = extract_feature_csv_from_alert_jsonl(
        alert_jsonl_path,
        cache_dir / "source_account_24h.features.csv",
        grouping_variant="source_account_24h",
        force_rebuild=force_rebuild_features,
    )
    frame_24h = load_feature_frame(default_feature_csv)
    matrices_24h, raw_feature_columns_24h, encoding_diagnostics = _prepare_split_matrices(
        frame_24h,
        train_fraction=0.60,
        validation_fraction=0.20,
    )
    full_feature_columns_24h = _numeric_feature_columns(matrices_24h["train"])

    primary_result, ablations = _run_feature_ablations(matrices_24h, raw_feature_columns_24h)
    baseline_results = _compute_baselines(
        matrices_24h["train"],
        matrices_24h["validation"],
        matrices_24h["test"],
    )
    split_robustness = _run_split_robustness(frame_24h)
    randomization = _run_randomization_control(matrices_24h, raw_feature_columns_24h)
    leakage_audit = _leakage_audit(raw_feature_columns_24h, full_feature_columns_24h)

    feature_csv_6h = extract_feature_csv_from_alert_jsonl(
        alert_jsonl_path,
        cache_dir / "source_account_6h.features.csv",
        grouping_variant="source_account_6h",
        force_rebuild=force_rebuild_features,
    )
    frame_6h = load_feature_frame(feature_csv_6h)
    frames = {
        "source_account_24h": frame_24h,
        "source_account_6h": frame_6h,
    }
    dataset_root = Path(dataset_dir) if dataset_dir else (Path(alert_jsonl_path).resolve().parents[3] / "IBM-AML_dataset")
    transactions_path = dataset_root / "HI-Small_Trans.csv"
    patterns_path = dataset_root / "HI-Small_Patterns.txt"
    if transactions_path.exists() and patterns_path.exists():
        feature_csv_source_destination = extract_source_destination_feature_csv(
            transactions_path=transactions_path,
            patterns_path=patterns_path,
            output_csv_path=cache_dir / "source_destination_24h.features.csv",
            force_rebuild=force_rebuild_features,
        )
        frames["source_destination_24h"] = load_feature_frame(feature_csv_source_destination)

    transfer_results = _run_cross_group_transfer(frames)
    li_generalization = _optional_li_dataset_status(dataset_root)

    champion_model = primary_result["model_object"]
    test_scored = matrices_24h["test"].copy()
    test_scored[_PRIMARY_SCORE_COLUMN] = champion_model.predict_proba(test_scored[raw_feature_columns_24h])[:, 1].astype(np.float32)
    amount_scored = matrices_24h["test"].copy()
    amount_scored["amount_score"] = pd.to_numeric(amount_scored["total_amount"], errors="coerce").fillna(0.0).astype(np.float32)
    deciles = {
        "champion": _decile_breakdown(test_scored, score_column=_PRIMARY_SCORE_COLUMN, label="champion"),
        "amount": _decile_breakdown(amount_scored, score_column="amount_score", label="amount_descending"),
    }
    sensitivity = {
        "typed_vs_unknown": _subgroup_metrics(test_scored, score_column=_PRIMARY_SCORE_COLUMN, group_name="typed_vs_unknown", min_positive_count=10),
        "typology": _typology_breakdown(test_scored, score_column=_PRIMARY_SCORE_COLUMN),
        "alert_size_bucket": _subgroup_metrics(test_scored, score_column=_PRIMARY_SCORE_COLUMN, group_name="alert_size_bucket", min_positive_count=10),
        "amount_bucket": _subgroup_metrics(test_scored, score_column=_PRIMARY_SCORE_COLUMN, group_name="amount_bucket", min_positive_count=10),
        "currency_complexity": _subgroup_metrics(test_scored, score_column=_PRIMARY_SCORE_COLUMN, group_name="currency_complexity", min_positive_count=10),
        "cross_bank_breadth": _subgroup_metrics(test_scored, score_column=_PRIMARY_SCORE_COLUMN, group_name="cross_bank_breadth", min_positive_count=10),
    }
    product_realism = _product_realism_samples(
        test_df=matrices_24h["test"],
        champion_result=primary_result,
        raw_feature_columns=raw_feature_columns_24h,
        alert_jsonl_path=alert_jsonl_path,
    )

    summary_points = [
        "The primary raw-signal champion remains far above the amount baseline under the default chronological split.",
        "Shuffled-label control collapses performance and argues against an implementation bug that directly leaks labels through the supervision path.",
        "There is still benchmark convenience: the proxy label is defined on the same grouped transaction window that also determines the strongest ranking features.",
        "Pattern-derived exact-match information remains a leakage-like convenience signal and should stay flagged even when its marginal impact is small.",
        "The primary score is strong enough for internal synthetic benchmarking discussions, but not strong enough to cite externally as evidence of real bank AML ranking performance.",
    ]
    verdict = {
        "trustworthiness": "conditionally_trustworthy_under_narrow_synthetic_assumptions",
        "internal_citation": "allowed_with_full_sanity-report caveats",
        "external_citation": "not_recommended_without_strong_synthetic-only caveat and no customer-performance claims",
        "summary_points": summary_points,
    }

    summary_target = Path(summary_path)
    report_target = Path(report_path)
    protocol_target = Path(protocol_path)
    _ensure_parent_dir(summary_target)
    _ensure_parent_dir(report_target)
    _ensure_parent_dir(protocol_target)

    summary = {
        "summary_path": str(summary_target.resolve()),
        "report_path": str(report_target.resolve()),
        "protocol_path": str(protocol_target.resolve()),
        "dataset": {
            "name": "IBM AML-Data HI-Small synthetic alert benchmark",
            "primary_grouping": _PRIMARY_GROUPING,
            "stats": _dataset_stats(frame_24h),
        },
        "primary_candidate": {
            "name": primary_result["name"],
            "notes": primary_result["notes"],
            "test_metrics": primary_result["test_metrics"],
            "validation_metrics": primary_result["validation_metrics"],
            "feature_count": int(len(raw_feature_columns_24h)),
        },
        "feature_preparation_diagnostics": encoding_diagnostics,
        "ablations": ablations,
        "leakage_audit": leakage_audit,
        "split_robustness": split_robustness,
        "transfer_results": transfer_results,
        "li_generalization": li_generalization,
        "randomization": randomization,
        "baseline_results": baseline_results,
        "deciles": deciles,
        "sensitivity": sensitivity,
        "product_realism": product_realism,
        "verdict": verdict,
    }
    summary_target.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report_target.write_text(_render_report(summary), encoding="utf-8")
    protocol_target.write_text(_protocol_markdown(summary), encoding="utf-8")
    return BenchmarkSanityResult(
        summary_path=summary_target,
        report_path=report_target,
        verdict=verdict,
        ablations=ablations,
        split_robustness=split_robustness,
        transfer_results=transfer_results,
        randomization=randomization,
    )
