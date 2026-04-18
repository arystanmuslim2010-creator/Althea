from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from benchmarks.ibm_aml_data import convert_transactions_to_alert_jsonl
from benchmarks.ibm_aml_improvement import (
    _apply_train_only_encodings,
    _compute_baselines,
    _dataset_stats,
    _fit_candidate_by_name,
    _numeric_feature_columns,
    _split_frame,
    _add_derived_features,
    _add_label_free_history_features,
    extract_feature_csv_from_alert_jsonl,
    load_feature_frame,
)

logger = logging.getLogger("althea.benchmarks.ibm_aml_li_transfer")

_EXPECTED_TRANSACTION_HEADER = [
    "timestamp",
    "from_bank",
    "account",
    "to_bank",
    "account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
]
_DEFAULT_DATASET_NAME = "IBM AML-Data LI-Small"
_DEFAULT_GROUPING_VARIANT = "source_account_24h"
_TRANSFER_KIND = "transfer"


@dataclass(slots=True)
class LiTransferBenchmarkResult:
    summary_path: Path
    report_path: Path
    li_alert_path: Path
    li_feature_path: Path
    baseline_results: list[dict[str, Any]]
    hi_transfer_result: dict[str, Any]
    li_native_result: dict[str, Any] | None
    reused_artifacts: dict[str, Any]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _display_path(path: str | Path) -> str:
    candidate = Path(path)
    try:
        return str(candidate.resolve().relative_to(_repo_root()))
    except Exception:
        return str(candidate)


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_header_token(token: str) -> str:
    return str(token or "").strip().lower().replace("-", "_").replace(" ", "_").replace("\ufeff", "")


def _validate_transaction_schema(transactions_path: str | Path) -> dict[str, Any]:
    path = Path(transactions_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None) or []
    normalized = [_normalize_header_token(item) for item in header[: len(_EXPECTED_TRANSACTION_HEADER)]]
    return {
        "path": str(path.resolve()),
        "normalized_header": normalized,
        "matches_hi_parser": normalized == _EXPECTED_TRANSACTION_HEADER,
        "expected_header": list(_EXPECTED_TRANSACTION_HEADER),
    }


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _prepare_frame(frame):
    return _add_derived_features(_add_label_free_history_features(frame))


def _reconstruct_hi_champion_transfer(
    *,
    champion_candidate_name: str,
    hi_frame,
    li_frame,
) -> dict[str, Any]:
    hi_prepared = _prepare_frame(hi_frame)
    li_prepared = _prepare_frame(li_frame)
    hi_splits = _split_frame(hi_prepared)
    li_splits = _split_frame(li_prepared)
    hi_train_encoded, hi_validation_encoded, li_test_transfer, _ = _apply_train_only_encodings(
        hi_splits["train"],
        hi_splits["validation"],
        li_splits["test"],
    )
    feature_columns = _numeric_feature_columns(hi_train_encoded)
    result = _fit_candidate_by_name(
        champion_candidate_name,
        hi_train_encoded,
        hi_validation_encoded,
        li_test_transfer,
        feature_columns,
    )
    result["kind"] = _TRANSFER_KIND
    result["notes"] = (
        "HI-trained champion reconstructed from cached HI feature matrix and evaluated on the LI test split "
        "without LI retraining."
    )
    return result


def _comparison_rows(
    *,
    baseline_results: list[dict[str, Any]],
    hi_transfer_result: dict[str, Any],
    li_native_result: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in baseline_results:
        if row["name"] not in {"chronological_queue", "amount_descending", "weighted_signal_heuristic"}:
            continue
        rows.append(
            {
                "name": row["name"],
                "kind": row["kind"],
                "test_metrics": row["test_metrics"],
                "notes": row.get("notes"),
            }
        )
    rows.append(
        {
            "name": f"hi_trained_{hi_transfer_result['name']}_on_li",
            "kind": _TRANSFER_KIND,
            "test_metrics": hi_transfer_result["test_metrics"],
            "notes": hi_transfer_result.get("notes"),
        }
    )
    if li_native_result:
        rows.append(
            {
                "name": f"li_native_{li_native_result['name']}",
                "kind": li_native_result["kind"],
                "test_metrics": li_native_result["test_metrics"],
                "notes": li_native_result.get("notes"),
            }
        )
    return rows


def _build_feature_schema_match(hi_frame, li_frame) -> dict[str, Any]:
    hi_columns = list(hi_frame.columns)
    li_columns = list(li_frame.columns)
    hi_only = sorted(set(hi_columns) - set(li_columns))
    li_only = sorted(set(li_columns) - set(hi_columns))
    return {
        "exact_match": not hi_only and not li_only,
        "hi_column_count": int(len(hi_columns)),
        "li_column_count": int(len(li_columns)),
        "hi_only_columns": hi_only,
        "li_only_columns": li_only,
    }


def _serializable_result(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    copy = dict(row)
    copy.pop("model_object", None)
    return copy


def _render_report(summary: dict[str, Any]) -> str:
    rows = summary["comparison_rows"]
    li_transfer = summary["hi_transfer_result"]["test_metrics"]
    amount_row = next((row for row in rows if row["name"] == "amount_descending"), None)
    amount_recall = (amount_row or {}).get("test_metrics", {}).get("recall_at_top_10pct", 0.0)
    generalized = li_transfer.get("recall_at_top_10pct", 0.0) >= amount_recall
    lines = [
        "# ALTHEA IBM LI-Small Transfer Benchmark",
        "",
        "This report adds only the next LI-Small transfer result. It reuses existing HI benchmark artifacts and does not rerun the full HI benchmark or the prior sanity suite.",
        "",
        "## Artifact Reuse",
        "",
        f"- Reused HI benchmark summary: `{_display_path(summary['reused_artifacts']['hi_summary_path'])}`",
        f"- Reused HI feature cache: `{_display_path(summary['reused_artifacts']['hi_feature_path'])}`",
        f"- Reused LI alert JSONL: `{summary['reused_artifacts']['li_alerts_reused']}`",
        f"- Reused LI feature CSV: `{summary['reused_artifacts']['li_features_reused']}`",
        "",
        "## LI Dataset",
        "",
        f"- Alert grouping: `{summary['grouping_variant']}`",
        f"- LI alerts: `{summary['li_dataset_stats']['total_alerts']}`",
        f"- LI positive alerts: `{summary['li_dataset_stats']['positive_alerts']}`",
        f"- LI negative alerts: `{summary['li_dataset_stats']['negative_alerts']}`",
        f"- LI positive rate: `{summary['li_dataset_stats']['positive_rate']:.4f}`",
        f"- LI average transactions per alert: `{summary['li_dataset_stats']['average_transactions_per_alert']:.2f}`",
        "",
        "## Schema Compatibility",
        "",
        f"- LI transaction header matches HI parser: `{summary['transaction_schema']['matches_hi_parser']}`",
        f"- LI feature columns exactly match HI feature columns: `{summary['feature_schema_match']['exact_match']}`",
        f"- LI accounts CSV present but unused by the current protocol: `{summary['accounts_file_present_but_unused']}`",
        "",
        "## LI Benchmark Comparison",
        "",
        "| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        metrics = row["test_metrics"]
        review = metrics.get("review_reduction_at_80pct_recall") or {}
        lines.append(
            "| "
            + f"{row['name']} | {row['kind']} | "
            + f"{metrics.get('recall_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('recall_at_top_20pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_20pct', 0.0):.4f} | "
            + f"{review.get('review_reduction', 0.0):.4f} | "
            + f"{metrics.get('pr_auc', 0.0):.4f} |"
        )
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- HI-trained champion recall@top10 on LI: `{li_transfer.get('recall_at_top_10pct', 0.0):.4f}`",
            f"- Amount baseline recall@top10 on LI: `{amount_recall:.4f}`",
            f"- Transfer verdict vs amount baseline: `{'generalized_above_amount_baseline' if generalized else 'collapsed_below_amount_baseline'}`",
            "- External-claim verdict: synthetic benchmark evidence only. This still is not sufficient for customer-facing or investor-facing performance claims without stronger out-of-distribution validation.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_li_transfer_benchmark(
    *,
    li_transactions_path: str | Path,
    li_patterns_path: str | Path,
    li_accounts_path: str | Path | None,
    report_path: str | Path,
    summary_path: str | Path,
    hi_summary_path: str | Path,
    hi_feature_path: str | Path,
    li_alert_path: str | Path,
    li_feature_path: str | Path,
    grouping_variant: str = _DEFAULT_GROUPING_VARIANT,
    include_li_native_model: bool = True,
    force_rebuild_alerts: bool = False,
    force_rebuild_features: bool = False,
) -> LiTransferBenchmarkResult:
    if grouping_variant != _DEFAULT_GROUPING_VARIANT:
        raise ValueError(f"Unsupported LI transfer grouping variant: {grouping_variant}")

    report_target = Path(report_path)
    summary_target = Path(summary_path)
    li_alert_target = Path(li_alert_path)
    li_feature_target = Path(li_feature_path)
    _ensure_parent_dir(report_target)
    _ensure_parent_dir(summary_target)
    _ensure_parent_dir(li_alert_target)
    _ensure_parent_dir(li_feature_target)

    transaction_schema = _validate_transaction_schema(li_transactions_path)
    if not transaction_schema["matches_hi_parser"]:
        raise ValueError("LI-Small transaction schema does not match the existing IBM AML parser header.")

    li_alerts_reused = li_alert_target.exists() and not force_rebuild_alerts
    if not li_alerts_reused:
        convert_transactions_to_alert_jsonl(
            transactions_path=li_transactions_path,
            patterns_path=li_patterns_path,
            output_path=li_alert_target,
            dataset_name=_DEFAULT_DATASET_NAME,
            write_summary_path=li_alert_target.with_suffix(".summary.json"),
        )

    li_features_reused = li_feature_target.exists() and not force_rebuild_features
    if not li_features_reused:
        extract_feature_csv_from_alert_jsonl(
            li_alert_target,
            li_feature_target,
            grouping_variant=grouping_variant,
            force_rebuild=True,
        )

    hi_summary = _load_json(hi_summary_path)
    champion_candidate_name = (
        ((hi_summary.get("champion") or {}).get("name"))
        or "logistic_regression_raw_signals"
    )
    hi_frame = load_feature_frame(hi_feature_path)
    li_frame = load_feature_frame(li_feature_target)
    li_matrices, li_feature_columns = _prepare_li_native_matrices(li_frame)
    li_dataset_stats = _dataset_stats(li_frame)
    li_split_stats = {name: _dataset_stats(frame) for name, frame in li_matrices.items()}
    baseline_results = _compute_baselines(li_matrices["train"], li_matrices["validation"], li_matrices["test"])
    hi_transfer_result = _reconstruct_hi_champion_transfer(
        champion_candidate_name=champion_candidate_name,
        hi_frame=hi_frame,
        li_frame=li_frame,
    )
    li_native_result = None
    if include_li_native_model:
        li_native_result = _fit_candidate_by_name(
            champion_candidate_name,
            li_matrices["train"],
            li_matrices["validation"],
            li_matrices["test"],
            li_feature_columns,
        )
        li_native_result["notes"] = (
            "Same candidate family retrained natively on LI-Small using the locked LI 60/20/20 chronological split."
        )

    feature_schema_match = _build_feature_schema_match(hi_frame, li_frame)
    comparison_rows = _comparison_rows(
        baseline_results=baseline_results,
        hi_transfer_result=hi_transfer_result,
        li_native_result=li_native_result,
    )
    serializable_baselines = [_serializable_result(row) for row in baseline_results]
    serializable_hi_transfer = _serializable_result(hi_transfer_result)
    serializable_li_native = _serializable_result(li_native_result)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset_name": _DEFAULT_DATASET_NAME,
        "grouping_variant": grouping_variant,
        "transaction_schema": transaction_schema,
        "feature_schema_match": feature_schema_match,
        "li_dataset_stats": li_dataset_stats,
        "li_split_stats": li_split_stats,
        "baseline_results": serializable_baselines,
        "hi_transfer_result": serializable_hi_transfer,
        "li_native_result": serializable_li_native,
        "comparison_rows": comparison_rows,
        "accounts_file_present_but_unused": bool(li_accounts_path and Path(li_accounts_path).exists()),
        "reused_artifacts": {
            "hi_summary_path": str(Path(hi_summary_path).resolve()),
            "hi_feature_path": str(Path(hi_feature_path).resolve()),
            "li_alert_path": str(li_alert_target.resolve()),
            "li_feature_path": str(li_feature_target.resolve()),
            "li_alerts_reused": bool(li_alerts_reused),
            "li_features_reused": bool(li_features_reused),
            "hi_champion_candidate_name": champion_candidate_name,
        },
    }
    summary_target.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report_target.write_text(_render_report(summary), encoding="utf-8")
    logger.info("LI transfer benchmark completed: %s", summary_target)
    return LiTransferBenchmarkResult(
        summary_path=summary_target,
        report_path=report_target,
        li_alert_path=li_alert_target,
        li_feature_path=li_feature_target,
        baseline_results=baseline_results,
        hi_transfer_result=hi_transfer_result,
        li_native_result=li_native_result,
        reused_artifacts=summary["reused_artifacts"],
    )


def _prepare_li_native_matrices(frame):
    prepared = _prepare_frame(frame)
    splits = _split_frame(prepared)
    train, validation, test, _ = _apply_train_only_encodings(
        splits["train"],
        splits["validation"],
        splits["test"],
    )
    feature_columns = _numeric_feature_columns(train)
    for dataset in (train, validation, test):
        dataset[feature_columns] = (
            dataset[feature_columns]
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
            .astype(np.float32)
        )
    return {
        "train": train,
        "validation": validation,
        "test": test,
    }, feature_columns
