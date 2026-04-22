from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from benchmarks.ibm_aml_improvement import _add_derived_features, _compute_baselines, _split_frame
from benchmarks.ibm_aml_protocol_b import _display_path, _ensure_parent_dir, _isoformat_utc, load_protocol_b_feature_frame
from benchmarks.ibm_aml_protocol_b_improvement import (
    _comparison_rows,
    _fit_sparse_logistic_candidate,
    _metric_tuple,
    _primary_feature_columns,
    _serialize_result,
    load_protocol_b_improved_feature_frame,
)
from feature_extraction.graph_features import (
    GRAPH_FEATURE_COLUMNS,
    extract_graph_feature_csv_from_alert_jsonl,
    load_graph_feature_frame,
)
from feature_extraction.horizon_features import (
    HORIZON_FEATURE_COLUMNS,
    extract_horizon_feature_csv,
    load_horizon_feature_frame,
)
from models.ranking_model import fit_lambdarank_candidate, fit_pairwise_ranker, fit_two_stage_reranker
from models.sequence_model import SEQUENCE_FEATURE_COLUMNS, build_sequence_feature_frame, merge_sequence_features


_DATASET_NAME = "IBM AML-Data HI-Small Protocol B v3"
_REFERENCE_PROTOCOL_B_V2_SUMMARY = "benchmark_protocol_b_v2.json"
_SELECTED_HORIZON_COLUMNS = {
    "hist_tx_count_24h",
    "hist_tx_count_7d",
    "hist_total_amount_vs_24h_mean",
    "hist_total_amount_vs_7d_mean",
    "hist_mean_amount_vs_24h_mean",
    "hist_recency_weighted_tx_count_24h",
    "hist_recency_weighted_amount_usd_24h",
    "hist_recency_weighted_counterparty_diversity_24h",
    "fingerprint_total_amount_usd_median",
    "fingerprint_counterparty_diversity_median",
    "deviation_total_amount_vs_fingerprint",
    "deviation_counterparty_diversity_vs_fingerprint",
    "deviation_currency_entropy_vs_fingerprint",
    "deviation_payment_entropy_vs_fingerprint",
    "deviation_cross_bank_ratio_vs_history_mean",
    "history_active_windows",
    "history_depth_hours",
}
_SELECTED_GRAPH_COLUMNS = set(GRAPH_FEATURE_COLUMNS)
_SELECTED_SEQUENCE_COLUMNS = set(SEQUENCE_FEATURE_COLUMNS)


@dataclass(slots=True)
class ProtocolBV3BenchmarkResult:
    summary_path: Path
    report_path: Path
    horizon_feature_csv_path: Path
    graph_feature_csv_path: Path
    sequence_feature_csv_path: Path
    dataset_stats: dict[str, Any]
    baseline_results: list[dict[str, Any]]
    candidate_results: list[dict[str, Any]]
    champion: dict[str, Any]
    ablations: list[dict[str, Any]]


def _current_protocol_b_champion_columns(
    *,
    feature_columns: list[str],
    base_feature_columns: list[str],
) -> list[str]:
    return list(
        base_feature_columns
        + [
            column
            for column in feature_columns
            if column in {
                "counterparty_hhi_tx",
                "counterparty_hhi_amount",
                "bank_hhi_tx",
                "bank_hhi_amount",
                "seen_counterparty_tx_ratio_hist",
                "seen_counterparty_amount_share_hist",
                "new_counterparty_ratio_hist",
                "new_bank_ratio_hist",
                "currency_entropy",
                "payment_format_entropy",
                "currency_entropy_vs_prior_anchor_avg",
                "payment_format_entropy_vs_prior_anchor_avg",
            }
        ]
    )


def _dataset_stats(frame: pd.DataFrame) -> dict[str, Any]:
    total = len(frame)
    positives = int(frame["evaluation_label_is_sar"].sum()) if total else 0
    return {
        "total_alerts": int(total),
        "positive_alerts": positives,
        "negative_alerts": int(max(total - positives, 0)),
        "positive_rate": float(positives / total) if total else 0.0,
        "average_transactions_per_alert": float(frame["transaction_count"].mean()) if total else 0.0,
    }


def _write_sequence_feature_csv(
    frame: pd.DataFrame,
    output_csv_path: str | Path,
    *,
    force_rebuild: bool = False,
) -> tuple[Path, dict[str, Any]]:
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path, {"reused_existing_sequence_feature_csv": True}
    sequence_frame = build_sequence_feature_frame(frame)
    _ensure_parent_dir(output_path)
    sequence_frame.to_csv(output_path, index=False)
    return output_path, {
        "reused_existing_sequence_feature_csv": False,
        "rows_written": int(len(sequence_frame)),
        "feature_columns": list(SEQUENCE_FEATURE_COLUMNS),
    }


def load_protocol_b_v3_frame(
    base_feature_csv_path: str | Path,
    extra_feature_csv_path: str | Path,
    horizon_feature_csv_path: str | Path,
    graph_feature_csv_path: str | Path,
    sequence_feature_csv_path: str | Path,
) -> pd.DataFrame:
    frame = load_protocol_b_improved_feature_frame(base_feature_csv_path, extra_feature_csv_path)
    frame = load_horizon_feature_frame(frame, horizon_feature_csv_path)
    frame = load_graph_feature_frame(frame, graph_feature_csv_path)
    sequence_frame = pd.read_csv(sequence_feature_csv_path)
    sequence_frame["alert_id"] = sequence_frame["alert_id"].astype(str)
    for column in SEQUENCE_FEATURE_COLUMNS:
        sequence_frame[column] = pd.to_numeric(sequence_frame[column], errors="coerce").fillna(0.0).astype(np.float32)
    frame = merge_sequence_features(frame, sequence_frame)
    numeric_columns = [
        column
        for column in frame.columns
        if column not in {
            "alert_id",
            "created_at",
            "grouping_variant",
            "source_account_key",
            "source_bank",
            "dominant_destination_bank",
            "dominant_currency",
            "dominant_payment_format",
            "typology",
        }
    ]
    frame[numeric_columns] = frame[numeric_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return frame


def _ranking_candidate_result(
    ranking_result: Any,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    *,
    name: str,
    notes: str,
) -> dict[str, Any]:
    from benchmarks.ibm_aml_improvement import _ranking_metrics_from_scores

    validation_scored = validation_df.copy()
    validation_scored["model_score"] = ranking_result.validation_scores.astype(np.float32)
    test_scored = test_df.copy()
    test_scored["model_score"] = ranking_result.test_scores.astype(np.float32)
    return {
        "name": name,
        "kind": "model",
        "family": ranking_result.metadata.get("family", "ranking_model"),
        "notes": notes,
        "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
        "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
        "top_features": [],
        "feature_columns": list(ranking_result.metadata.get("feature_columns", [])),
        "ranking_metadata": ranking_result.metadata,
        "model_object": ranking_result.model_object,
    }


def _run_v3_candidates(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    *,
    reference_columns: list[str],
    horizon_columns: list[str],
    graph_columns: list[str],
    sequence_columns: list[str],
    include_lambdarank: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    fused_columns = list(dict.fromkeys(reference_columns + horizon_columns + graph_columns + sequence_columns))
    horizon_augmented = list(dict.fromkeys(reference_columns + horizon_columns))
    graph_augmented = list(dict.fromkeys(reference_columns + graph_columns))
    sequence_augmented = list(dict.fromkeys(reference_columns + sequence_columns))
    sparse_specs = [
        (
            "protocol_b_v3_reference_sparse_logistic",
            reference_columns,
            "Current Protocol B validation-safe reference carried into the v3 run.",
        ),
        (
            "protocol_b_v3_horizon_sparse_logistic",
            horizon_augmented,
            "Reference feature stack plus strict multi-horizon account-history features.",
        ),
        (
            "protocol_b_v3_graph_sparse_logistic",
            graph_augmented,
            "Reference feature stack plus strict graph/motif features from prior interactions only.",
        ),
        (
            "protocol_b_v3_sequence_sparse_logistic",
            sequence_augmented,
            "Reference feature stack plus compact past-only sequence summary features.",
        ),
        (
            "protocol_b_v3_fused_sparse_logistic",
            fused_columns,
            "Reference stack plus horizon, graph, and sequence layers combined in one sparse linear model.",
        ),
    ]
    for name, columns, notes in sparse_specs:
        candidates.append(
            _fit_sparse_logistic_candidate(
                train_df,
                validation_df,
                test_df,
                columns,
                name=name,
                notes=notes,
                c_value=0.15 if name.endswith("fused_sparse_logistic") else 0.2,
                l1_ratio=1.0,
            )
        )
    pairwise = fit_pairwise_ranker(train_df, validation_df, test_df, fused_columns)
    candidates.append(
        _ranking_candidate_result(
            pairwise,
            validation_df,
            test_df,
            name="protocol_b_v3_pairwise_ranker",
            notes="Pairwise logistic ranking model trained on positive-vs-negative queue ordering within chronological day buckets.",
        )
    )
    reranker = fit_two_stage_reranker(train_df, validation_df, test_df, fused_columns)
    candidates.append(
        _ranking_candidate_result(
            reranker,
            validation_df,
            test_df,
            name="protocol_b_v3_two_stage_reranker",
            notes="Two-stage reranker: logistic stage-1 queue score with a second-stage top-queue rerank pass.",
        )
    )
    if include_lambdarank:
        lambdarank = fit_lambdarank_candidate(train_df, validation_df, test_df, fused_columns)
        if lambdarank is not None:
            candidates.append(
                _ranking_candidate_result(
                    lambdarank,
                    validation_df,
                    test_df,
                    name="protocol_b_v3_lambdarank",
                    notes="LambdaRank candidate over fused strict Protocol B v3 features using chronological day buckets as query groups.",
                )
            )
    champion = max(candidates, key=lambda row: _metric_tuple(row["validation_metrics"]))
    fused_reference = next(row for row in candidates if row["name"] == "protocol_b_v3_fused_sparse_logistic")
    return candidates, champion, fused_reference


def _run_v3_ablations(
    fused_reference: dict[str, Any],
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    *,
    reference_columns: list[str],
    horizon_columns: list[str],
    graph_columns: list[str],
    sequence_columns: list[str],
) -> list[dict[str, Any]]:
    fused_columns = list(dict.fromkeys(reference_columns + horizon_columns + graph_columns + sequence_columns))
    specs = [
        ("ablation_remove_horizon_layer", set(horizon_columns), "Removed the v3 multi-horizon history layer."),
        ("ablation_remove_graph_layer", set(graph_columns), "Removed the v3 graph/motif layer."),
        ("ablation_remove_sequence_layer", set(sequence_columns), "Removed the v3 sequence-summary layer."),
    ]
    baseline_recall = float(fused_reference["test_metrics"]["recall_at_top_10pct"])
    ablations: list[dict[str, Any]] = []
    for name, excluded, notes in specs:
        columns = [column for column in fused_columns if column not in excluded]
        result = _fit_sparse_logistic_candidate(
            train_df,
            validation_df,
            test_df,
            columns,
            name=name,
            notes=notes,
            c_value=0.15,
            l1_ratio=1.0,
        )
        serialized = _serialize_result(result)
        serialized["delta_recall_at_top_10pct_vs_fused_sparse"] = float(
            serialized["test_metrics"]["recall_at_top_10pct"] - baseline_recall
        )
        ablations.append(serialized)
    return ablations


def _load_v2_reference() -> dict[str, Any] | None:
    path = Path(__file__).resolve().parents[2] / "reports" / _REFERENCE_PROTOCOL_B_V2_SUMMARY
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _render_report(summary: dict[str, Any]) -> str:
    champion = summary["champion"]
    reference = summary["reference_candidate"]
    strongest = summary.get("strongest_component")
    lines = [
        "# ALTHEA IBM AML Benchmark Protocol B v3",
        "",
        "This run keeps the strict Protocol B benchmark unchanged and evaluates a richer past-only stack.",
        "",
        "## Final Outcome",
        "",
        f"- Validation-safe champion: `{champion['name']}`",
        f"- Champion test Recall@Top 10%: `{champion['test_metrics']['recall_at_top_10pct']:.4f}`",
        f"- Reference test Recall@Top 10%: `{reference['test_metrics']['recall_at_top_10pct']:.4f}`",
        f"- Improvement vs reference: `{champion['test_metrics']['recall_at_top_10pct'] - reference['test_metrics']['recall_at_top_10pct']:+.4f}`",
    ]
    if strongest is not None:
        lines.extend(
            [
                "",
                "## Strongest Component",
                "",
                f"- Strongest ablation drop: `{strongest['name']}`",
                f"- Delta Recall@Top 10% vs fused sparse candidate: `{strongest['delta_recall_at_top_10pct_vs_fused_sparse']:+.4f}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Benchmark Safety",
            "",
            "- Past-only features: `True`",
            "- Future-only labels: `True`",
            "- Chronological split: `True`",
            "- Pattern shortcuts in primary score: `False`",
            "- Test-based model selection: `False`",
        ]
    )
    return "\n".join(lines) + "\n"


def run_protocol_b_v3_benchmark(
    *,
    alert_jsonl_path: str | Path,
    base_feature_csv_path: str | Path,
    extra_feature_csv_path: str | Path,
    horizon_feature_csv_path: str | Path,
    graph_feature_csv_path: str | Path,
    sequence_feature_csv_path: str | Path,
    report_path: str | Path,
    summary_path: str | Path,
    force_rebuild_horizon_features: bool = False,
    force_rebuild_graph_features: bool = False,
    force_rebuild_sequence_features: bool = False,
    include_lambdarank: bool = True,
) -> ProtocolBV3BenchmarkResult:
    reference_base_frame = _add_derived_features(load_protocol_b_feature_frame(base_feature_csv_path))
    base_feature_columns = _primary_feature_columns(reference_base_frame)
    improved_frame = load_protocol_b_improved_feature_frame(base_feature_csv_path, extra_feature_csv_path)
    horizon_path, horizon_summary = extract_horizon_feature_csv(
        improved_frame,
        horizon_feature_csv_path,
        force_rebuild=force_rebuild_horizon_features,
    )
    graph_path, graph_summary = extract_graph_feature_csv_from_alert_jsonl(
        alert_jsonl_path,
        graph_feature_csv_path,
        force_rebuild=force_rebuild_graph_features,
    )
    sequence_path, sequence_summary = _write_sequence_feature_csv(
        improved_frame,
        sequence_feature_csv_path,
        force_rebuild=force_rebuild_sequence_features,
    )
    frame = load_protocol_b_v3_frame(
        base_feature_csv_path,
        extra_feature_csv_path,
        horizon_path,
        graph_path,
        sequence_path,
    )
    dataset_stats = _dataset_stats(frame)
    splits = _split_frame(frame)
    split_stats = {name: _dataset_stats(dataset) for name, dataset in splits.items()}
    feature_columns = _primary_feature_columns(frame)
    reference_columns = _current_protocol_b_champion_columns(
        feature_columns=feature_columns,
        base_feature_columns=base_feature_columns,
    )
    horizon_columns = [column for column in HORIZON_FEATURE_COLUMNS if column in frame.columns and column in _SELECTED_HORIZON_COLUMNS]
    graph_columns = [column for column in GRAPH_FEATURE_COLUMNS if column in frame.columns and column in _SELECTED_GRAPH_COLUMNS]
    sequence_columns = [column for column in SEQUENCE_FEATURE_COLUMNS if column in frame.columns and column in _SELECTED_SEQUENCE_COLUMNS]
    for dataset in splits.values():
        numeric_columns = list(dict.fromkeys(reference_columns + horizon_columns + graph_columns + sequence_columns + feature_columns))
        dataset[numeric_columns] = dataset[numeric_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0).astype(np.float32)
    baseline_results = [
        _serialize_result(row)
        for row in _compute_baselines(splits["train"], splits["validation"], splits["test"])
        if row["name"] in {"amount_descending", "weighted_signal_heuristic", "chronological_queue"}
    ]
    candidate_results_raw, champion_raw, fused_reference_raw = _run_v3_candidates(
        splits["train"],
        splits["validation"],
        splits["test"],
        reference_columns=reference_columns,
        horizon_columns=horizon_columns,
        graph_columns=graph_columns,
        sequence_columns=sequence_columns,
        include_lambdarank=include_lambdarank,
    )
    candidate_results = [_serialize_result(row) for row in candidate_results_raw]
    champion = _serialize_result(champion_raw)
    fused_reference = _serialize_result(fused_reference_raw)
    ablations = _run_v3_ablations(
        fused_reference,
        splits["train"],
        splits["validation"],
        splits["test"],
        reference_columns=reference_columns,
        horizon_columns=horizon_columns,
        graph_columns=graph_columns,
        sequence_columns=sequence_columns,
    )
    strongest_component = min(
        ablations,
        key=lambda row: float(row.get("delta_recall_at_top_10pct_vs_fused_sparse", 0.0)),
    ) if ablations else None
    summary = {
        "generated_at": _isoformat_utc(datetime.now(timezone.utc)),
        "dataset_name": _DATASET_NAME,
        "grouping_variant": "source_account_past24h_future24h",
        "source_alert_jsonl_path": str(Path(alert_jsonl_path).resolve()),
        "base_feature_csv_path": str(Path(base_feature_csv_path).resolve()),
        "extra_feature_csv_path": str(Path(extra_feature_csv_path).resolve()),
        "horizon_feature_csv_path": str(horizon_path.resolve()),
        "graph_feature_csv_path": str(graph_path.resolve()),
        "sequence_feature_csv_path": str(sequence_path.resolve()),
        "feature_extraction_summary": {
            "horizon": horizon_summary,
            "graph": graph_summary,
            "sequence": sequence_summary,
        },
        "dataset_stats": dataset_stats,
        "split_stats": split_stats,
        "feature_groups": {
            "reference": reference_columns,
            "horizon": horizon_columns,
            "graph": graph_columns,
            "sequence": sequence_columns,
        },
        "baseline_results": baseline_results,
        "candidate_results": candidate_results,
        "reference_candidate": next(row for row in candidate_results if row["name"] == "protocol_b_v3_reference_sparse_logistic"),
        "fused_sparse_candidate": fused_reference,
        "champion": champion,
        "ablations": ablations,
        "strongest_component": strongest_component,
        "benchmark_rows": _comparison_rows(baseline_results + candidate_results),
        "protocol_b_v2_reference_summary": _load_v2_reference(),
        "protocol_safety_claims": {
            "past_only_features": True,
            "future_only_labels": True,
            "chronological_split": True,
            "pattern_shortcuts_removed_from_primary": True,
            "test_data_used_for_tuning": False,
        },
    }
    summary_target = Path(summary_path)
    report_target = Path(report_path)
    _ensure_parent_dir(summary_target)
    _ensure_parent_dir(report_target)
    summary_target.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report_target.write_text(_render_report(summary), encoding="utf-8")
    return ProtocolBV3BenchmarkResult(
        summary_path=summary_target,
        report_path=report_target,
        horizon_feature_csv_path=horizon_path,
        graph_feature_csv_path=graph_path,
        sequence_feature_csv_path=sequence_path,
        dataset_stats=dataset_stats,
        baseline_results=baseline_results,
        candidate_results=candidate_results,
        champion=champion,
        ablations=ablations,
    )
