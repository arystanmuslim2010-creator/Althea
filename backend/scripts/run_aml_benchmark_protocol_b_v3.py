from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[1]


if str(_backend_root()) not in sys.path:
    sys.path.insert(0, str(_backend_root()))


from benchmarks.ibm_aml_protocol_b_v3 import run_protocol_b_v3_benchmark


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_alert_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_base_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.features.csv"


def _default_extra_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.v2_extra.features.csv"


def _default_horizon_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.v3_horizon.features.csv"


def _default_graph_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.v3_graph.features.csv"


def _default_sequence_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.v3_sequence.features.csv"


def _default_report_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v3.md"


def _default_summary_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v3.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the strict ALTHEA IBM AML Benchmark Protocol B v3 stack.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Path to the strict Protocol B source alert JSONL.")
    parser.add_argument("--base-feature-csv", default=str(_default_base_feature_path()), help="Path to the Protocol B base feature CSV.")
    parser.add_argument("--extra-feature-csv", default=str(_default_extra_feature_path()), help="Path to the Protocol B v2 extra feature CSV.")
    parser.add_argument("--horizon-feature-csv", default=str(_default_horizon_feature_path()), help="Path to the Protocol B v3 horizon feature CSV.")
    parser.add_argument("--graph-feature-csv", default=str(_default_graph_feature_path()), help="Path to the Protocol B v3 graph feature CSV.")
    parser.add_argument("--sequence-feature-csv", default=str(_default_sequence_feature_path()), help="Path to the Protocol B v3 sequence feature CSV.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Destination markdown report path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="Destination JSON summary path.")
    parser.add_argument("--force-rebuild-horizon-features", action="store_true", help="Rebuild only the horizon feature cache.")
    parser.add_argument("--force-rebuild-graph-features", action="store_true", help="Rebuild only the graph feature cache.")
    parser.add_argument("--force-rebuild-sequence-features", action="store_true", help="Rebuild only the sequence feature cache.")
    parser.add_argument("--skip-lambdarank", action="store_true", help="Skip the LambdaRank candidate even if xgboost is available.")
    args = parser.parse_args()

    result = run_protocol_b_v3_benchmark(
        alert_jsonl_path=args.alerts,
        base_feature_csv_path=args.base_feature_csv,
        extra_feature_csv_path=args.extra_feature_csv,
        horizon_feature_csv_path=args.horizon_feature_csv,
        graph_feature_csv_path=args.graph_feature_csv,
        sequence_feature_csv_path=args.sequence_feature_csv,
        report_path=args.report,
        summary_path=args.summary,
        force_rebuild_horizon_features=args.force_rebuild_horizon_features,
        force_rebuild_graph_features=args.force_rebuild_graph_features,
        force_rebuild_sequence_features=args.force_rebuild_sequence_features,
        include_lambdarank=not args.skip_lambdarank,
    )

    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path),
                "report_path": str(result.report_path),
                "protocol_b_v3_champion": result.champion["name"],
                "protocol_b_v3_recall_at_top_10pct": result.champion["test_metrics"]["recall_at_top_10pct"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
