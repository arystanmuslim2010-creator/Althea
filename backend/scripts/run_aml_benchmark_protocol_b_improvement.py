from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_protocol_b_improvement import run_protocol_b_improvement_benchmark


def _repo_root() -> Path:
    return BACKEND_ROOT.parent


def _default_alert_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_base_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.features.csv"


def _default_extra_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.v2_extra.features.csv"


def _default_report_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v2.md"


def _default_summary_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v2.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the strict ALTHEA IBM AML Benchmark Protocol B improvement sprint.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Path to strict Protocol B source alert JSONL.")
    parser.add_argument("--base-feature-csv", default=str(_default_base_feature_path()), help="Path to the existing Protocol B v1 base feature CSV.")
    parser.add_argument("--extra-feature-csv", default=str(_default_extra_feature_path()), help="Path to the additional strict Protocol B v2 feature CSV.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="JSON summary output path.")
    parser.add_argument("--force-rebuild-extra-features", action="store_true", help="Rebuild only the Protocol B v2 extra feature cache.")
    parser.add_argument("--skip-lightgbm", action="store_true", help="Run logistic candidates only.")
    args = parser.parse_args()

    result = run_protocol_b_improvement_benchmark(
        alert_jsonl_path=args.alerts,
        base_feature_csv_path=args.base_feature_csv,
        extra_feature_csv_path=args.extra_feature_csv,
        report_path=args.report,
        summary_path=args.summary,
        force_rebuild_extra_features=bool(args.force_rebuild_extra_features),
        include_lightgbm=not bool(args.skip_lightgbm),
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "base_feature_csv_path": str(result.base_feature_csv_path.resolve()),
                "extra_feature_csv_path": str(result.extra_feature_csv_path.resolve()),
                "protocol_b_v2_champion": result.champion["name"],
                "protocol_b_v2_recall_at_top_10pct": result.champion["test_metrics"]["recall_at_top_10pct"],
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
