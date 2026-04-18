from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_protocol_b import run_protocol_b_benchmark


def _repo_root() -> Path:
    return BACKEND_ROOT.parent


def _default_alert_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "protocol_b_source_account_24h.features.csv"


def _default_report_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v1.md"


def _default_summary_path() -> Path:
    return _repo_root() / "reports" / "benchmark_protocol_b_v1.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the stricter ALTHEA IBM AML Benchmark Protocol B.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Path to source alert JSONL with raw transaction payloads.")
    parser.add_argument("--feature-csv", default=str(_default_feature_path()), help="Path to protocol-B feature cache CSV.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="JSON summary output path.")
    parser.add_argument("--force-rebuild-features", action="store_true", help="Rebuild protocol-B feature CSV even if it already exists.")
    args = parser.parse_args()

    result = run_protocol_b_benchmark(
        alert_jsonl_path=args.alerts,
        feature_csv_path=args.feature_csv,
        report_path=args.report,
        summary_path=args.summary,
        force_rebuild_features=bool(args.force_rebuild_features),
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "feature_csv_path": str(result.feature_csv_path.resolve()),
                "protocol_b_recall_at_top_10pct": result.primary_result["test_metrics"]["recall_at_top_10pct"],
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
