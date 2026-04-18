from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_li_transfer import run_li_transfer_benchmark


def _repo_root() -> Path:
    return BACKEND_ROOT.parent


def _default_hi_summary_path() -> Path:
    return _repo_root() / "reports" / "benchmark_v2.json"


def _default_hi_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "source_account_24h.features.csv"


def _default_li_alert_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "li_small_alerts.jsonl"


def _default_li_feature_path() -> Path:
    return _repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features" / "li_source_account_24h.features.csv"


def _default_report_path() -> Path:
    return _repo_root() / "reports" / "benchmark_li_transfer_v1.md"


def _default_summary_path() -> Path:
    return _repo_root() / "reports" / "benchmark_li_transfer_v1.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the next LI-Small transfer benchmark using cached HI benchmark artifacts.")
    parser.add_argument("--li-transactions", required=True, help="Path to LI-Small_Trans.csv")
    parser.add_argument("--li-patterns", required=True, help="Path to LI-Small_patterns.txt")
    parser.add_argument("--li-accounts", default=None, help="Optional LI-Small_accounts.csv path; currently recorded only as dataset context.")
    parser.add_argument("--hi-summary", default=str(_default_hi_summary_path()), help="Path to existing HI benchmark_v2 summary JSON.")
    parser.add_argument("--hi-feature-csv", default=str(_default_hi_feature_path()), help="Path to cached HI source_account_24h feature CSV.")
    parser.add_argument("--li-alerts", default=str(_default_li_alert_path()), help="Path to LI alert JSONL output/cache.")
    parser.add_argument("--li-feature-csv", default=str(_default_li_feature_path()), help="Path to LI source_account_24h feature CSV output/cache.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="JSON summary output path.")
    parser.add_argument("--skip-li-native-model", action="store_true", help="Skip LI-native retraining and report only transfer plus baselines.")
    parser.add_argument("--force-rebuild-alerts", action="store_true", help="Rebuild LI alert JSONL even if a cached file exists.")
    parser.add_argument("--force-rebuild-features", action="store_true", help="Rebuild LI feature CSV even if a cached file exists.")
    args = parser.parse_args()

    result = run_li_transfer_benchmark(
        li_transactions_path=args.li_transactions,
        li_patterns_path=args.li_patterns,
        li_accounts_path=args.li_accounts,
        report_path=args.report,
        summary_path=args.summary,
        hi_summary_path=args.hi_summary,
        hi_feature_path=args.hi_feature_csv,
        li_alert_path=args.li_alerts,
        li_feature_path=args.li_feature_csv,
        include_li_native_model=not bool(args.skip_li_native_model),
        force_rebuild_alerts=bool(args.force_rebuild_alerts),
        force_rebuild_features=bool(args.force_rebuild_features),
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "li_alert_path": str(result.li_alert_path.resolve()),
                "li_feature_path": str(result.li_feature_path.resolve()),
                "hi_transfer_recall_at_top_10pct": result.hi_transfer_result["test_metrics"].get("recall_at_top_10pct"),
                "li_native_ran": bool(result.li_native_result),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
