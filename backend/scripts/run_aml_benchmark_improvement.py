from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_improvement import run_improved_benchmark


def _default_alert_path() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_feature_cache_dir() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features"


def _default_report_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_v2.md"


def _default_summary_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_v2.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the ALTHEA IBM AML benchmark improvement sprint.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Path to source_account_24h alert JSONL.")
    parser.add_argument("--transactions", default=None, help="Optional raw HI-Small transaction CSV path for source+destination grouping sensitivity.")
    parser.add_argument("--patterns", default=None, help="Optional HI-Small patterns path for source+destination grouping sensitivity.")
    parser.add_argument("--feature-cache-dir", default=str(_default_feature_cache_dir()), help="Directory for compact benchmark feature caches.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="Machine-readable benchmark summary output path.")
    parser.add_argument("--tenant-id", default="default-bank", help="Tenant id used for current ALTHEA baseline diagnosis.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional runtime database URL for current ALTHEA baseline lookup. Defaults to sqlite:///data/althea_enterprise.db",
    )
    parser.add_argument(
        "--object-storage-root",
        default=None,
        help="Optional runtime object-storage root for current ALTHEA baseline lookup. Defaults to data/object_storage",
    )
    parser.add_argument("--skip-grouping-variants", action="store_true", help="Skip 6h and source+destination grouping sensitivity runs.")
    parser.add_argument("--skip-althea-diagnosis", action="store_true", help="Skip current ALTHEA scoring diagnosis.")
    parser.add_argument("--force-rebuild-features", action="store_true", help="Rebuild cached benchmark feature CSVs.")
    args = parser.parse_args()

    result = run_improved_benchmark(
        alert_jsonl_path=args.alerts,
        report_path=args.report,
        summary_path=args.summary,
        feature_cache_dir=args.feature_cache_dir,
        transactions_path=args.transactions,
        patterns_path=args.patterns,
        tenant_id=args.tenant_id,
        database_url=args.database_url,
        object_storage_root=args.object_storage_root,
        include_grouping_variants=not bool(args.skip_grouping_variants),
        include_althea_diagnosis=not bool(args.skip_althea_diagnosis),
        force_rebuild_features=bool(args.force_rebuild_features),
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "dataset_stats": result.dataset_stats,
                "champion": {
                    "name": result.champion.get("name"),
                    "test_recall_at_top_10pct": (result.champion.get("test_metrics") or {}).get("recall_at_top_10pct"),
                },
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
