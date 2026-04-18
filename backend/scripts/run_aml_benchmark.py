from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_data import run_benchmark


def _default_alert_path() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_conversion_summary_path() -> Path:
    return _default_alert_path().with_suffix(".summary.json")


def _default_report_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_v1.md"


def _default_summary_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_v1.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the first ALTHEA benchmark on IBM AML-Data derived alerts.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Path to alert JSONL produced by the converter")
    parser.add_argument(
        "--conversion-summary",
        default=str(_default_conversion_summary_path()),
        help="Optional conversion summary JSON from aml_data_to_alert_jsonl.py",
    )
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="Machine-readable benchmark summary JSON path")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional ALTHEA runtime database URL for local model registry lookup. Defaults to sqlite:///data/althea_enterprise.db",
    )
    parser.add_argument(
        "--object-storage-root",
        default=None,
        help="Optional object storage root for model artifacts. Defaults to data/object_storage",
    )
    parser.add_argument("--tenant-id", default="default-bank", help="Tenant id used for ALTHEA score baseline")
    parser.add_argument(
        "--disable-althea-baseline",
        action="store_true",
        help="Skip ALTHEA score-based ranking and run only the simple baselines",
    )
    parser.add_argument(
        "--model-selection-strategy",
        default="active_approved",
        help="Model selection strategy for the ALTHEA score baseline",
    )
    args = parser.parse_args()

    result = run_benchmark(
        alert_jsonl_path=args.alerts,
        conversion_summary_path=args.conversion_summary,
        report_path=args.report,
        summary_path=args.summary,
        database_url=args.database_url,
        object_storage_root=args.object_storage_root,
        tenant_id=args.tenant_id,
        include_althea_baseline=not bool(args.disable_althea_baseline),
        model_selection_strategy=args.model_selection_strategy,
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "dataset_stats": result.dataset_stats,
                "althea_baseline_status": result.althea_baseline_status,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
