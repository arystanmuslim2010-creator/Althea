from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_sanity import run_benchmark_sanity_check


def _default_alert_path() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def _default_feature_cache_dir() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features"


def _default_report_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_sanity_v1.md"


def _default_summary_path() -> Path:
    return BACKEND_ROOT.parent / "reports" / "benchmark_sanity_v1.json"


def _default_protocol_path() -> Path:
    return BACKEND_ROOT.parent / "docs" / "benchmarks" / "ibm_aml_benchmark_protocol.md"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run strict sanity checks on the ALTHEA IBM-derived alert benchmark.")
    parser.add_argument("--alerts", default=str(_default_alert_path()), help="Alert JSONL path.")
    parser.add_argument("--feature-cache-dir", default=str(_default_feature_cache_dir()), help="Feature cache directory.")
    parser.add_argument("--report", default=str(_default_report_path()), help="Markdown report output path.")
    parser.add_argument("--summary", default=str(_default_summary_path()), help="JSON summary output path.")
    parser.add_argument("--protocol", default=str(_default_protocol_path()), help="Benchmark protocol markdown output path.")
    parser.add_argument("--dataset-dir", default=None, help="Optional IBM dataset directory containing HI/LI transaction and pattern files.")
    parser.add_argument("--force-rebuild-features", action="store_true", help="Rebuild cached feature CSVs.")
    args = parser.parse_args()

    result = run_benchmark_sanity_check(
        alert_jsonl_path=args.alerts,
        feature_cache_dir=args.feature_cache_dir,
        report_path=args.report,
        summary_path=args.summary,
        protocol_path=args.protocol,
        dataset_dir=args.dataset_dir,
        force_rebuild_features=bool(args.force_rebuild_features),
    )
    print(
        json.dumps(
            {
                "summary_path": str(result.summary_path.resolve()),
                "report_path": str(result.report_path.resolve()),
                "trustworthiness": result.verdict.get("trustworthiness"),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
