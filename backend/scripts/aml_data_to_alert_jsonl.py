from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from benchmarks.ibm_aml_data import convert_transactions_to_alert_jsonl


def _default_output_path() -> Path:
    return BACKEND_ROOT.parent / "data" / "processed" / "ibm_aml_alerts" / "hi_small_alerts.jsonl"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert IBM AML-Data transactions into ALTHEA-compatible alert JSONL."
    )
    parser.add_argument("--transactions", required=True, help="Path to IBM AML-Data transaction CSV")
    parser.add_argument("--patterns", required=True, help="Path to IBM AML-Data pattern file")
    parser.add_argument("--output", default=str(_default_output_path()), help="Path to output alert JSONL")
    parser.add_argument(
        "--summary-output",
        default=None,
        help="Optional path for conversion summary JSON. Defaults to <output>.summary.json",
    )
    parser.add_argument(
        "--dataset-name",
        default="IBM AML-Data HI-Small",
        help="Dataset label written into alert metadata and used for deterministic alert id prefixing.",
    )
    parser.add_argument("--window-hours", type=int, default=24, help="Anchored source-account grouping window in hours")
    args = parser.parse_args()

    summary = convert_transactions_to_alert_jsonl(
        transactions_path=args.transactions,
        patterns_path=args.patterns,
        output_path=args.output,
        dataset_name=args.dataset_name,
        write_summary_path=args.summary_output,
        window_hours=args.window_hours,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
