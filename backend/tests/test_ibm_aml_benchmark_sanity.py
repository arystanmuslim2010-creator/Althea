from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from benchmarks.ibm_aml_sanity import _subgroup_metrics, run_benchmark_sanity_check


def _write_alert_jsonl(path: Path, alerts: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")
    return path


def test_run_benchmark_sanity_check_produces_reports_and_randomization_collapse(tmp_path: Path) -> None:
    alert_path = tmp_path / "alerts.jsonl"
    alerts: list[dict] = []
    for index in range(240):
        is_positive = 1 if index % 4 == 0 else 0
        tx_count = 4 if is_positive else 1
        transactions = []
        for offset in range(tx_count):
            transactions.append(
                {
                    "transaction_id": f"T-{index}-{offset}",
                    "amount": float((index + 1) * (250 if is_positive else 15) * (offset + 1)),
                    "timestamp": f"2022-09-{(index % 28) + 1:02d}T0{offset}:00:00Z",
                    "receiver": f"{300 + offset}:DST-{index}-{offset}",
                    "currency": "US Dollar" if offset % 2 == 0 else "Euro",
                    "channel": "Wire" if is_positive else "ACH",
                    "optional_fields": {
                        "from_bank": f"{100 + (index % 5)}",
                        "to_bank": f"{300 + offset}",
                        "is_laundering": is_positive if offset == 0 else 0,
                    },
                }
            )
        alerts.append(
            {
                "alert_id": f"A{index:04d}",
                "created_at": f"2022-09-{(index % 28) + 1:02d}T00:00:00Z",
                "source_account_key": f"{100 + (index % 5)}:SRC-{index // 3}",
                "typology": "unknown",
                "evaluation_label_is_sar": is_positive,
                "transactions": transactions,
            }
        )
    _write_alert_jsonl(alert_path, alerts)

    result = run_benchmark_sanity_check(
        alert_jsonl_path=alert_path,
        feature_cache_dir=tmp_path / "feature_cache",
        report_path=tmp_path / "benchmark_sanity.md",
        summary_path=tmp_path / "benchmark_sanity.json",
        protocol_path=tmp_path / "benchmark_protocol.md",
        dataset_dir=tmp_path / "missing_dataset_dir",
        force_rebuild_features=True,
    )

    assert result.summary_path.exists()
    assert result.report_path.exists()
    assert (tmp_path / "benchmark_protocol.md").exists()
    full_row = next(row for row in result.ablations if row["name"] == "full_feature_champion")
    shuffled_recall = result.randomization["metrics"]["recall_at_top_10pct"]
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert full_row["recall_at_top_10pct"] >= shuffled_recall
    assert "sensitivity" in summary
    assert result.verdict["trustworthiness"]


def test_subgroup_metrics_keeps_amount_buckets_consistent_between_full_and_top_decile() -> None:
    rows = []
    for index in range(120):
        rows.append(
            {
                "alert_id": f"A{index:04d}",
                "created_at": pd.Timestamp("2022-09-01T00:00:00Z") + pd.Timedelta(hours=index),
                "evaluation_label_is_sar": 1 if index % 3 == 0 else 0,
                "total_amount_usd": float(index + 1),
                "score": float(1000 - index),
            }
        )
    frame = pd.DataFrame(rows)
    metrics = _subgroup_metrics(frame, score_column="score", group_name="amount_bucket", min_positive_count=1)
    assert metrics
    assert all(0.0 <= row["subgroup_recall_in_global_top_10pct"] <= 1.0 for row in metrics)
