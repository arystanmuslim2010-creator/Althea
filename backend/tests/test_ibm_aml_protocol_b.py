from __future__ import annotations

import json
from pathlib import Path

from benchmarks.ibm_aml_protocol_b import (
    extract_protocol_b_feature_csv_from_alert_jsonl,
    load_protocol_b_feature_frame,
    run_protocol_b_benchmark,
)


def _write_alert_jsonl(path: Path, alerts: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")
    return path


def test_protocol_b_uses_past_window_for_features_and_future_window_for_labels(tmp_path: Path) -> None:
    alerts = [
        {
            "alert_id": "A1",
            "created_at": "2022-09-01T00:00:00Z",
            "source_account_key": "010:SRC1",
            "transactions": [
                {
                    "transaction_id": "T1",
                    "amount": 100.0,
                    "timestamp": "2022-09-01T00:00:00Z",
                    "receiver": "011:DST1",
                    "currency": "US Dollar",
                    "channel": "ACH",
                    "optional_fields": {"from_bank": "010", "to_bank": "011", "is_laundering": 0, "pattern_typology": "FAN-OUT"},
                },
                {
                    "transaction_id": "T2",
                    "amount": 80.0,
                    "timestamp": "2022-09-01T01:00:00Z",
                    "receiver": "012:DST2",
                    "currency": "US Dollar",
                    "channel": "ACH",
                    "optional_fields": {"from_bank": "010", "to_bank": "012", "is_laundering": 1, "pattern_typology": "unknown"},
                },
                {
                    "transaction_id": "T3",
                    "amount": 90.0,
                    "timestamp": "2022-09-02T01:00:00Z",
                    "receiver": "013:DST3",
                    "currency": "US Dollar",
                    "channel": "Wire",
                    "optional_fields": {"from_bank": "010", "to_bank": "013", "is_laundering": 0, "pattern_typology": "unknown"},
                },
            ],
        }
    ]
    alert_path = _write_alert_jsonl(tmp_path / "alerts.jsonl", alerts)
    feature_path, _ = extract_protocol_b_feature_csv_from_alert_jsonl(alert_path, tmp_path / "protocol_b.csv", force_rebuild=True)
    frame = load_protocol_b_feature_frame(feature_path)

    assert len(frame) == 2
    first = frame.iloc[0]
    second = frame.iloc[1]
    assert int(first["evaluation_label_is_sar"]) == 1
    assert int(second["evaluation_label_is_sar"]) == 0
    assert int(first["pattern_tx_count"]) == 1


def test_run_protocol_b_benchmark_produces_report_and_summary(tmp_path: Path) -> None:
    alerts: list[dict] = []
    for index in range(80):
        tx_count = 3 if index % 5 == 0 else 1
        transactions = []
        for offset in range(tx_count):
            transactions.append(
                {
                    "transaction_id": f"T-{index}-{offset}",
                    "amount": float((index + 1) * (50 if index % 5 == 0 else 10) * (offset + 1)),
                    "timestamp": f"2022-09-{(index % 28) + 1:02d}T0{offset}:00:00Z",
                    "receiver": f"{300 + offset}:DST-{index}-{offset}",
                    "currency": "US Dollar" if offset % 2 == 0 else "Euro",
                    "channel": "Wire" if index % 5 == 0 else "ACH",
                    "optional_fields": {
                        "from_bank": f"{100 + (index % 7)}",
                        "to_bank": f"{300 + offset}",
                        "is_laundering": 1 if index % 5 == 0 and offset == tx_count - 1 else 0,
                        "pattern_typology": "unknown",
                    },
                }
            )
        alerts.append(
            {
                "alert_id": f"A{index:04d}",
                "created_at": f"2022-09-{(index % 28) + 1:02d}T00:00:00Z",
                "source_account_key": f"{100 + (index % 7)}:SRC-{index // 3}",
                "transactions": transactions,
            }
        )
    alert_path = _write_alert_jsonl(tmp_path / "alerts.jsonl", alerts)
    result = run_protocol_b_benchmark(
        alert_jsonl_path=alert_path,
        feature_csv_path=tmp_path / "protocol_b.features.csv",
        report_path=tmp_path / "benchmark_protocol_b_v1.md",
        summary_path=tmp_path / "benchmark_protocol_b_v1.json",
        force_rebuild_features=True,
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert result.report_path.exists()
    assert result.summary_path.exists()
    assert summary["protocol_reduction_claims"]["label_feature_window_decoupled"] is True
    assert summary["primary_result"]["test_metrics"]["recall_at_top_10pct"] >= 0.0
