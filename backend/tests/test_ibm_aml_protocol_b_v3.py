from __future__ import annotations

import json
from pathlib import Path

from benchmarks.ibm_aml_protocol_b import extract_protocol_b_feature_csv_from_alert_jsonl
from benchmarks.ibm_aml_protocol_b_improvement import extract_protocol_b_extra_feature_csv_from_alert_jsonl
from benchmarks.ibm_aml_protocol_b_v3 import run_protocol_b_v3_benchmark


def _write_alert_jsonl(path: Path, alerts: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")
    return path


def _build_toy_alerts() -> list[dict]:
    alerts: list[dict] = []
    for index in range(120):
        account_group = index % 8
        base_day = (index % 28) + 1
        tx_count = 4 if index % 6 == 0 else 2 if index % 4 == 0 else 1
        transactions = []
        for offset in range(tx_count):
            suspicious = index % 6 == 0 and offset == tx_count - 1
            transactions.append(
                {
                    "transaction_id": f"T-{index}-{offset}",
                    "amount": float((index + 1) * (120 if suspicious else 20) * (offset + 1)),
                    "timestamp": f"2022-09-{base_day:02d}T{offset:02d}:00:00Z",
                    "receiver": f"{300 + ((index + offset) % 15)}:DST-{index}-{offset}",
                    "currency": "Euro" if suspicious and offset % 2 == 0 else "US Dollar",
                    "channel": "Wire" if suspicious else ("ACH" if offset % 2 == 0 else "Cash"),
                    "optional_fields": {
                        "from_bank": f"{100 + (account_group % 4)}",
                        "to_bank": f"{300 + ((index + offset) % 15)}",
                        "is_laundering": 1 if suspicious else 0,
                        "pattern_typology": "unknown",
                    },
                }
            )
        alerts.append(
            {
                "alert_id": f"A{index:04d}",
                "created_at": f"2022-09-{base_day:02d}T00:00:00Z",
                "source_account_key": f"{100 + (account_group % 4)}:SRC-{index // 2}",
                "transactions": transactions,
            }
        )
    return alerts


def test_run_protocol_b_v3_benchmark_produces_outputs(tmp_path: Path) -> None:
    alert_path = _write_alert_jsonl(tmp_path / "alerts.jsonl", _build_toy_alerts())
    base_feature_path, _ = extract_protocol_b_feature_csv_from_alert_jsonl(alert_path, tmp_path / "protocol_b.features.csv", force_rebuild=True)
    extra_feature_path, _ = extract_protocol_b_extra_feature_csv_from_alert_jsonl(alert_path, tmp_path / "protocol_b_v2.features.csv", force_rebuild=True)

    result = run_protocol_b_v3_benchmark(
        alert_jsonl_path=alert_path,
        base_feature_csv_path=base_feature_path,
        extra_feature_csv_path=extra_feature_path,
        horizon_feature_csv_path=tmp_path / "protocol_b_v3_horizon.features.csv",
        graph_feature_csv_path=tmp_path / "protocol_b_v3_graph.features.csv",
        sequence_feature_csv_path=tmp_path / "protocol_b_v3_sequence.features.csv",
        report_path=tmp_path / "benchmark_protocol_b_v3.md",
        summary_path=tmp_path / "benchmark_protocol_b_v3.json",
        force_rebuild_horizon_features=True,
        force_rebuild_graph_features=True,
        force_rebuild_sequence_features=True,
        include_lambdarank=False,
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert result.report_path.exists()
    assert result.summary_path.exists()
    assert summary["protocol_safety_claims"]["future_only_labels"] is True
    assert summary["champion"]["test_metrics"]["recall_at_top_10pct"] >= 0.0
