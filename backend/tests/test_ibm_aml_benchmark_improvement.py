from __future__ import annotations

import csv
import json
from pathlib import Path

from benchmarks.ibm_aml_improvement import (
    extract_feature_csv_from_alert_jsonl,
    extract_source_destination_feature_csv,
    load_feature_frame,
    run_improved_benchmark,
)


def _write_alert_jsonl(path: Path, alerts: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")
    return path


def _write_transactions_csv(path: Path, rows: list[str]) -> Path:
    header = (
        "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
        "Amount Paid,Payment Currency,Payment Format,Is Laundering\n"
    )
    path.write_text(header + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_extract_feature_csv_from_alert_jsonl_supports_6h_resplitting(tmp_path: Path) -> None:
    alert_path = tmp_path / "alerts.jsonl"
    alerts = [
        {
            "alert_id": "A1",
            "created_at": "2022-09-01T00:00:00Z",
            "source_account_key": "010:SRC1",
            "typology": "unknown",
            "evaluation_label_is_sar": 1,
            "transactions": [
                {
                    "transaction_id": "T1",
                    "amount": 100.0,
                    "timestamp": "2022-09-01T00:00:00Z",
                    "receiver": "011:DST1",
                    "currency": "US Dollar",
                    "channel": "ACH",
                    "optional_fields": {"from_bank": "010", "to_bank": "011", "is_laundering": 0},
                },
                {
                    "transaction_id": "T2",
                    "amount": 200.0,
                    "timestamp": "2022-09-01T02:00:00Z",
                    "receiver": "011:DST2",
                    "currency": "US Dollar",
                    "channel": "ACH",
                    "optional_fields": {"from_bank": "010", "to_bank": "011", "is_laundering": 1},
                },
                {
                    "transaction_id": "T3",
                    "amount": 300.0,
                    "timestamp": "2022-09-01T07:30:00Z",
                    "receiver": "011:DST3",
                    "currency": "US Dollar",
                    "channel": "Wire",
                    "optional_fields": {"from_bank": "010", "to_bank": "011", "is_laundering": 0},
                },
            ],
        }
    ]
    _write_alert_jsonl(alert_path, alerts)

    feature_path = extract_feature_csv_from_alert_jsonl(
        alert_path,
        tmp_path / "source_account_6h.features.csv",
        grouping_variant="source_account_6h",
    )
    frame = load_feature_frame(feature_path)

    assert len(frame) == 2
    assert set(frame["transaction_count"].tolist()) == {1, 2}
    assert frame["grouping_variant"].nunique() == 1
    assert frame["grouping_variant"].iloc[0] == "source_account_6h"


def test_extract_source_destination_feature_csv_groups_by_source_and_destination(tmp_path: Path) -> None:
    transactions_path = _write_transactions_csv(
        tmp_path / "HI-Small_Trans.csv",
        [
            "2022/09/01 00:00,010,SRC1,011,DST1,100.00,US Dollar,100.00,US Dollar,ACH,0",
            "2022/09/01 01:00,010,SRC1,011,DST1,200.00,US Dollar,200.00,US Dollar,ACH,1",
            "2022/09/01 02:00,010,SRC1,012,DST2,300.00,US Dollar,300.00,US Dollar,Wire,0",
        ],
    )
    patterns_path = tmp_path / "HI-Small_Patterns.txt"
    patterns_path.write_text(
        "\n".join(
            [
                "BEGIN LAUNDERING ATTEMPT - FAN-OUT",
                "2022/09/01 01:00,010,SRC1,011,DST1,200.00,US Dollar,200.00,US Dollar,ACH,1",
                "END LAUNDERING ATTEMPT - FAN-OUT",
            ]
        ),
        encoding="utf-8",
    )

    feature_path = extract_source_destination_feature_csv(
        transactions_path=transactions_path,
        patterns_path=patterns_path,
        output_csv_path=tmp_path / "source_destination_24h.features.csv",
    )
    frame = load_feature_frame(feature_path)

    assert len(frame) == 2
    assert set(frame["grouping_variant"].tolist()) == {"source_destination_24h"}
    assert sorted(frame["transaction_count"].tolist()) == [1, 2]


def test_run_improved_benchmark_generates_baselines_models_and_report(tmp_path: Path) -> None:
    alert_path = tmp_path / "alerts.jsonl"
    alerts: list[dict] = []
    for index in range(60):
        is_positive = 1 if index % 5 == 0 else 0
        tx_count = 3 if is_positive else 1
        transactions = []
        for offset in range(tx_count):
            transactions.append(
                {
                    "transaction_id": f"T-{index}-{offset}",
                    "amount": float((index + 1) * (200 if is_positive else 20) * (offset + 1)),
                    "timestamp": f"2022-09-{(index % 28) + 1:02d}T0{offset}:00:00Z",
                    "receiver": f"{200 + offset}:DST-{index}-{offset}",
                    "currency": "US Dollar" if offset % 2 == 0 else "Euro",
                    "channel": "Wire" if is_positive else "ACH",
                    "optional_fields": {
                        "from_bank": f"{100 + (index % 4)}",
                        "to_bank": f"{200 + offset}",
                        "is_laundering": is_positive if offset == 0 else 0,
                    },
                }
            )
        alerts.append(
            {
                "alert_id": f"A{index:04d}",
                "created_at": f"2022-09-{(index % 28) + 1:02d}T00:00:00Z",
                "source_account_key": f"{100 + (index % 4)}:SRC-{index // 3}",
                "typology": "unknown",
                "evaluation_label_is_sar": is_positive,
                "transactions": transactions,
            }
        )
    _write_alert_jsonl(alert_path, alerts)

    result = run_improved_benchmark(
        alert_jsonl_path=alert_path,
        report_path=tmp_path / "benchmark_v2.md",
        summary_path=tmp_path / "benchmark_v2.json",
        feature_cache_dir=tmp_path / "feature_cache",
        include_grouping_variants=False,
        include_althea_diagnosis=False,
        force_rebuild_features=True,
    )

    baseline_names = {row["name"] for row in result.baseline_results}
    model_names = {row["name"] for row in result.model_results}
    assert "weighted_signal_heuristic" in baseline_names
    assert "amount_descending" in baseline_names
    assert "logistic_regression_raw_signals" in model_names
    assert result.summary_path.exists()
    assert result.report_path.exists()
    assert result.champion["name"] in baseline_names | model_names
