from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


class FeatureSchemaValidator:
    def from_frame(self, df: pd.DataFrame) -> dict[str, Any]:
        columns = [{"name": str(name), "dtype": str(dtype)} for name, dtype in df.dtypes.items()]
        serialized = json.dumps(columns, separators=(",", ":"), ensure_ascii=True)
        return {
            "columns": columns,
            "schema_hash": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
            "column_count": len(columns),
        }

    def validate(self, expected_schema: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        current = self.from_frame(df)
        expected_columns = {item["name"]: item["dtype"] for item in expected_schema.get("columns", [])}
        current_columns = {item["name"]: item["dtype"] for item in current.get("columns", [])}
        missing = sorted(set(expected_columns) - set(current_columns))
        extra = sorted(set(current_columns) - set(expected_columns))
        mismatched = sorted(
            column
            for column in set(expected_columns).intersection(current_columns)
            if expected_columns[column] != current_columns[column]
        )
        return {
            "is_valid": not missing and not mismatched,
            "missing_columns": missing,
            "extra_columns": extra,
            "mismatched_types": mismatched,
            "current_schema": current,
        }
