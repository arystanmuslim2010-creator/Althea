# Add backend root to path for tests
import sys
import os
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

os.environ.setdefault("ALTHEA_JWT_SECRET", "test-only-jwt-secret-value-for-pytest-collection")


def pytest_collection_modifyitems(config, items):
    benchmark_tokens = (
        "benchmark",
        "ibm_aml",
        "protocol_b",
        "li_transfer",
        "sequence_model",
        "horizon_features",
        "graph_features",
        "event_sequence",
        "regime",
        "account_state",
    )
    import pytest

    for item in items:
        path = str(item.fspath).lower()
        name = item.name.lower()
        if any(token in path or token in name for token in benchmark_tokens):
            item.add_marker(pytest.mark.benchmark)
            item.add_marker(pytest.mark.slow)
