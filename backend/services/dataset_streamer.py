from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pandas as pd


DEFAULT_PIPELINE_BATCH_SIZE = 20000


def stream_csv_dataset(path: Path, batch_size: int = DEFAULT_PIPELINE_BATCH_SIZE) -> Iterator[pd.DataFrame]:
    """
    Stream CSV files in bounded chunks so pipeline execution never loads full datasets into memory.
    """
    if not path.exists():
        raise ValueError(f"Dataset artifact is missing: {path}")
    safe_batch = max(1000, int(batch_size or DEFAULT_PIPELINE_BATCH_SIZE))
    for chunk in pd.read_csv(path, chunksize=safe_batch):
        if chunk is None or chunk.empty:
            continue
        yield chunk
