from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ObjectStorage:
    """Local filesystem object storage abstraction compatible with S3-style keys."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def put_bytes(self, key: str, payload: bytes) -> str:
        self._path(key).write_bytes(payload)
        return key

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def put_json(self, key: str, payload: Any) -> str:
        self._path(key).write_text(json.dumps(payload, ensure_ascii=True, default=str), encoding="utf-8")
        return key

    def get_json(self, key: str) -> Any:
        return json.loads(self._path(key).read_text(encoding="utf-8"))

    def exists(self, key: str) -> bool:
        return self._path(key).exists()
