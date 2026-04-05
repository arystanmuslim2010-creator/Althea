from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO


class ObjectStorage:
    """Local filesystem object storage abstraction compatible with S3-style keys."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        clean_key = str(key or "").strip().replace("\\", "/")
        if not clean_key:
            raise ValueError("Storage key must be non-empty.")

        pure = PurePosixPath(clean_key)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError("Invalid storage key path.")

        path = (self.root / Path(*pure.parts)).resolve()
        if path != self.root and self.root not in path.parents:
            raise ValueError("Storage key resolves outside storage root.")

        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def put_bytes(self, key: str, payload: bytes) -> str:
        self._path(key).write_bytes(payload)
        return key

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def resolve_path(self, key: str) -> Path:
        return self._path(key)

    def open_read(self, key: str) -> BinaryIO:
        return self._path(key).open("rb")

    def put_json(self, key: str, payload: Any) -> str:
        self._path(key).write_text(json.dumps(payload, ensure_ascii=True, default=str), encoding="utf-8")
        return key

    def get_json(self, key: str) -> Any:
        return json.loads(self._path(key).read_text(encoding="utf-8"))

    def exists(self, key: str) -> bool:
        return self._path(key).exists()
