# Storage abstraction: base interface + SQLite adapter (default). Postgres skeleton optional.
from .base import StorageBase
from .sqlite_adapter import get_storage as get_sqlite_storage

__all__ = ["StorageBase", "get_sqlite_storage"]
