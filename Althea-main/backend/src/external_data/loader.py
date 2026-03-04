"""
External risk data loader: file-based, versioned, hash-verified.
No live internet calls; all data from committed or dropped files.
Fails loudly if file missing or hash mismatch.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Registry: logical name -> filename (version in filename; change filename = new version)
EXTERNAL_SOURCE_FILES: Dict[str, str] = {
    "high_risk_countries": "high_risk_countries_v1.json",
    "sanctions": "sanctions_v1.json",
}

_DATA_DIR = Path(__file__).resolve().parent


class ExternalDataError(Exception):
    """Raised when external data cannot be loaded (missing file or hash mismatch)."""
    pass


def _sha256_raw(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _version_from_filename(filename: str) -> str:
    """Extract version from filename, e.g. high_risk_countries_v1.json -> v1."""
    stem = Path(filename).stem
    if "_v" in stem:
        return stem.split("_v")[-1]
    return "v1"


def load_external_source(
    name: str,
    base_path: Optional[Path] = None,
    expected_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load a single external source from file (JSON or CSV). Computes sha256 of raw file.

    Args:
        name: Logical source name (e.g. "high_risk_countries", "sanctions").
        base_path: Directory containing data files. Default: package directory.
        expected_hash: If set, fail with ExternalDataError when computed hash != expected_hash.

    Returns:
        {
            "source_name": str,
            "version": str,
            "as_of_date": str,
            "hash": str,
            "data": object  # parsed list/dict
        }

    Raises:
        ExternalDataError: If file is missing or hash mismatch (when expected_hash is set).
    """
    base = base_path or _DATA_DIR
    filename = EXTERNAL_SOURCE_FILES.get(name)
    if not filename:
        raise ExternalDataError(f"Unknown external source: {name}")
    path = base / filename
    if not path.is_file():
        raise ExternalDataError(f"External data file missing: {path}")

    raw = path.read_bytes()
    file_hash = _sha256_raw(raw)
    if expected_hash is not None and file_hash != expected_hash:
        raise ExternalDataError(
            f"Hash mismatch for {name}: expected {expected_hash}, got {file_hash}"
        )

    version = _version_from_filename(filename)
    as_of_date = ""
    if path.suffix.lower() == ".json":
        data = json.loads(raw.decode("utf-8"))
        if isinstance(data, dict):
            as_of_date = str(data.get("as_of_date", ""))
    else:
        # CSV: minimal parse for compatibility
        lines = raw.decode("utf-8").strip().splitlines()
        if not lines:
            data = []
        else:
            header = lines[0].split(",")
            data = [dict(zip(header, line.split(","))) for line in lines[1:] if line]
        as_of_date = ""

    return {
        "source_name": name,
        "version": version,
        "as_of_date": as_of_date,
        "hash": file_hash,
        "data": data,
    }


def load_all_configured_sources(
    base_path: Optional[Path] = None,
    expected_hashes: Optional[Dict[str, str]] = None,
    source_names: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Load all configured external sources (or a subset). Used once per pipeline run.

    Args:
        base_path: Directory for data files.
        expected_hashes: Optional dict source_name -> expected sha256 hash.
        source_names: If set, only load these (e.g. ["high_risk_countries", "sanctions"]).

    Returns:
        Dict mapping source_name -> load_external_source() result.
    """
    expected_hashes = expected_hashes or {}
    names = source_names or list(EXTERNAL_SOURCE_FILES.keys())
    # Dedupe and preserve order
    seen = set()
    ordered = []
    for n in names:
        if n in EXTERNAL_SOURCE_FILES and n not in seen:
            seen.add(n)
            ordered.append(n)
    result = {}
    for name in ordered:
        result[name] = load_external_source(
            name,
            base_path=base_path,
            expected_hash=expected_hashes.get(name),
        )
    return result
