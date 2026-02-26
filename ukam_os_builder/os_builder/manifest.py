from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests

from ukam_os_builder.api.settings import Settings, _validate_env_vars


def _duckdb_columns(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> list[dict[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}')").fetchall()
    return [{"name": r[0], "type": r[1]} for r in rows]


def _duckdb_row_count(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0])


def _normalize_to_yyyy_mm_dd(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    # accept "2026-02-26" or "2026-02-26T..." and keep date
    m = re.match(r"(\d{4}-\d{2}-\d{2})", value)
    return m.group(1) if m else None


def _fetch_os_downloads_version_metadata(
    package_id: str,
    version_id: str,
    *,
    base_url: str = "https://api.os.uk/downloads/v1",
    timeout: tuple[int, int] = (30, 300),
) -> dict[str, Any]:
    api_key, api_secret = _validate_env_vars()  # validates both exist per your package

    url = f"{base_url}/dataPackages/{package_id}/versions/{version_id}"
    resp = requests.get(url, headers={"key": api_key}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _glob_matches(glob_pattern: str) -> list[Path]:
    pattern_path = Path(glob_pattern)
    if not pattern_path.parent.exists():
        return []
    return list(pattern_path.parent.glob(pattern_path.name))


def _describe_parquet_glob(
    con: duckdb.DuckDBPyConnection,
    glob_pattern: str | None,
) -> tuple[list[dict[str, str]] | None, int | None]:
    if not glob_pattern:
        return None, None

    matches = _glob_matches(glob_pattern)
    if not matches:
        return None, None

    return _duckdb_columns(con, glob_pattern), _duckdb_row_count(con, glob_pattern)


def _determine_output_glob(settings: Settings) -> str:
    source = settings.source.type.lower()
    prefix = f"{source}_for_uk_address_matcher"
    return str(settings.paths.output_dir / f"{prefix}.chunk_*.parquet")


def _determine_downloaded_glob(settings: Settings) -> str | None:
    source = settings.source.type.lower()
    if source == "ngd":
        return str(settings.paths.extracted_dir / "parquet" / "*.parquet")
    if source == "abp" and settings.paths.parquet_dir:
        return str(settings.paths.parquet_dir / "raw" / "*.parquet")
    return None


def generate_manifest_file(
    *,
    settings: Settings,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Generate a manifest.json describing the current pipeline output and metadata.

    Args:
        settings: Parsed settings used for the pipeline run.
        manifest_path: Optional destination path for the manifest JSON.
    """

    package_id = settings.os_downloads.package_id
    version_id = settings.os_downloads.version_id
    source_type = settings.source.type
    num_chunks = settings.processing.num_chunks

    output_dir = settings.paths.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = Path(manifest_path) if manifest_path else (output_dir / "manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    version_meta = _fetch_os_downloads_version_metadata(str(package_id), str(version_id))
    os_hub_last_updated = _normalize_to_yyyy_mm_dd(version_meta.get("createdOn"))

    extract_dt = datetime.now(timezone.utc).isoformat()
    output_glob = _determine_output_glob(settings)
    downloaded_glob = _determine_downloaded_glob(settings)

    with duckdb.connect(database=":memory:") as con:
        out_cols, out_rows = _describe_parquet_glob(con, output_glob)
        dl_cols, dl_rows = _describe_parquet_glob(con, downloaded_glob)

    manifest: dict[str, Any] = {
        "extract_datetime_utc": extract_dt,
        "source": {
            "type": source_type,
            "os_downloads": {
                "package_id": str(package_id),
                "version_id": str(version_id),
                "os_hub_last_updated": os_hub_last_updated,  # derived from version createdOn
                "productVersion": version_meta.get("productVersion"),
                "reason": version_meta.get("reason"),
                "supplyType": version_meta.get("supplyType"),
                "format": version_meta.get("format"),
                "downloads": version_meta.get("downloads", []) or [],
                # Optional traceability fields if present:
                "url": version_meta.get("url"),
                "dataPackageUrl": version_meta.get("dataPackageUrl"),
            },
        },
        "downloaded_dataset": {
            "parquet_glob": downloaded_glob,
            "row_count": dl_rows,
            "columns": dl_cols,
        },
        "output_dataset": {
            "parquet_glob": output_glob,
            "row_count": out_rows,
            "columns": out_cols,
        },
        "num_chunks": num_chunks,
    }

    manifest_path.write_text(json.dumps(manifest, indent=4), encoding="utf-8")
    return manifest
