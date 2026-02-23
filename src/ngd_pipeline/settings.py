"""Settings module for NGD Pipeline.

Loads configuration from YAML file and environment variables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class PathSettings:
    """Paths for data directories."""

    work_dir: Path
    downloads_dir: Path
    extracted_dir: Path
    output_dir: Path


@dataclass
class OSDownloadSettings:
    """OS Data Hub download configuration."""

    package_id: str
    version_id: str
    api_key: str
    api_secret: str


@dataclass
class ProcessingSettings:
    """Data processing configuration."""

    parquet_compression: str = "zstd"
    parquet_compression_level: int = 9
    duckdb_memory_limit: str | None = None
    num_chunks: int = 1


@dataclass
class Settings:
    """Complete application settings."""

    paths: PathSettings
    os_downloads: OSDownloadSettings
    processing: ProcessingSettings
    config_path: Path


class SettingsError(Exception):
    """Error loading or validating settings."""


def _resolve_path(base_dir: Path, path_str: str) -> Path:
    """Resolve a path relative to the config file directory."""
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _load_yaml(config_path: Path) -> dict[str, Any]:
    """Load YAML configuration file."""
    if not config_path.exists():
        raise SettingsError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise SettingsError(f"Invalid config file format: {config_path}")

    return config


def _validate_env_vars() -> tuple[str, str]:
    """Validate required environment variables exist."""
    api_key = os.environ.get("OS_PROJECT_API_KEY")
    api_secret = os.environ.get("OS_PROJECT_API_SECRET")

    if not api_key:
        raise SettingsError(
            "OS_PROJECT_API_KEY not found in environment. "
            "Create a .env file with OS_PROJECT_API_KEY=<your-key>"
        )
    if not api_secret:
        raise SettingsError(
            "OS_PROJECT_API_SECRET not found in environment. "
            "Create a .env file with OS_PROJECT_API_SECRET=<your-secret>"
        )

    return api_key, api_secret


def load_settings(
    config_path: str | Path,
    load_env: bool = True,
    env_path: str | Path | None = None,
) -> Settings:
    """Load settings from YAML config file and environment variables.

    Args:
        config_path: Path to the YAML configuration file.
        load_env: Whether to load .env file (default True).

    Returns:
        Complete Settings object with resolved paths.

    Raises:
        SettingsError: If config file is missing or invalid,
                       or if required environment variables are not set.
    """
    config_path = Path(config_path).resolve()
    base_dir = config_path.parent

    # Load .env file from the same directory as config
    if load_env:
        env_file = Path(env_path).resolve() if env_path else (base_dir / ".env")
        load_dotenv(env_file)
        if env_file.exists():
            logger.debug("Loaded environment from %s", env_file)

    # Load YAML config
    config = _load_yaml(config_path)

    # Validate environment variables
    api_key, api_secret = _validate_env_vars()

    # Build path settings
    paths_config = config.get("paths", {})
    paths = PathSettings(
        work_dir=_resolve_path(base_dir, paths_config.get("work_dir", "./data")),
        downloads_dir=_resolve_path(
            base_dir, paths_config.get("downloads_dir", "./data/downloads")
        ),
        extracted_dir=_resolve_path(
            base_dir, paths_config.get("extracted_dir", "./data/extracted")
        ),
        output_dir=_resolve_path(base_dir, paths_config.get("output_dir", "./data/output")),
    )

    # Build OS download settings
    os_config = config.get("os_downloads", {})
    os_downloads = OSDownloadSettings(
        package_id=os_config.get("package_id", "16331"),
        version_id=os_config.get("version_id", "103792"),
        api_key=api_key,
        api_secret=api_secret,
    )

    # Build processing settings
    proc_config = config.get("processing", {})
    num_chunks = proc_config.get("num_chunks", 1)
    if num_chunks < 1:
        raise SettingsError(f"processing.num_chunks must be >= 1, got {num_chunks}")
    processing = ProcessingSettings(
        parquet_compression=proc_config.get("parquet_compression", "zstd"),
        parquet_compression_level=proc_config.get("parquet_compression_level", 9),
        duckdb_memory_limit=proc_config.get("duckdb_memory_limit"),
        num_chunks=num_chunks,
    )

    return Settings(
        paths=paths,
        os_downloads=os_downloads,
        processing=processing,
        config_path=config_path,
    )


def create_duckdb_connection(settings: Settings) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with optional memory limit applied.

    Args:
        settings: Settings object containing processing configuration.

    Returns:
        DuckDB connection with memory limit applied if configured.
    """
    con = duckdb.connect()

    # Apply memory limit if configured
    if settings.processing.duckdb_memory_limit:
        con.execute(f"SET memory_limit = '{settings.processing.duckdb_memory_limit}'")
        logger.info("Set DuckDB memory limit to %s", settings.processing.duckdb_memory_limit)

    return con
