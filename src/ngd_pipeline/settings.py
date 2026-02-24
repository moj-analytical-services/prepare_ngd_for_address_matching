from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import duckdb
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, SecretStr, ValidationError, field_validator

logger = logging.getLogger(__name__)


class StrictBaseModel(BaseModel):
    """Base model for strict settings parsing."""

    model_config = ConfigDict(extra="forbid")


class PathSettings(StrictBaseModel):
    """Paths for data directories."""

    work_dir: Path
    downloads_dir: Path
    extracted_dir: Path
    output_dir: Path


class OSDownloadSettings(StrictBaseModel):
    """OS Data Hub download configuration."""

    package_id: str
    version_id: str
    api_key: SecretStr
    api_secret: SecretStr
    connect_timeout_seconds: int = 30
    read_timeout_seconds: int = 300

    @field_validator("package_id", "version_id")
    @classmethod
    def _validate_non_empty_str(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must be non-empty")
        return stripped

    @field_validator("api_key", "api_secret", mode="before")
    @classmethod
    def _validate_secret(cls, value: Any) -> Any:
        if isinstance(value, str) and not value.strip():
            raise ValueError("must be non-empty")
        return value

    @field_validator("connect_timeout_seconds", "read_timeout_seconds")
    @classmethod
    def _validate_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be > 0")
        return value


class ProcessingSettings(StrictBaseModel):
    """Data processing configuration."""

    parquet_compression: str = "zstd"
    parquet_compression_level: int = 9
    duckdb_memory_limit: str | None = None
    num_chunks: int = 1

    @field_validator("num_chunks")
    @classmethod
    def _validate_num_chunks(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return value


class Settings(StrictBaseModel):
    """Complete application settings."""

    paths: PathSettings
    os_downloads: OSDownloadSettings
    processing: ProcessingSettings
    config_path: Path


class SettingsError(Exception):
    """Error loading or validating settings."""

    def __init__(
        self,
        message: str,
        *,
        validation_error: ValidationError | None = None,
        config_path: Path | None = None,
    ) -> None:
        super().__init__(message)
        self.validation_error = validation_error
        self.config_path = config_path


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

    paths_config = config.get("paths", {})
    if not isinstance(paths_config, dict):
        raise SettingsError("paths must be a mapping in config.yaml")

    work_dir_raw = str(paths_config.get("work_dir", "./data"))
    downloads_dir_raw = str(paths_config.get("downloads_dir", Path(work_dir_raw) / "downloads"))
    extracted_dir_raw = str(paths_config.get("extracted_dir", Path(work_dir_raw) / "extracted"))
    output_dir_raw = str(paths_config.get("output_dir", Path(work_dir_raw) / "output"))

    resolved_paths = {
        "work_dir": _resolve_path(base_dir, work_dir_raw),
        "downloads_dir": _resolve_path(base_dir, downloads_dir_raw),
        "extracted_dir": _resolve_path(base_dir, extracted_dir_raw),
        "output_dir": _resolve_path(base_dir, output_dir_raw),
    }

    os_config = config.get("os_downloads", {})
    if not isinstance(os_config, dict):
        raise SettingsError("os_downloads must be a mapping in config.yaml")

    settings_payload = {
        **config,
        "paths": resolved_paths,
        "os_downloads": {
            **os_config,
            "api_key": api_key,
            "api_secret": api_secret,
        },
        "processing": config.get("processing", {}),
        "config_path": config_path,
    }

    try:
        return Settings.model_validate(settings_payload)
    except ValidationError as exc:
        raise SettingsError(
            "Invalid configuration",
            validation_error=exc,
            config_path=config_path,
        ) from exc


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
