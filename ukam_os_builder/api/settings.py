from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal

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
    parquet_dir: Path | None = None
    schema_path: Path | None = None


class SourceSettings(StrictBaseModel):
    """Source selection for pipeline runtime."""

    type: Literal["ngd", "abp"] = "ngd"


class OSDownloadSettings(StrictBaseModel):
    """OS Data Hub download configuration."""

    package_id: str
    version_id: str
    api_key: SecretStr | None = None
    api_secret: SecretStr | None = None
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
        if value is None:
            return value
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
    num_chunks: int = 20

    @field_validator("num_chunks")
    @classmethod
    def _validate_num_chunks(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return value


class Settings(StrictBaseModel):
    """Complete application settings."""

    paths: PathSettings
    source: SourceSettings = SourceSettings()
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


def resolve_paths(config: dict[str, Any], config_dir: Path) -> dict[str, Path | None]:
    """Resolve all runtime paths from config using work_dir defaults and optional overrides."""
    paths_config = config.get("paths", {})
    if not isinstance(paths_config, dict):
        raise SettingsError("paths must be a mapping in config.yaml")

    work_dir_raw = str(paths_config.get("work_dir", "./data"))
    work_dir = _resolve_path(config_dir, work_dir_raw)

    defaults = {
        "downloads_dir": work_dir / "downloads",
        "extracted_dir": work_dir / "extracted",
        "parquet_dir": work_dir / "parquet",
        "output_dir": work_dir / "output",
    }

    raw_overrides = paths_config.get("overrides") or {}
    if not isinstance(raw_overrides, dict):
        raise SettingsError("paths.overrides must be a mapping in config.yaml")

    legacy_keys = ("downloads_dir", "extracted_dir", "parquet_dir", "output_dir")
    illegal_legacy = [key for key in legacy_keys if key in paths_config]
    if illegal_legacy:
        illegal_display = ", ".join(f"paths.{key}" for key in illegal_legacy)
        raise SettingsError(
            f"{illegal_display} are no longer supported. Use paths.overrides instead."
        )

    overrides: dict[str, Path] = {}
    for key in defaults:
        value = raw_overrides.get(key)
        if value is not None:
            overrides[key] = _resolve_path(config_dir, str(value))

    schema_path_value = paths_config.get("schema_path")

    return {
        "work_dir": work_dir,
        "downloads_dir": overrides.get("downloads_dir", defaults["downloads_dir"]),
        "extracted_dir": overrides.get("extracted_dir", defaults["extracted_dir"]),
        "parquet_dir": overrides.get("parquet_dir", defaults["parquet_dir"]),
        "output_dir": overrides.get("output_dir", defaults["output_dir"]),
        "schema_path": (
            _resolve_path(config_dir, str(schema_path_value))
            if schema_path_value is not None
            else None
        ),
    }


def _load_yaml(config_path: Path) -> dict[str, Any]:
    """Load YAML configuration file."""
    if not config_path.exists():
        raise SettingsError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise SettingsError(f"Invalid config file format: {config_path}")

    return config


def _load_env_vars() -> tuple[str | None, str | None]:
    """Load API credentials from environment variables if available."""
    api_key = os.environ.get("OS_PROJECT_API_KEY")
    api_secret = os.environ.get("OS_PROJECT_API_SECRET")

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
        SettingsError: If config file is missing or invalid.
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

    # Load environment variables (optional)
    api_key, api_secret = _load_env_vars()

    resolved_paths = resolve_paths(config=config, config_dir=base_dir)

    os_config = config.get("os_downloads", {})
    if not isinstance(os_config, dict):
        raise SettingsError("os_downloads must be a mapping in config.yaml")

    settings_payload = {
        **config,
        "paths": resolved_paths,
        "source": config.get("source", {}),
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
