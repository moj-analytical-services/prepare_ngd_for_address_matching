from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal

import yaml

from ukam_os_builder.api.settings import Settings, SettingsError, load_settings
from ukam_os_builder.os_builder.os_hub import get_package_version
from ukam_os_builder.pipeline import run as run_pipeline
from ukam_os_builder.pipeline import supported_steps_for_source

logger = logging.getLogger(__name__)

SourceType = Literal["ngd", "abp"]

DEFAULT_CONFIG: dict[str, object] = {
    "paths": {
        "work_dir": "./data",
        "overrides": {},
    },
    "source": {
        "type": "ngd",
    },
    "os_downloads": {
        "package_id": "",
        "version_id": "",
    },
    "processing": {
        "parquet_compression": "zstd",
        "parquet_compression_level": 9,
        "num_chunks": 20,
    },
}


def render_annotated_config(config: dict[str, object]) -> str:
    """Render config YAML with explanatory comments."""
    paths = config["paths"]
    os_downloads = config["os_downloads"]
    processing = config["processing"]

    duckdb_memory_limit = processing.get("duckdb_memory_limit")
    duckdb_memory_limit_line = (
        f'  duckdb_memory_limit: "{duckdb_memory_limit}"\n'
        if duckdb_memory_limit
        else '  # duckdb_memory_limit: "8GB"\n'
    )

    return (
        "# UKAM OS Builder Configuration\n"
        "# All paths are relative to this config file's directory unless absolute\n\n"
        "paths:\n"
        "  # Base working directory for all data\n"
        f"  work_dir: {paths['work_dir']}\n"
        "\n"
        "  # Most users won't need this: override derived directories only if required\n"
        "  # overrides:\n"
        "  #   downloads_dir: ./somewhere/downloads\n"
        "  #   extracted_dir: /mnt/fast/extracted\n"
        "  #   parquet_dir: ./data/parquet\n"
        "  #   output_dir: ./output\n\n"
        "source:\n"
        "  # Source dataset to process: ngd or abp\n"
        f"  type: {config['source']['type']}\n\n"
        "# OS Data Hub download settings\n"
        "# Data package and version IDs are mandatory and taken from OS Data Hub\n"
        "# API docs: https://api.os.uk/downloads/v1\n"
        "os_downloads:\n"
        "  # Data package ID from OS Data Hub\n"
        f'  package_id: "{os_downloads["package_id"]}"\n'
        "  # Version ID (update this when new data is released)\n"
        f'  version_id: "{os_downloads["version_id"]}"\n\n'
        "# Processing options\n"
        "processing:\n"
        "  # Parquet compression codec for intermediate/final files\n"
        f"  parquet_compression: {processing['parquet_compression']}\n"
        "  # Compression level (higher usually means smaller files but slower writes)\n"
        f"  parquet_compression_level: {processing['parquet_compression_level']}\n\n"
        "  # DuckDB memory limit (optional)\n"
        "  # If set, limits how much RAM DuckDB can use (e.g., '4GB', '500MB')\n"
        "  # If not set, DuckDB uses its default memory strategy\n"
        f"{duckdb_memory_limit_line}\n"
        "  # Number of chunks to split flatfile processing into (default: 1)\n"
        "  # Use higher values (e.g., 10-20) for lower memory usage\n"
        f"  num_chunks: {processing['num_chunks']}\n"
    )


def load_existing_defaults(config_path: Path) -> dict[str, object]:
    """Load existing config as defaults, merged with built-in defaults."""
    if not config_path.exists():
        return DEFAULT_CONFIG.copy()

    with open(config_path) as f:
        loaded = yaml.safe_load(f) or {}

    merged = DEFAULT_CONFIG | loaded
    loaded_paths = loaded.get("paths") if isinstance(loaded.get("paths"), dict) else {}
    merged["paths"] = {
        "work_dir": loaded_paths.get("work_dir", DEFAULT_CONFIG["paths"]["work_dir"]),
        "overrides": dict(loaded_paths.get("overrides") or {}),
    }
    merged["source"] = {**DEFAULT_CONFIG["source"], **(loaded.get("source") or {})}
    merged["os_downloads"] = {
        **DEFAULT_CONFIG["os_downloads"],
        **(loaded.get("os_downloads") or {}),
    }
    merged["processing"] = {
        **DEFAULT_CONFIG["processing"],
        **(loaded.get("processing") or {}),
    }
    return merged


def write_env_file(
    path: Path,
    overwrite: bool = False,
    api_key: str | None = None,
    api_secret: str | None = None,
) -> bool:
    """Write .env file with credential placeholders.

    Returns True if file was written, False if skipped.
    """
    if path.exists() and not overwrite:
        return False

    # If either API key or secret are not provided, error the pipeline
    if (api_key and not api_secret) or (api_secret and not api_key):
        raise ValueError("Both 'api_key' and 'api_secret' must be provided together.")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# OS Data Hub API credentials\n"
        f"OS_PROJECT_API_KEY={api_key or 'your_api_key_here'}\n"
        f"OS_PROJECT_API_SECRET={api_secret or 'your_api_secret_here'}\n",
        encoding="utf-8",
    )
    return True


def write_config_and_env(
    config: dict[str, object],
    config_out: str | Path,
    env_out: str | Path = ".env",
    *,
    overwrite_env: bool = False,
    write_env: bool = True,
    api_key: str | None = None,
    api_secret: str | None = None,
) -> tuple[Path, Path, bool]:
    """Write provided config plus .env template to disk."""
    config_out_path = Path(config_out).resolve()
    env_out_path = Path(env_out).resolve()

    config_out_path.parent.mkdir(parents=True, exist_ok=True)
    config_text = render_annotated_config(config)
    config_out_path.write_text(config_text, encoding="utf-8")
    env_written = False
    if write_env:
        env_written = write_env_file(
            env_out_path,
            overwrite=overwrite_env,
            api_key=api_key,
            api_secret=api_secret,
        )

    logger.info(f"Wrote config for '{config['source']['type']}' to {config_out_path}")
    if write_env and env_written:
        logger.info(f"Wrote .env template to {env_out_path}")
    elif write_env:
        logger.info(
            f".env file already exists at {env_out_path} and was not overwritten. "
            "Set overwrite_env=True to overwrite it.",
        )
    else:
        logger.info("Skipped writing .env file")

    return config_out_path, env_out_path, env_written


def create_config_and_env(
    config_out: str | Path,
    env_out: str | Path = ".env",
    *,
    package_id: str,
    version_id: str,
    source: SourceType,
    overwrite_env: bool = False,
    paths: dict[str, str] | None = None,
    processing: dict[str, Any] | None = None,
    api_key: str | None = None,
    api_secret: str | None = None,
) -> tuple[Path, Path, bool]:
    """Create config.yaml and .env template programmatically."""
    if not package_id or not package_id.strip():
        raise ValueError("package_id is required")
    if not version_id or not version_id.strip():
        raise ValueError("version_id is required")

    config_out_path = Path(config_out).resolve()
    config = load_existing_defaults(config_out_path)

    config["source"]["type"] = source
    config["os_downloads"]["package_id"] = package_id.strip()
    config["os_downloads"]["version_id"] = version_id.strip()

    if paths:
        config["paths"] = {**config["paths"], **paths}
    if processing:
        config["processing"] = {**config["processing"], **processing}

    return write_config_and_env(
        config=config,
        config_out=config_out_path,
        env_out=env_out,
        overwrite_env=overwrite_env,
        api_key=api_key,
        api_secret=api_secret,
    )


def apply_run_overrides(
    settings: Settings,
    *,
    source: SourceType | None = None,
    package_id: str | None = None,
    version_id: str | None = None,
    work_dir: str | Path | None = None,
    downloads_dir: str | Path | None = None,
    extracted_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    schema_path: str | Path | None = None,
    num_chunks: int | None = None,
    duckdb_memory_limit: str | None = None,
    parquet_compression: str | None = None,
    parquet_compression_level: int | None = None,
) -> None:
    """Apply runtime overrides to loaded settings."""
    if source:
        settings.source.type = source

    if package_id:
        settings.os_downloads.package_id = package_id
    if version_id:
        settings.os_downloads.version_id = version_id

    if work_dir:
        settings.paths.work_dir = Path(work_dir).resolve()
        settings.paths.downloads_dir = settings.paths.work_dir / "downloads"
        settings.paths.extracted_dir = settings.paths.work_dir / "extracted"
        settings.paths.parquet_dir = settings.paths.work_dir / "parquet"
        settings.paths.output_dir = settings.paths.work_dir / "output"
    if downloads_dir:
        settings.paths.downloads_dir = Path(downloads_dir).resolve()
    if extracted_dir:
        settings.paths.extracted_dir = Path(extracted_dir).resolve()
    if output_dir:
        settings.paths.output_dir = Path(output_dir).resolve()
    if schema_path:
        settings.paths.schema_path = Path(schema_path).resolve()

    if num_chunks is not None:
        if num_chunks < 1:
            raise SettingsError("--num-chunks must be >= 1")
        settings.processing.num_chunks = num_chunks

    if duckdb_memory_limit:
        settings.processing.duckdb_memory_limit = duckdb_memory_limit

    if parquet_compression:
        settings.processing.parquet_compression = parquet_compression

    if parquet_compression_level is not None:
        settings.processing.parquet_compression_level = parquet_compression_level


def run_from_config(
    config_path: str | Path,
    *,
    step: Literal["all", "download"] = "all",
    source: SourceType | None = None,
    env_file: str | Path | None = None,
    overwrite: bool | None = None,
    force: bool | None = None,
    list_only: bool = False,
    package_id: str | None = None,
    version_id: str | None = None,
    work_dir: str | Path | None = None,
    downloads_dir: str | Path | None = None,
    extracted_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    schema_path: str | Path | None = None,
    num_chunks: int | None = None,
    duckdb_memory_limit: str | None = None,
    parquet_compression: str | None = None,
    parquet_compression_level: int | None = None,
    api_key: str | None = None,
    api_secret: str | None = None,
    check_api: bool = True,
) -> Any:
    """Load settings from config, apply overrides, and run the pipeline."""
    if list_only and step not in {"download", "all"}:
        raise ValueError("--list-only can only be used with --step download or --step all")

    if (api_key and not api_secret) or (api_secret and not api_key):
        raise ValueError("Both '--api-key' and '--api-secret' must be provided together.")

    config_path = Path(config_path).resolve()

    if api_key and api_secret:
        os.environ["OS_PROJECT_API_KEY"] = api_key
        os.environ["OS_PROJECT_API_SECRET"] = api_secret

    settings = load_settings(config_path, load_env=True, env_path=env_file)

    apply_run_overrides(
        settings,
        source=source,
        package_id=package_id,
        version_id=version_id,
        work_dir=work_dir,
        downloads_dir=downloads_dir,
        extracted_dir=extracted_dir,
        output_dir=output_dir,
        schema_path=schema_path,
        num_chunks=num_chunks,
        duckdb_memory_limit=duckdb_memory_limit,
        parquet_compression=parquet_compression,
        parquet_compression_level=parquet_compression_level,
    )
    logger.info("Resolved work_dir: %s", settings.paths.work_dir)
    logger.info("Resolved downloads_dir: %s", settings.paths.downloads_dir)
    logger.info("Resolved extracted_dir: %s", settings.paths.extracted_dir)
    logger.info("Resolved parquet_dir: %s", settings.paths.parquet_dir)
    logger.info("Resolved output_dir: %s", settings.paths.output_dir)

    source_type = settings.source.type
    if step != "all":
        supported_steps = supported_steps_for_source(source_type)
        if step not in supported_steps:
            valid_steps = ", ".join([*sorted(supported_steps), "all"])
            raise ValueError(
                f"--step {step} is not valid for source {source_type}. Valid steps: {valid_steps}"
            )

    has_api_key = bool(os.environ.get("OS_PROJECT_API_KEY"))
    if check_api and has_api_key:
        get_package_version(settings)

    overwrite_effective = overwrite if overwrite is not None else bool(force)
    run_pipeline(step=step, settings=settings, force=overwrite_effective, list_only=list_only)
    return settings
