from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ngd_pipeline.os_downloads import get_package_version
from ngd_pipeline.pipeline import run as run_pipeline
from ngd_pipeline.settings import Settings, SettingsError, load_settings

DEFAULT_CONFIG: dict[str, object] = {
    "paths": {
        "work_dir": "./data",
        "downloads_dir": "./data/downloads",
        "extracted_dir": "./data/extracted",
        "output_dir": "./data/output",
    },
    "os_downloads": {
        "package_id": "",
        "version_id": "",
    },
    "processing": {
        "parquet_compression": "zstd",
        "parquet_compression_level": 9,
        "num_chunks": 1,
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
        "# NGD Pipeline Configuration\n"
        "# All paths are relative to this config file's directory unless absolute\n\n"
        "paths:\n"
        "  # Base working directory for all data\n"
        f"  work_dir: {paths['work_dir']}\n\n"
        "  # Downloaded zip files from OS\n"
        f"  downloads_dir: {paths['downloads_dir']}\n\n"
        "  # Extracted CSV files and intermediate parquet\n"
        f"  extracted_dir: {paths['extracted_dir']}\n\n"
        "  # Final output parquet files\n"
        f"  output_dir: {paths['output_dir']}\n\n"
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
    merged["paths"] = {**DEFAULT_CONFIG["paths"], **(loaded.get("paths") or {})}
    merged["os_downloads"] = {
        **DEFAULT_CONFIG["os_downloads"],
        **(loaded.get("os_downloads") or {}),
    }
    merged["processing"] = {
        **DEFAULT_CONFIG["processing"],
        **(loaded.get("processing") or {}),
    }
    return merged


def write_env_file(path: Path, overwrite: bool = False) -> bool:
    """Write .env file with credential placeholders.

    Returns True if file was written, False if skipped.
    """
    if path.exists() and not overwrite:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# OS Data Hub API credentials\n"
        "OS_PROJECT_API_KEY=your_api_key_here\n"
        "OS_PROJECT_API_SECRET=your_api_secret_here\n",
        encoding="utf-8",
    )
    return True


def write_config_and_env(
    config: dict[str, object],
    config_out: str | Path,
    env_out: str | Path = ".env",
    *,
    overwrite_env: bool = False,
) -> tuple[Path, Path, bool]:
    """Write provided config plus .env template to disk."""
    config_out_path = Path(config_out).resolve()
    env_out_path = Path(env_out).resolve()

    config_out_path.parent.mkdir(parents=True, exist_ok=True)
    config_text = render_annotated_config(config)
    config_out_path.write_text(config_text, encoding="utf-8")
    env_written = write_env_file(env_out_path, overwrite=overwrite_env)

    return config_out_path, env_out_path, env_written


def create_config_and_env(
    config_out: str | Path,
    env_out: str | Path = ".env",
    *,
    package_id: str,
    version_id: str,
    overwrite_env: bool = False,
    paths: dict[str, str] | None = None,
    processing: dict[str, Any] | None = None,
) -> tuple[Path, Path, bool]:
    """Create config.yaml and .env template programmatically."""
    if not package_id or not package_id.strip():
        raise ValueError("package_id is required")
    if not version_id or not version_id.strip():
        raise ValueError("version_id is required")

    config_out_path = Path(config_out).resolve()
    config = load_existing_defaults(config_out_path)

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
    )


def apply_run_overrides(
    settings: Settings,
    *,
    package_id: str | None = None,
    version_id: str | None = None,
    work_dir: str | Path | None = None,
    downloads_dir: str | Path | None = None,
    extracted_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    num_chunks: int | None = None,
    duckdb_memory_limit: str | None = None,
    parquet_compression: str | None = None,
    parquet_compression_level: int | None = None,
) -> None:
    """Apply runtime overrides to loaded settings."""
    if package_id:
        settings.os_downloads.package_id = package_id
    if version_id:
        settings.os_downloads.version_id = version_id

    if work_dir:
        settings.paths.work_dir = Path(work_dir).resolve()
    if downloads_dir:
        settings.paths.downloads_dir = Path(downloads_dir).resolve()
    if extracted_dir:
        settings.paths.extracted_dir = Path(extracted_dir).resolve()
    if output_dir:
        settings.paths.output_dir = Path(output_dir).resolve()

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
    step: str = "all",
    env_file: str | Path | None = None,
    force: bool = False,
    list_only: bool = False,
    package_id: str | None = None,
    version_id: str | None = None,
    work_dir: str | Path | None = None,
    downloads_dir: str | Path | None = None,
    extracted_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    num_chunks: int | None = None,
    duckdb_memory_limit: str | None = None,
    parquet_compression: str | None = None,
    parquet_compression_level: int | None = None,
    check_api: bool = True,
) -> Settings:
    """Load settings from config, apply overrides, and run the pipeline."""
    if list_only and step != "download":
        raise ValueError("--list-only can only be used with --step download")

    settings = load_settings(Path(config_path).resolve(), load_env=True, env_path=env_file)

    apply_run_overrides(
        settings,
        package_id=package_id,
        version_id=version_id,
        work_dir=work_dir,
        downloads_dir=downloads_dir,
        extracted_dir=extracted_dir,
        output_dir=output_dir,
        num_chunks=num_chunks,
        duckdb_memory_limit=duckdb_memory_limit,
        parquet_compression=parquet_compression,
        parquet_compression_level=parquet_compression_level,
    )

    if check_api:
        get_package_version(settings)

    run_pipeline(step=step, settings=settings, force=force, list_only=list_only)
    return settings
