from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from rich.console import Console
from rich.panel import Panel

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

console = Console()


def _prompt_non_empty(label: str, default: str = "") -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = console.input(f"{label}{suffix}: ", markup=False).strip() or default
        if value:
            return value
        console.print("[red]Value is required.[/red]")


def _prompt_optional(label: str, default: str = "") -> str | None:
    suffix = f" [{default}]" if default else ""
    value = console.input(f"{label}{suffix}: ", markup=False).strip()
    if value:
        return value
    return default or None


def _prompt_int(label: str, default: int) -> int:
    while True:
        raw = console.input(f"{label} [{default}]: ", markup=False).strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            console.print("[red]Please enter a whole number.[/red]")
            continue
        if value < 1:
            console.print("[red]Value must be >= 1.[/red]")
            continue
        return value


def _confirm(label: str, default_yes: bool = True) -> bool:
    default = "Y/n" if default_yes else "y/N"
    raw = console.input(f"{label} [{default}]: ", markup=False).strip().lower()
    if not raw:
        return default_yes
    return raw in {"y", "yes"}


def _render_annotated_config(config: dict[str, object]) -> str:
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


def _load_existing_defaults(config_path: Path) -> dict[str, object]:
    if not config_path.exists():
        return DEFAULT_CONFIG

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


def _write_env_file(path: Path, overwrite: bool = False) -> bool:
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ukam-ngd-setup",
        description="Interactive setup wizard for NGD pipeline config.",
    )
    parser.add_argument(
        "--config-out",
        default="config.yaml",
        help="Path to write config YAML (default: config.yaml).",
    )
    parser.add_argument(
        "--env-out",
        default=".env",
        help="Path to write .env template (default: .env).",
    )
    parser.add_argument(
        "--overwrite-env",
        action="store_true",
        help="Overwrite .env output file if it already exists.",
    )
    parser.add_argument(
        "--env-example-out",
        dest="env_out",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Write config using defaults and any provided required flags.",
    )
    parser.add_argument("--package-id", help="OS package ID (required in non-interactive mode).")
    parser.add_argument("--version-id", help="OS version ID (required in non-interactive mode).")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for `ukam-ngd-setup`."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    config_out = Path(args.config_out).resolve()
    env_out = Path(args.env_out).resolve()

    config = _load_existing_defaults(config_out)

    if args.non_interactive:
        if not args.package_id or not args.version_id:
            parser.error("--package-id and --version-id are required with --non-interactive")

        config["os_downloads"]["package_id"] = args.package_id
        config["os_downloads"]["version_id"] = args.version_id
    else:
        console.print(
            Panel.fit(
                "[bold]NGD setup wizard[/bold]\nProvide required values first, then optional tuning.",
                border_style="cyan",
            )
        )
        console.print("[bold]Mandatory settings[/bold]")
        config["os_downloads"]["package_id"] = _prompt_non_empty(
            "OS package_id",
            "",
        )
        config["os_downloads"]["version_id"] = _prompt_non_empty(
            "OS version_id",
            "",
        )

        console.print("\n[bold]Paths[/bold] (press Enter to keep defaults)")
        config["paths"]["work_dir"] = _prompt_non_empty(
            "work_dir",
            str(config["paths"].get("work_dir", "./data")),
        )
        config["paths"]["downloads_dir"] = _prompt_non_empty(
            "downloads_dir",
            str(config["paths"].get("downloads_dir", "./data/downloads")),
        )
        config["paths"]["extracted_dir"] = _prompt_non_empty(
            "extracted_dir",
            str(config["paths"].get("extracted_dir", "./data/extracted")),
        )
        config["paths"]["output_dir"] = _prompt_non_empty(
            "output_dir",
            str(config["paths"].get("output_dir", "./data/output")),
        )

        if _confirm("Configure advanced processing settings?", default_yes=False):
            config["processing"]["num_chunks"] = _prompt_int(
                "num_chunks",
                int(config["processing"].get("num_chunks", 1)),
            )
            config["processing"]["parquet_compression"] = _prompt_non_empty(
                "parquet_compression",
                str(config["processing"].get("parquet_compression", "zstd")),
            )
            config["processing"]["parquet_compression_level"] = _prompt_int(
                "parquet_compression_level",
                int(config["processing"].get("parquet_compression_level", 9)),
            )
            memory_limit = _prompt_optional(
                "duckdb_memory_limit (optional, e.g. 8GB)",
                str(config["processing"].get("duckdb_memory_limit", "")),
            )
            if memory_limit:
                config["processing"]["duckdb_memory_limit"] = memory_limit
            elif "duckdb_memory_limit" in config["processing"]:
                del config["processing"]["duckdb_memory_limit"]

    config_out.parent.mkdir(parents=True, exist_ok=True)
    config_text = _render_annotated_config(config)
    config_out.write_text(config_text, encoding="utf-8")

    env_written = _write_env_file(env_out, overwrite=args.overwrite_env)

    console.print(f"[green]✓[/green] Wrote config: [bold]{config_out}[/bold]")
    if env_written:
        console.print(f"[green]✓[/green] Wrote .env template: [bold]{env_out}[/bold]")
    else:
        console.print(
            f"[yellow]•[/yellow] Kept existing .env file: [bold]{env_out}[/bold] "
            "(use --overwrite-env to replace)"
        )
    console.print(
        "[yellow]Next:[/yellow] add real values for OS_PROJECT_API_KEY and "
        "OS_PROJECT_API_SECRET in .env before running."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
