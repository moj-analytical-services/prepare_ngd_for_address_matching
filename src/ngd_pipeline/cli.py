from __future__ import annotations

import argparse
import logging
from pathlib import Path

from rich.console import Console

from ngd_pipeline.cli_errors import format_settings_error, render_config_error_panel
from ngd_pipeline.os_downloads import get_package_version
from ngd_pipeline.pipeline import run
from ngd_pipeline.settings import Settings, SettingsError, load_settings

logger = logging.getLogger(__name__)
console = Console()


def _configure_logging(verbose: bool) -> None:
    """Configure root logging for CLI runs."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _apply_overrides(settings: Settings, args: argparse.Namespace) -> None:
    """Apply CLI-provided config overrides to loaded settings."""
    if args.package_id:
        settings.os_downloads.package_id = args.package_id
    if args.version_id:
        settings.os_downloads.version_id = args.version_id

    if args.work_dir:
        settings.paths.work_dir = Path(args.work_dir).resolve()
    if args.downloads_dir:
        settings.paths.downloads_dir = Path(args.downloads_dir).resolve()
    if args.extracted_dir:
        settings.paths.extracted_dir = Path(args.extracted_dir).resolve()
    if args.output_dir:
        settings.paths.output_dir = Path(args.output_dir).resolve()

    if args.num_chunks is not None:
        if args.num_chunks < 1:
            raise SettingsError("--num-chunks must be >= 1")
        settings.processing.num_chunks = args.num_chunks

    if args.duckdb_memory_limit:
        settings.processing.duckdb_memory_limit = args.duckdb_memory_limit

    if args.parquet_compression:
        settings.processing.parquet_compression = args.parquet_compression

    if args.parquet_compression_level is not None:
        settings.processing.parquet_compression_level = args.parquet_compression_level


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ukam-ngd-build",
        description="Build NGD data for uk_address_matcher.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml).",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env file path (default: <config-dir>/.env).",
    )
    parser.add_argument(
        "--step",
        choices=["download", "extract", "flatfile", "all"],
        default="all",
        help="Pipeline step to run (default: all).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-run even if outputs already exist.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List available download files (only valid with --step download).",
    )

    parser.add_argument("--package-id", help="Override os_downloads.package_id.")
    parser.add_argument("--version-id", help="Override os_downloads.version_id.")

    parser.add_argument("--work-dir", help="Override paths.work_dir.")
    parser.add_argument("--downloads-dir", help="Override paths.downloads_dir.")
    parser.add_argument("--extracted-dir", help="Override paths.extracted_dir.")
    parser.add_argument("--output-dir", help="Override paths.output_dir.")

    parser.add_argument("--num-chunks", type=int, help="Override processing.num_chunks.")
    parser.add_argument(
        "--duckdb-memory-limit",
        help="Override processing.duckdb_memory_limit, e.g. 8GB.",
    )
    parser.add_argument(
        "--parquet-compression",
        help="Override processing.parquet_compression, e.g. zstd.",
    )
    parser.add_argument(
        "--parquet-compression-level",
        type=int,
        help="Override processing.parquet_compression_level.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for `ukam-ngd-build`."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list_only and args.step != "download":
        parser.error("--list-only can only be used with --step download")

    _configure_logging(args.verbose)

    try:
        console.rule("[bold cyan]NGD Builder[/bold cyan]")
        config_path = Path(args.config).resolve()
        settings = load_settings(config_path, load_env=True, env_path=args.env_file)
        _apply_overrides(settings, args)

        logger.info("Loaded config from %s", config_path)
        console.print(f"[green]✓[/green] Loaded config: [bold]{config_path}[/bold]")
        console.print(f"[cyan]Step:[/cyan] {args.step}")

        logger.info("Running OS API auth/connectivity check...")
        console.print("[cyan]Checking OS API credentials and connectivity...[/cyan]")
        get_package_version(settings)
        logger.info("API connectivity check passed")
        console.print("[green]✓[/green] API connectivity check passed")

        run(
            step=args.step,
            settings=settings,
            force=args.force,
            list_only=args.list_only,
        )
        console.print("[bold green]Build completed successfully[/bold green]")
        return 0
    except (SettingsError, ValueError) as exc:
        if isinstance(exc, SettingsError):
            error_config_path = exc.config_path or Path(args.config).resolve()
            message = format_settings_error(exc, config_path=error_config_path)
        else:
            message = str(exc)
        console.print(render_config_error_panel(message))
        logger.error("Configuration error")
        if args.verbose:
            logger.error("Configuration details: %s", message)
        return 2
    except Exception as exc:  # noqa: BLE001
        console.print(f"[bold red]Pipeline failed:[/bold red] {exc}")
        logger.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
