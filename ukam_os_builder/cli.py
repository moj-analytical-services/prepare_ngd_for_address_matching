from __future__ import annotations

import argparse
import logging
from pathlib import Path

from rich.console import Console

from ukam_os_builder.api.api import run_from_config
from ukam_os_builder.api.cli_errors import format_settings_error, render_config_error_panel
from ukam_os_builder.api.settings import SettingsError

logger = logging.getLogger(__name__)
console = Console()


def _configure_logging(verbose: bool) -> None:
    """Configure root logging for CLI runs."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ukam-os-build",
        description="Build OS address data for uk_address_matcher.",
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
        choices=["download", "extract", "split", "flatfile", "all"],
        default="all",
        help="Pipeline step to run (default: all).",
    )
    parser.add_argument(
        "--source",
        choices=["ngd", "abp"],
        default=None,
        help="Override source.type from config (ngd or abp).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite step outputs and re-run even if outputs already exist.",
    )
    parser.add_argument(
        "--force",
        dest="overwrite",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List available download files (only valid with --step download or --step all).",
    )

    parser.add_argument("--package-id", help="Override os_downloads.package_id.")
    parser.add_argument("--version-id", help="Override os_downloads.version_id.")
    parser.add_argument(
        "--api-key",
        help="Override OS_PROJECT_API_KEY for this run.",
    )
    parser.add_argument(
        "--api-secret",
        help="Override OS_PROJECT_API_SECRET for this run.",
    )

    parser.add_argument("--work-dir", help="Override paths.work_dir.")
    parser.add_argument("--downloads-dir", help="Override paths.downloads_dir.")
    parser.add_argument("--extracted-dir", help="Override paths.extracted_dir.")
    parser.add_argument("--output-dir", help="Override paths.output_dir.")
    parser.add_argument("--schema-path", help="Override paths.schema_path (ABP split schema).")

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
    """Entry point for `ukam-os-build`."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list_only and args.step not in {"download", "all"}:
        parser.error("--list-only can only be used with --step download or --step all")

    _configure_logging(args.verbose)

    try:
        console.rule("[bold cyan]OS Builder[/bold cyan]")
        config_path = Path(args.config).resolve()
        console.print(f"[green]✓[/green] Loaded config: [bold]{config_path}[/bold]")
        console.print(f"[cyan]Step:[/cyan] {args.step}")
        console.print("[cyan]Starting pipeline...[/cyan]")

        run_from_config(
            config_path=config_path,
            step=args.step,
            source=args.source,
            env_file=args.env_file,
            overwrite=args.overwrite,
            list_only=args.list_only,
            api_key=args.api_key,
            api_secret=args.api_secret,
            package_id=args.package_id,
            version_id=args.version_id,
            work_dir=args.work_dir,
            downloads_dir=args.downloads_dir,
            extracted_dir=args.extracted_dir,
            output_dir=args.output_dir,
            schema_path=args.schema_path,
            num_chunks=args.num_chunks,
            duckdb_memory_limit=args.duckdb_memory_limit,
            parquet_compression=args.parquet_compression,
            parquet_compression_level=args.parquet_compression_level,
        )
        logger.info("Pipeline run completed")
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
