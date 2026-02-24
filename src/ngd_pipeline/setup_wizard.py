from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from ngd_pipeline.api import load_existing_defaults, write_config_and_env

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

    config = load_existing_defaults(config_out)

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

    config_out, env_out, env_written = write_config_and_env(
        config=config,
        config_out=config_out,
        env_out=env_out,
        overwrite_env=args.overwrite_env,
    )

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
