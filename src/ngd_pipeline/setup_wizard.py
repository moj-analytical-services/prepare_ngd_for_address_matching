"""Interactive setup wizard for NGD pipeline configuration."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

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


def _prompt_non_empty(label: str, default: str = "") -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip() or default
        if value:
            return value
        print("Value is required.")


def _prompt_optional(label: str, default: str = "") -> str | None:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    if value:
        return value
    return default or None


def _prompt_int(label: str, default: int) -> int:
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            print("Please enter a whole number.")
            continue
        if value < 1:
            print("Value must be >= 1.")
            continue
        return value


def _confirm(label: str, default_yes: bool = True) -> bool:
    default = "Y/n" if default_yes else "y/N"
    raw = input(f"{label} [{default}]: ").strip().lower()
    if not raw:
        return default_yes
    return raw in {"y", "yes"}


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


def _write_env_example(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# OS Data Hub API credentials\n"
        "OS_PROJECT_API_KEY=your_api_key_here\n"
        "OS_PROJECT_API_SECRET=your_api_secret_here\n",
        encoding="utf-8",
    )


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
        "--env-example-out",
        default=".env.example",
        help="Path to write .env example template (default: .env.example).",
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
    env_example_out = Path(args.env_example_out).resolve()

    config = _load_existing_defaults(config_out)

    if args.non_interactive:
        if not args.package_id or not args.version_id:
            parser.error("--package-id and --version-id are required with --non-interactive")

        config["os_downloads"]["package_id"] = args.package_id
        config["os_downloads"]["version_id"] = args.version_id
    else:
        print("NGD setup wizard")
        print("Mandatory settings:")
        config["os_downloads"]["package_id"] = _prompt_non_empty(
            "OS package_id",
            str(config["os_downloads"].get("package_id", "")),
        )
        config["os_downloads"]["version_id"] = _prompt_non_empty(
            "OS version_id",
            str(config["os_downloads"].get("version_id", "")),
        )

        print("\nPaths (press Enter to keep defaults):")
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
    with open(config_out, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)

    _write_env_example(env_example_out)

    print(f"Wrote config: {config_out}")
    print(f"Wrote env template: {env_example_out}")
    print("Create a .env file with OS_PROJECT_API_KEY and OS_PROJECT_API_SECRET before running.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
