from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from pydantic import ValidationError
from rich.panel import Panel

from ngd_pipeline.settings import SettingsError


def _build_yaml_snippet_from_missing_paths(missing_paths: list[str]) -> str:
    """Build a minimal YAML snippet showing missing keys."""
    grouped: dict[str, list[str]] = defaultdict(list)
    for path in missing_paths:
        parts = path.split(".")
        if len(parts) >= 2:
            grouped[parts[0]].append(parts[1])

    if not grouped:
        return ""

    lines = ["Example config.yaml snippet:"]
    for section, keys in grouped.items():
        lines.append(f"{section}:")
        for key in sorted(set(keys)):
            lines.append(f'  {key}: "<required>"')
    return "\n".join(lines)


def format_pydantic_validation_error(
    exc: ValidationError,
    *,
    file_name: str = "config.yaml",
) -> str:
    """Format pydantic validation errors into concise, actionable text."""
    lines = [f"Invalid configuration in {file_name}:"]
    missing_paths: list[str] = []

    for err in exc.errors(include_url=False):
        path = ".".join(str(p) for p in err.get("loc", ()))
        msg = err.get("msg", "Invalid value")
        lines.append(f"• {path}: {msg}")
        if err.get("type") == "missing" and path:
            missing_paths.append(path)

    yaml_snippet = _build_yaml_snippet_from_missing_paths(missing_paths)
    if yaml_snippet:
        lines.append("")
        lines.append(yaml_snippet)

    lines.append("")
    lines.append("Fix:")
    lines.append("• Add or correct these values in config.yaml")
    lines.append("• Pass equivalent CLI overrides where available.")
    return "\n".join(lines)


def format_settings_error(exc: SettingsError, *, config_path: Path) -> str:
    """Format settings-related errors for user-facing CLI output."""
    if exc.validation_error is not None:
        return format_pydantic_validation_error(exc.validation_error, file_name=str(config_path))
    return str(exc)


def render_config_error_panel(message: str) -> Panel:
    """Render a Rich panel for configuration errors."""
    return Panel.fit(message, title="Configuration error", border_style="red")
