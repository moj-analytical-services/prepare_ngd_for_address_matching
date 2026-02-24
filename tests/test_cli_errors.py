from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from ngd_pipeline.cli_errors import format_pydantic_validation_error
from ngd_pipeline.settings import Settings


def test_format_pydantic_validation_error_includes_yaml_snippet_for_missing_fields() -> None:
    payload = {
        "paths": {
            "work_dir": "/tmp/work",
            "downloads_dir": "/tmp/work/downloads",
            "extracted_dir": "/tmp/work/extracted",
            "output_dir": "/tmp/work/output",
        },
        "os_downloads": {
            "version_id": "104444",
            "api_key": "key",
            "api_secret": "secret",
        },
        "processing": {},
        "config_path": Path("/tmp/config.yaml"),
    }

    try:
        Settings.model_validate(payload)
    except ValidationError as exc:
        message = format_pydantic_validation_error(exc, file_name="/tmp/config.yaml")
    else:
        raise AssertionError("Expected ValidationError")

    assert "Invalid configuration in /tmp/config.yaml" in message
    assert "• os_downloads.package_id: Field required" in message
    assert "Example config.yaml snippet:" in message
    assert "os_downloads:" in message
    assert '  package_id: "<required>"' in message
