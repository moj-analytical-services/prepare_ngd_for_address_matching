from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
from pydantic import ValidationError

from ukam_os_builder.api.settings import SettingsError, load_settings


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n")


def test_load_settings_resolves_paths_relative_to_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data

        os_downloads:
          package_id: "16465"
          version_id: "104444"

        processing:
          num_chunks: 2
        """,
    )

    settings = load_settings(config_path, load_env=False)

    assert settings.paths.work_dir == (tmp_path / "data").resolve()
    assert settings.paths.downloads_dir == (tmp_path / "data/downloads").resolve()
    assert settings.processing.num_chunks == 2


def test_load_settings_rejects_unknown_config_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data

        os_downloads:
          package_id: "16465"
          version_id: "104444"
          extra_key: nope
        """,
    )

    with pytest.raises(SettingsError) as exc_info:
        load_settings(config_path, load_env=False)

    assert exc_info.value.validation_error is not None
    assert isinstance(exc_info.value.validation_error, ValidationError)


def test_load_settings_missing_package_id_has_clear_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data

        os_downloads:
          version_id: "104444"
        """,
    )

    with pytest.raises(SettingsError) as exc_info:
        load_settings(config_path, load_env=False)

    assert str(exc_info.value) == "Invalid configuration"
    assert exc_info.value.validation_error is not None


def test_load_settings_uses_work_dir_for_default_subpaths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./custom_data

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    settings = load_settings(config_path, load_env=False)

    assert settings.paths.work_dir == (tmp_path / "custom_data").resolve()
    assert settings.paths.downloads_dir == (tmp_path / "custom_data/downloads").resolve()
    assert settings.paths.extracted_dir == (tmp_path / "custom_data/extracted").resolve()
    assert settings.paths.parquet_dir == (tmp_path / "custom_data/parquet").resolve()
    assert settings.paths.output_dir == (tmp_path / "custom_data/output").resolve()


def test_load_settings_allows_missing_env_vars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OS_PROJECT_API_KEY", raising=False)
    monkeypatch.delenv("OS_PROJECT_API_SECRET", raising=False)

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    settings = load_settings(config_path, load_env=False)

    assert settings.os_downloads.api_key is None
    assert settings.os_downloads.api_secret is None


def test_load_settings_validates_positive_read_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data

        os_downloads:
          package_id: "16465"
          version_id: "104444"
          read_timeout_seconds: 0
        """,
    )

    with pytest.raises(SettingsError) as exc_info:
        load_settings(config_path, load_env=False)

    assert exc_info.value.validation_error is not None
    assert "read_timeout_seconds" in str(exc_info.value.validation_error)


def test_load_settings_defaults_source_and_num_chunks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    settings = load_settings(config_path, load_env=False)

    assert settings.source.type == "ngd"
    assert settings.processing.num_chunks == 20


def test_load_settings_applies_path_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data
          overrides:
            downloads_dir: ./custom/downloads
            extracted_dir: /tmp/extracted

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    settings = load_settings(config_path, load_env=False)

    assert settings.paths.work_dir == (tmp_path / "data").resolve()
    assert settings.paths.downloads_dir == (tmp_path / "custom/downloads").resolve()
    assert str(settings.paths.extracted_dir).endswith("/tmp/extracted")
    assert settings.paths.parquet_dir == (tmp_path / "data/parquet").resolve()
    assert settings.paths.output_dir == (tmp_path / "data/output").resolve()


def test_load_settings_rejects_legacy_path_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        paths:
          work_dir: ./data
          downloads_dir: ./legacy/downloads

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    with pytest.raises(SettingsError, match="no longer supported"):
        load_settings(config_path, load_env=False)
