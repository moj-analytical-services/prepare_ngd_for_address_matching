from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent
from typing import Literal

import pytest
import requests

from ukam_os_builder.api.api import create_config_and_env, run_from_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n")


def test_create_config_and_env_writes_expected_files(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    created_config, created_env, env_written = create_config_and_env(
        config_out=config_path,
        env_out=env_path,
        source="ngd",
        package_id="16331",
        version_id="104444",
    )

    assert created_config == config_path.resolve()
    assert created_env == env_path.resolve()
    assert env_written is True

    config_text = config_path.read_text()
    assert "type: ngd" in config_text
    assert 'package_id: "16331"' in config_text
    assert 'version_id: "104444"' in config_text
    assert "num_chunks: 20" in config_text

    env_text = env_path.read_text()
    assert "OS_PROJECT_API_KEY=your_api_key_here" in env_text
    assert "OS_PROJECT_API_SECRET=your_api_secret_here" in env_text


def test_create_config_and_env_writes_supplied_api_credentials(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    create_config_and_env(
        config_out=config_path,
        env_out=env_path,
        source="ngd",
        package_id="16331",
        version_id="104444",
        api_key="my-key",
        api_secret="my-secret",
    )

    env_text = env_path.read_text()
    assert "OS_PROJECT_API_KEY=my-key" in env_text
    assert "OS_PROJECT_API_SECRET=my-secret" in env_text


def test_create_config_and_env_rejects_partial_api_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        create_config_and_env(
            config_out=tmp_path / "config.yaml",
            env_out=tmp_path / ".env",
            source="ngd",
            package_id="16331",
            version_id="104444",
            api_key="my-key",
        )


def test_run_from_config_applies_overrides(
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

        os_downloads:
          package_id: "16465"
          version_id: "104444"

        processing:
          num_chunks: 1
        """,
    )

    calls: dict[str, object] = {}

    def fake_check_api(_settings: object) -> None:
        calls["checked_api"] = True

    def fake_run_pipeline(
        step: Literal["all", "download"], settings: object, force: bool, list_only: bool
    ) -> None:
        calls["step"] = step
        calls["force"] = force
        calls["list_only"] = list_only
        calls["num_chunks"] = settings.processing.num_chunks

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", fake_check_api)
    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", fake_run_pipeline)

    run_from_config(
        config_path=config_path,
        step="download",
        list_only=True,
        force=True,
        num_chunks=5,
    )

    assert calls["checked_api"] is True
    assert calls["step"] == "download"
    assert calls["force"] is True
    assert calls["list_only"] is True
    assert calls["num_chunks"] == 5


def test_run_from_config_accepts_api_key_secret_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("OS_PROJECT_API_KEY", raising=False)
    monkeypatch.delenv("OS_PROJECT_API_SECRET", raising=False)

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", lambda _settings: None)
    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", lambda **_kwargs: None)

    run_from_config(
        config_path=config_path,
        api_key="runtime-key",
        api_secret="runtime-secret",
    )

    assert os.environ["OS_PROJECT_API_KEY"] == "runtime-key"
    assert os.environ["OS_PROJECT_API_SECRET"] == "runtime-secret"


def test_run_from_config_rejects_partial_api_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        run_from_config(
            config_path=tmp_path / "config.yaml",
            api_key="runtime-key",
        )


def test_run_from_config_validates_list_only_step(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="--list-only can only be used"):
        run_from_config(config_path=tmp_path / "config.yaml", step="extract", list_only=True)


def test_run_from_config_uses_source_override_for_pipeline_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
                source:
                    type: ngd

                os_downloads:
                    package_id: "16465"
                    version_id: "104444"

                processing:
                    num_chunks: 1
                """,
    )

    calls: dict[str, object] = {}

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", lambda _settings: None)

    def fake_run_pipeline(
        step: Literal["all", "download"], settings: object, force: bool, list_only: bool
    ) -> None:
        calls["step"] = step
        calls["source"] = settings.source.type
        calls["force"] = force
        calls["list_only"] = list_only

    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", fake_run_pipeline)

    run_from_config(
        config_path=config_path,
        step="split",
        source="abp",
        force=True,
        check_api=True,
    )

    assert calls["step"] == "split"
    assert calls["source"] == "abp"
    assert calls["force"] is True
    assert calls["list_only"] is False


def test_run_from_config_rejects_invalid_step_for_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
                source:
                    type: ngd

                os_downloads:
                    package_id: "16465"
                    version_id: "104444"
                """,
    )

    with pytest.raises(ValueError, match="--step split is not valid for source ngd"):
        run_from_config(
            config_path=config_path,
            step="split",
            check_api=False,
        )


def test_run_from_config_applies_schema_path_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    custom_schema = tmp_path / "custom_schema.yaml"
    custom_schema.write_text("header:\n  columns: {}\n", encoding="utf-8")

    _write_config(
        config_path,
        """
                source:
                    type: abp

                paths:
                    work_dir: ./data

                os_downloads:
                    package_id: "16465"
                    version_id: "104444"
                """,
    )

    calls: dict[str, object] = {}
    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", lambda _settings: None)

    def fake_run_pipeline(step: str, settings: object, force: bool, list_only: bool) -> None:
        calls["step"] = step
        calls["schema_path"] = settings.paths.schema_path

    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", fake_run_pipeline)

    run_from_config(
        config_path=config_path,
        step="split",
        source="abp",
        schema_path=custom_schema,
    )

    assert calls["step"] == "split"
    assert calls["schema_path"] == custom_schema.resolve()


def test_run_from_config_continues_when_api_preflight_is_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    calls: dict[str, object] = {}

    def fake_check_api(_settings: object) -> None:
        raise requests.exceptions.ConnectionError("offline")

    def fake_run_pipeline(step: str, settings: object, force: bool, list_only: bool) -> None:
        calls["step"] = step
        calls["list_only"] = list_only

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", fake_check_api)
    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", fake_run_pipeline)

    with caplog.at_level("WARNING"):
        run_from_config(config_path=config_path, step="all")

    assert calls["step"] == "all"
    assert calls["list_only"] is False
    assert "Could not reach OS Data Hub during API preflight" in caplog.text


def test_run_from_config_raises_when_list_only_api_preflight_is_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd

        os_downloads:
          package_id: "16465"
          version_id: "104444"
        """,
    )

    def fake_check_api(_settings: object) -> None:
        raise requests.exceptions.ConnectionError("offline")

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", fake_check_api)
    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", lambda **_kwargs: None)

    with pytest.raises(requests.exceptions.ConnectionError, match="offline"):
        run_from_config(config_path=config_path, step="download", list_only=True)
