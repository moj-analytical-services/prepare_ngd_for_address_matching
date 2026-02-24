from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from ngd_pipeline.api import create_config_and_env, run_from_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n")


def test_create_config_and_env_writes_expected_files(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    created_config, created_env, env_written = create_config_and_env(
        config_out=config_path,
        env_out=env_path,
        package_id="16331",
        version_id="104444",
    )

    assert created_config == config_path.resolve()
    assert created_env == env_path.resolve()
    assert env_written is True

    config_text = config_path.read_text()
    assert 'package_id: "16331"' in config_text
    assert 'version_id: "104444"' in config_text

    env_text = env_path.read_text()
    assert "OS_PROJECT_API_KEY=your_api_key_here" in env_text
    assert "OS_PROJECT_API_SECRET=your_api_secret_here" in env_text


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
          downloads_dir: ./data/downloads
          extracted_dir: ./data/extracted
          output_dir: ./data/output

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

    def fake_run_pipeline(step: str, settings: object, force: bool, list_only: bool) -> None:
        calls["step"] = step
        calls["force"] = force
        calls["list_only"] = list_only
        calls["num_chunks"] = settings.processing.num_chunks

    monkeypatch.setattr("ngd_pipeline.api.get_package_version", fake_check_api)
    monkeypatch.setattr("ngd_pipeline.api.run_pipeline", fake_run_pipeline)

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


def test_run_from_config_validates_list_only_step(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="--list-only can only be used"):
        run_from_config(config_path=tmp_path / "config.yaml", step="extract", list_only=True)
