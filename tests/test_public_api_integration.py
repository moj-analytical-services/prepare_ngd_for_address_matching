from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import duckdb
import pytest

from ukam_os_builder import create_config_and_env, inspect_flatfile_variants, run_from_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")


def test_package_root_exports_expected_symbols() -> None:
    import ukam_os_builder as pkg

    assert hasattr(pkg, "create_config_and_env")
    assert hasattr(pkg, "run_from_config")
    assert hasattr(pkg, "inspect_flatfile_variants")


def test_package_root_create_and_run_from_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OS_PROJECT_API_KEY", "key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "secret")

    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    create_config_and_env(
        config_out=config_path,
        env_out=env_path,
        source="ngd",
        package_id="16465",
        version_id="104444",
    )

    calls: dict[str, object] = {}

    monkeypatch.setattr("ukam_os_builder.api.api.get_package_version", lambda _settings: None)

    def fake_run_pipeline(step: str, settings: object, force: bool, list_only: bool) -> None:
        calls["step"] = step
        calls["source"] = settings.source.type
        calls["list_only"] = list_only

    monkeypatch.setattr("ukam_os_builder.api.api.run_pipeline", fake_run_pipeline)

    run_from_config(config_path=config_path, step="download", list_only=True)

    assert calls["step"] == "download"
    assert calls["source"] == "ngd"
    assert calls["list_only"] is True


def test_package_root_inspect_flatfile_variants(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (4001::BIGINT, 'A'::VARCHAR),
                    (4001::BIGINT, 'B'::VARCHAR),
                    (4002::BIGINT, 'C'::VARCHAR)
            ) AS t(unique_id, address_concat)
        ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)
        """
    )

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd
        paths:
          output_dir: ./out
        """,
    )

    result = inspect_flatfile_variants(config_path=config_path, show=False)
    assert result["selected_uprn"] == 4001
    assert result["variant_count"] == 2
