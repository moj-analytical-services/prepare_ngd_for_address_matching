from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import duckdb

from ukam_os_builder.os_builder.inspect_results import inspect_flatfile_variants


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")


def test_inspect_flatfile_variants_uses_config_defaults(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (1001::BIGINT, 'A'::VARCHAR),
                    (1001::BIGINT, 'B'::VARCHAR),
                    (1002::BIGINT, 'C'::VARCHAR)
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
    assert result["selected_uprn"] == 1001
    assert result["variant_count"] == 2


def test_inspect_flatfile_variants_supports_abp_pattern(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "abp_for_uk_address_matcher.chunk_001_of_001.parquet"

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (2001::BIGINT, 'A'::VARCHAR),
                    (2002::BIGINT, 'B'::VARCHAR),
                    (2002::BIGINT, 'C'::VARCHAR)
            ) AS t(unique_id, address_concat)
        ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)
        """
    )

    result = inspect_flatfile_variants(
        output_dir=output_dir,
        source="abp",
        target_uprn=2002,
        show=False,
    )
    assert result["selected_uprn"] == 2002
    assert result["variant_count"] == 2
