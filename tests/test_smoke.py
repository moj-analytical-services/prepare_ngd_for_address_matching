"""Smoke tests for NGD Pipeline.

These tests run the pipeline on real NGD sample data to verify
the basic functionality without downloading the full dataset.
"""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from ngd_pipeline.extract import convert_csv_to_parquet
from ngd_pipeline.settings import (
    OSDownloadSettings,
    PathSettings,
    ProcessingSettings,
    Settings,
    create_duckdb_connection,
)
from ngd_pipeline.to_flatfile import run_flatfile_step

# Path to sample data (real NGD data from a small area)
SAMPLE_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def temp_settings() -> Generator[Settings, None, None]:
    """Create settings pointing to a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create directories
        extracted_dir = tmpdir_path / "extracted"
        parquet_dir = extracted_dir / "parquet"
        output_dir = tmpdir_path / "output"

        extracted_dir.mkdir()
        parquet_dir.mkdir()
        output_dir.mkdir()

        paths = PathSettings(
            work_dir=tmpdir_path,
            downloads_dir=tmpdir_path / "downloads",
            extracted_dir=extracted_dir,
            output_dir=output_dir,
        )

        # Dummy OS download settings (not used in smoke test)
        os_downloads = OSDownloadSettings(
            package_id="test",
            version_id="test",
            api_key="test",
        )

        processing = ProcessingSettings(
            parquet_compression="zstd",
            parquet_compression_level=1,  # Faster for tests
            num_chunks=1,
        )

        settings = Settings(
            paths=paths,
            os_downloads=os_downloads,
            processing=processing,
            config_path=tmpdir_path / "config.yaml",
        )

        yield settings


@pytest.fixture
def temp_settings_chunked() -> Generator[Settings, None, None]:
    """Create settings with num_chunks=2 for chunking tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        extracted_dir = tmpdir_path / "extracted"
        parquet_dir = extracted_dir / "parquet"
        output_dir = tmpdir_path / "output"

        extracted_dir.mkdir()
        parquet_dir.mkdir()
        output_dir.mkdir()

        paths = PathSettings(
            work_dir=tmpdir_path,
            downloads_dir=tmpdir_path / "downloads",
            extracted_dir=extracted_dir,
            output_dir=output_dir,
        )

        os_downloads = OSDownloadSettings(
            package_id="test",
            version_id="test",
            api_key="test",
        )

        processing = ProcessingSettings(
            parquet_compression="zstd",
            parquet_compression_level=1,
            num_chunks=2,
        )

        settings = Settings(
            paths=paths,
            os_downloads=os_downloads,
            processing=processing,
            config_path=tmpdir_path / "config.yaml",
        )

        yield settings


def _prepare_test_parquet(settings: Settings) -> None:
    """Convert sample CSV files to parquet in the test directory."""
    parquet_dir = settings.paths.extracted_dir / "parquet"

    # Map sample files to expected parquet names (use actual file names)
    sample_files = [
        "add_gb_builtaddress.csv",
        "add_gb_builtaddress_altadd.csv",
        "add_gb_royalmailaddress.csv",
        "add_gb_prebuildaddress.csv",
        "add_gb_historicaddress.csv",
    ]

    for csv_name in sample_files:
        csv_path = SAMPLE_DATA_DIR / csv_name
        if csv_path.exists():
            parquet_name = csv_name.replace(".csv", ".parquet")
            parquet_path = parquet_dir / parquet_name
            convert_csv_to_parquet(csv_path, parquet_path, force=True)


def test_settings_creation(temp_settings: Settings) -> None:
    """Test that settings can be created with test values."""
    assert temp_settings.paths.work_dir.exists()
    assert temp_settings.processing.num_chunks == 1


def test_duckdb_connection(temp_settings: Settings) -> None:
    """Test that DuckDB connection can be created."""
    con = create_duckdb_connection(temp_settings)
    result = con.execute("SELECT 1 AS test").fetchone()
    assert result == (1,)
    con.close()


def test_flatfile_single_chunk(temp_settings: Settings) -> None:
    """Test flatfile generation with single chunk output."""
    # Prepare test data
    _prepare_test_parquet(temp_settings)

    # Run flatfile step
    output_files = run_flatfile_step(temp_settings, force=True)

    # Verify output
    assert len(output_files) == 1
    assert output_files[0].name == "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"
    assert output_files[0].exists()

    # Verify content
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT COUNT(*) as cnt FROM read_parquet('{output_files[0].as_posix()}')
    """).fetchone()
    assert result[0] > 0, "Output should contain records"

    # Check columns exist
    schema = con.execute(f"""
        DESCRIBE SELECT * FROM read_parquet('{output_files[0].as_posix()}')
    """).fetchall()
    column_names = [row[0] for row in schema]

    expected_columns = [
        "uprn",
        "address_concat",
        "filename",
        "classificationcode",
        "parentuprn",
        "rootuprn",
        "hierarchylevel",
        "floorlevel",
        "lowestfloorlevel",
        "highestfloorlevel",
    ]
    for col in expected_columns:
        assert col in column_names, f"Column {col} should exist in output"

    con.close()


def test_flatfile_multi_chunk(temp_settings_chunked: Settings) -> None:
    """Test flatfile generation with multiple chunk output."""
    # Prepare test data
    _prepare_test_parquet(temp_settings_chunked)

    # Run flatfile step
    output_files = run_flatfile_step(temp_settings_chunked, force=True)

    # Verify we have 2 chunks (or fewer if not enough data)
    assert len(output_files) >= 1
    assert len(output_files) <= 2

    for f in output_files:
        assert f.exists()
        assert "chunk_" in f.name


def test_flatfile_idempotent(temp_settings: Settings) -> None:
    """Test that flatfile step is idempotent (safe to re-run)."""
    # Prepare test data
    _prepare_test_parquet(temp_settings)

    # Run twice
    output1 = run_flatfile_step(temp_settings, force=True)
    output2 = run_flatfile_step(temp_settings, force=False)  # Should skip

    assert len(output1) == len(output2)
    assert output1[0].name == output2[0].name


def test_deduplication(temp_settings: Settings) -> None:
    """Test that deduplication removes duplicate UPRN+address combinations."""
    # Prepare test data
    _prepare_test_parquet(temp_settings)

    # Run flatfile step
    output_files = run_flatfile_step(temp_settings, force=True)

    # Verify no exact duplicates
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT uprn, address_concat, COUNT(*) as cnt
        FROM read_parquet('{output_files[0].as_posix()}')
        GROUP BY uprn, address_concat
        HAVING COUNT(*) > 1
    """).fetchall()

    assert len(result) == 0, f"Should have no duplicate UPRN+address: {result}"
    con.close()
