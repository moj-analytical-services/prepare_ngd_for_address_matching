from __future__ import annotations

from pathlib import Path

from ukam_os_builder.os_builder.extract import (
    _filter_zips_for_source,
    _should_convert_csv_to_parquet,
)


def test_filter_zips_for_source_prefers_ngd_named_zips() -> None:
    zip_files = [
        Path("add_gb_builtaddress.zip"),
        Path("AddressBasePremium_FULL_2025-12-15_002.zip"),
    ]

    filtered = _filter_zips_for_source(zip_files, "ngd")

    assert filtered == [Path("add_gb_builtaddress.zip")]


def test_should_convert_csv_to_parquet_skips_non_ngd_for_ngd_source() -> None:
    ngd_csv = Path("add_gb_builtaddress.csv")
    abp_csv = Path("AddressBasePremium_FULL_2025-12-15_002.csv")

    assert _should_convert_csv_to_parquet(ngd_csv, "ngd") is True
    assert _should_convert_csv_to_parquet(abp_csv, "ngd") is False


def test_filter_zips_for_source_excludes_ngd_historicaddress() -> None:
    zip_files = [
        Path("add_gb_builtaddress.zip"),
        Path("add_gb_historicaddress.zip"),
        Path("add_gb_historicaddress_altadd.zip"),
        Path("add_gb_prebuildaddress.zip"),
    ]

    filtered = _filter_zips_for_source(zip_files, "ngd")

    assert Path("add_gb_builtaddress.zip") in filtered
    assert Path("add_gb_prebuildaddress.zip") in filtered
    assert Path("add_gb_historicaddress.zip") not in filtered
    assert Path("add_gb_historicaddress_altadd.zip") not in filtered


def test_should_convert_csv_to_parquet_skips_ngd_historicaddress() -> None:
    assert _should_convert_csv_to_parquet(Path("add_gb_builtaddress.csv"), "ngd") is True
    assert _should_convert_csv_to_parquet(Path("add_gb_historicaddress.csv"), "ngd") is False
    assert _should_convert_csv_to_parquet(Path("add_gb_historicaddress_altadd.csv"), "ngd") is False
