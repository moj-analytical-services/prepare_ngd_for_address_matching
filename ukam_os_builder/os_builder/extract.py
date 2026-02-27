from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path

import duckdb

from ukam_os_builder.api.settings import Settings

logger = logging.getLogger(__name__)

# NGD file stems to exclude (historic addresses are not used in output)
_NGD_EXCLUDED_STEMS = {"historicaddress"}


def find_downloaded_zips(downloads_dir: Path) -> list[Path]:
    """Find all downloaded zip files in a directory."""
    if not downloads_dir.exists():
        return []

    zip_files = list(downloads_dir.glob("*.zip"))
    zip_files.sort()
    return zip_files


def _is_excluded_ngd_file(name: str) -> bool:
    """Return True if *name* matches an excluded NGD stem (e.g. historicaddress)."""
    name_lower = name.lower()
    return any(stem in name_lower for stem in _NGD_EXCLUDED_STEMS)


def _filter_zips_for_source(zip_files: list[Path], source: str) -> list[Path]:
    source_lower = source.lower()
    if source_lower == "ngd":
        ngd_zips = [
            zip_path
            for zip_path in zip_files
            if zip_path.name.lower().startswith("add_gb_")
            and not _is_excluded_ngd_file(zip_path.name)
        ]
        return ngd_zips or zip_files
    if source_lower == "abp":
        abp_zips = [
            zip_path for zip_path in zip_files if "addressbasepremium" in zip_path.name.lower()
        ]
        return abp_zips or zip_files
    return zip_files


def _should_convert_csv_to_parquet(csv_path: Path, source: str) -> bool:
    if source.lower() == "ngd":
        name_lower = csv_path.name.lower()
        return name_lower.startswith("add_gb_") and not _is_excluded_ngd_file(name_lower)
    return True


def extract_zip_to_csv(
    zip_path: Path,
    extracted_dir: Path,
    force: bool = False,
) -> list[Path]:
    """Extract CSV files from a zip archive.

    Args:
        zip_path: Path to the zip file.
        extracted_dir: Directory to extract to.
        force: Force re-extraction even if files exist.

    Returns:
        List of paths to extracted CSV files.

    Raises:
        FileNotFoundError: If zip file doesn't exist.
        zipfile.BadZipFile: If zip file is corrupted.
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    # Create extraction subdirectory named after the zip file
    extract_subdir = extracted_dir / zip_path.stem

    # Check if already extracted
    existing_csvs = list(extract_subdir.rglob("*.csv")) if extract_subdir.exists() else []
    if existing_csvs and not force:
        logger.info("Already extracted %d CSV files from: %s", len(existing_csvs), zip_path.name)
        return existing_csvs

    # Clear existing directory on force
    if extract_subdir.exists() and force:
        logger.info("Removing existing extraction: %s", extract_subdir)
        shutil.rmtree(extract_subdir)

    # Extract only CSV files
    extract_subdir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting CSV files from %s to %s...", zip_path.name, extract_subdir)

    csv_paths: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            name = info.filename

            # Skip directory entries
            if name.endswith("/"):
                continue

            # Filter to only CSV files
            if not name.lower().endswith(".csv"):
                continue

            # Extract file
            out_path = extract_subdir / name
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with zf.open(info) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            csv_paths.append(out_path)

    logger.info("Extraction complete: %d CSV files", len(csv_paths))
    return csv_paths


def convert_csv_to_parquet(
    csv_path: Path,
    output_path: Path,
    force: bool = False,
) -> Path:
    """Convert a CSV file to parquet format.

    Args:
        csv_path: Path to the CSV file.
        output_path: Path for the output parquet file.
        force: Force reconversion even if file exists.

    Returns:
        Path to the output parquet file.
    """
    if output_path.exists() and not force:
        logger.debug("Parquet file already exists: %s", output_path.name)
        return output_path

    # Remove existing file to avoid DuckDB "File already exists" errors
    if output_path.exists():
        output_path.unlink()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.debug("Converting %s -> %s", csv_path.name, output_path.name)

    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv_auto('{csv_path.as_posix()}', sample_size=1000000)
        ) TO '{output_path.as_posix()}' (FORMAT 'PARQUET');
    """)
    con.close()

    return output_path


def discover_raw_csv_files(extracted_dir: Path) -> list[Path]:
    """Discover raw ABP CSV files in the extracted directory.

    The ABP data comes as multiple CSV files (chunks) that need to be
    processed together.

    Args:
        extracted_dir: Directory containing extracted files.

    Returns:
        List of paths to CSV files to process.
    """
    if not extracted_dir.exists():
        logger.warning("Extracted directory does not exist: %s", extracted_dir)
        return []

    # Find all CSV files recursively
    csv_files = list(extracted_dir.rglob("*.csv"))

    # Sort for deterministic ordering
    csv_files.sort()

    logger.info("Discovered %d CSV file(s) in %s", len(csv_files), extracted_dir)
    for f in csv_files[:5]:  # Log first few
        logger.debug("  %s", f.name)
    if len(csv_files) > 5:
        logger.debug("  ... and %d more", len(csv_files) - 5)

    return csv_files


def run_extract_step(
    settings: Settings, force: bool = False, convert_to_parquet: bool = True
) -> list[Path]:
    """Run the extract step of the pipeline.

    Extracts all downloaded zip files and converts CSVs to parquet.

    Args:
        settings: Application settings.
        force: Force re-extraction even if files exist.

    Returns:
        List of parquet file paths.
    """
    downloads_dir = settings.paths.downloads_dir
    extracted_dir = settings.paths.extracted_dir

    # Ensure directories exist
    extracted_dir.mkdir(parents=True, exist_ok=True)

    # Find downloaded zips
    zip_files = find_downloaded_zips(downloads_dir)
    if not zip_files:
        logger.warning("No zip files found in %s. Run --step download first.", downloads_dir)
        return []

    source_type = settings.source.type
    filtered_zip_files = _filter_zips_for_source(zip_files, source_type)
    if len(filtered_zip_files) != len(zip_files):
        logger.info(
            "Filtered %d zip file(s) for source '%s' (from %d total)",
            len(filtered_zip_files),
            source_type,
            len(zip_files),
        )
    zip_files = filtered_zip_files

    logger.info("Found %d zip file(s) to extract", len(zip_files))

    # Extract each zip and convert CSVs to parquet
    parquet_files: list[Path] = []
    extracted_csvs: list[Path] = []
    for zip_path in zip_files:
        csv_paths = extract_zip_to_csv(zip_path, extracted_dir, force=force)
        extracted_csvs.extend(csv_paths)

        if convert_to_parquet:
            # Convert each CSV to parquet
            parquet_dir = extracted_dir / "parquet"
            for csv_path in csv_paths:
                if not _should_convert_csv_to_parquet(csv_path, source_type):
                    logger.debug(
                        "Skipping CSV-to-parquet for source '%s': %s", source_type, csv_path
                    )
                    continue
                parquet_name = csv_path.stem + ".parquet"
                parquet_path = parquet_dir / parquet_name
                convert_csv_to_parquet(csv_path, parquet_path, force=force)
                parquet_files.append(parquet_path)

    logger.info("Extraction complete: %d parquet files", len(parquet_files))
    return parquet_files if convert_to_parquet else extracted_csvs


def get_parquet_dir(settings: Settings) -> Path:
    """Get the directory containing extracted parquet files.

    Args:
        settings: Application settings.

    Returns:
        Path to the parquet directory.
    """
    return settings.paths.extracted_dir / "parquet"
