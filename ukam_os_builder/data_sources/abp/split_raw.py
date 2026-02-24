"""Split raw ABP data module.

Reads raw ABP CSV files (which contain all record types mixed together),
splits them by record identifier (10/11/15/21/24/28/31/32/99 etc.),
and writes one parquet file per record type.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

import yaml

from ukam_os_builder.api.settings import Settings, create_duckdb_connection

logger = logging.getLogger(__name__)

# Record identifier to table name mapping
RECORD_TYPE_MAP = {
    "10": "header",
    "11": "street",
    "15": "street_descriptor",
    "21": "blpu",
    "23": "application_cross_reference",
    "24": "lpi",
    "28": "delivery_point",
    "29": "metadata",
    "30": "successor",
    "31": "organisation",
    "32": "classification",
    "99": "trailer",
}

DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "abp_schema.yaml"


def load_schema(schema_path: Path) -> dict[str, Any]:
    """Load ABP schema definitions from YAML file.

    Args:
        schema_path: Path to the schema YAML file.

    Returns:
        Schema dictionary with table definitions.
    """
    with open(schema_path) as f:
        return yaml.safe_load(f)


def _get_column_types(schema: dict[str, Any], table_name: str) -> dict[str, str]:
    """Extract column types from schema for a given table.

    Args:
        schema: Full schema dictionary.
        table_name: Name of the table.

    Returns:
        Dictionary mapping column names to DuckDB types.
    """
    table_schema = schema.get(table_name, {})
    columns = table_schema.get("columns", {})

    return {col: info.get("type", "VARCHAR") for col, info in columns.items()}


def _resolve_schema_path(settings: Settings) -> Path:
    schema_path = getattr(settings.paths, "schema_path", None)
    if schema_path is None:
        return DEFAULT_SCHEMA_PATH
    return Path(schema_path)


def split_raw_to_parquet(
    settings: Settings,
    input_dir: Path | None = None,
    force: bool = False,
) -> dict[str, Path]:
    """Split raw ABP CSV files into separate parquet files by record type.

    Reads all CSV files in the input directory, splits by record identifier,
    applies schema-based typing, and writes parquet files.

    Args:
        settings: Application settings.
        input_dir: Directory containing raw CSV files. If None, auto-detected.
        force: Force re-processing even if output exists.

    Returns:
        Dictionary mapping table names to output parquet paths.

    Raises:
        FileNotFoundError: If input directory or schema not found.
        ValueError: If line count validation fails.
    """
    # Determine input directory
    if input_dir is None:
        input_dir = settings.paths.extracted_dir
        if not list(input_dir.rglob("*.csv")):
            raise FileNotFoundError("Could not find raw CSV files. Run --step extract first.")

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    # Load schema
    schema_path = _resolve_schema_path(settings)
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    schema = load_schema(schema_path)

    # Setup output directory
    parquet_dir = settings.paths.parquet_dir / "raw"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Check if outputs already exist
    expected_outputs = {name: parquet_dir / f"{name}.parquet" for name in RECORD_TYPE_MAP.values()}
    if not force and all(p.exists() for p in expected_outputs.values()):
        logger.info("All output parquet files already exist. Use --force to re-process.")
        return expected_outputs

    # Compression settings
    compression = settings.processing.parquet_compression
    compression_level = settings.processing.parquet_compression_level

    logger.info("Splitting raw CSV files from %s", input_dir)
    logger.info("Output directory: %s", parquet_dir)

    # Create temp directory for intermediate CSVs
    tmp_dir = Path(tempfile.mkdtemp(prefix="abp_split_"))

    try:
        # Create DuckDB connection
        con = create_duckdb_connection(settings)

        # 1) Ingest all CSV files as single-column 'line' records
        # Use Unit Separator (ASCII 31) as delimiter to read entire lines
        # Use **/*.csv to find CSVs in subdirectories (extract creates subdirs per zip)
        csv_glob = (input_dir / "**/*.csv").as_posix()
        con.execute(f"""
            CREATE TEMP VIEW lines AS
            SELECT regexp_replace(line, '^\ufeff', '') AS line
            FROM read_csv('{csv_glob}',
                          auto_detect=false,
                          header=false,
                          columns={{'line': 'VARCHAR'}},
                          delim='{chr(31)}',
                          quote='',
                          escape='')
        """)

        # 2) Extract record identifier (first 2 characters)
        con.execute("""
            CREATE TEMP VIEW lines_with_rid AS
            SELECT
                line,
                trim(both '"' from split_part(line, ',', 1)) AS rid
            FROM lines
        """)

        # 3) Count input lines for validation
        input_counts: dict[str, int] = {}
        for rid, name in RECORD_TYPE_MAP.items():
            count = con.execute(f"""
                SELECT COUNT(*) FROM lines_with_rid WHERE rid = '{rid}'
            """).fetchone()[0]
            input_counts[name] = count
            logger.debug("Record type %s (%s): %d lines", rid, name, count)

        total_input = sum(input_counts.values())
        logger.info("Total input lines (with valid record IDs): %d", total_input)
        if total_input == 0:
            raise ValueError(
                "No ABP record identifiers found in extracted CSV input. "
                "Ensure --source abp is used with ABP raw extracts (record IDs 10/11/15/21/24/28/31/32/99)."
            )

        # 4) Process each record type
        output_paths: dict[str, Path] = {}
        output_counts: dict[str, int] = {}

        for rid, name in RECORD_TYPE_MAP.items():
            parquet_path = parquet_dir / f"{name}.parquet"
            output_paths[name] = parquet_path

            # Remove existing output if force
            if parquet_path.exists() and force:
                parquet_path.unlink()

            # Skip if no records of this type - but still create empty parquet
            if input_counts[name] == 0:
                logger.debug("Skipping %s (no records)", name)
                output_counts[name] = 0

                # Create empty parquet with schema for idempotency
                col_types = _get_column_types(schema, name)
                if col_types and not parquet_path.exists():
                    con.execute(f"""
                        COPY (SELECT * FROM (SELECT {", ".join([f"NULL::{dtype} AS {col}" for col, dtype in col_types.items()])} WHERE false))
                        TO '{parquet_path.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION '{compression}', COMPRESSION_LEVEL {compression_level})
                    """)
                continue

            # Write filtered lines to temp CSV
            tmp_csv = tmp_dir / f"{name}.csv"
            con.execute(f"""
                COPY (
                    SELECT line FROM lines_with_rid WHERE rid = '{rid}'
                )
                TO '{tmp_csv.as_posix()}' (FORMAT CSV, HEADER false, QUOTE '');
            """)

            # Get schema for this table
            col_types = _get_column_types(schema, name)

            if col_types:
                # Build column specification
                col_spec = ", ".join([f"'{col}': '{dtype}'" for col, dtype in col_types.items()])

                # Read temp CSV with proper schema
                con.execute(f"""
                    CREATE OR REPLACE TEMP VIEW typed_{name} AS
                    SELECT * FROM read_csv(
                        '{tmp_csv.as_posix()}',
                        auto_detect=false,
                        delim=',',
                        quote='"',
                        escape='"',
                        header=false,
                        strict_mode=false,
                        null_padding=true,
                        nullstr='',
                        dateformat='%Y-%m-%d',
                        timestampformat='%Y-%m-%d %H:%M:%S',
                        columns={{ {col_spec} }}
                    )
                """)
            else:
                # No schema - use auto-detect (shouldn't happen for ABP)
                logger.warning("No schema for %s, using auto-detect", name)
                con.execute(f"""
                    CREATE OR REPLACE TEMP VIEW typed_{name} AS
                    SELECT * FROM read_csv(
                        '{tmp_csv.as_posix()}',
                        auto_detect=true,
                        delim=',',
                        quote='"',
                        escape='"',
                        header=false,
                        strict_mode=false,
                        null_padding=true,
                        nullstr='',
                        dateformat='%Y-%m-%d',
                        timestampformat='%Y-%m-%d %H:%M:%S',
                        ignore_errors=true
                    )
                """)

            # Write parquet
            con.execute(f"""
                COPY typed_{name} TO '{parquet_path.as_posix()}'
                (FORMAT PARQUET, COMPRESSION '{compression}', COMPRESSION_LEVEL {compression_level})
            """)

            # Remove temp CSV
            tmp_csv.unlink(missing_ok=True)

            # Count output rows
            output_count = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')
            """).fetchone()[0]
            output_counts[name] = output_count
            logger.info("Wrote %s: %d rows", name, output_count)

        # 5) Validation
        total_output = sum(output_counts.values())
        logger.info("")
        logger.info("=== Validation: Line count check ===")
        logger.info("Input lines (with valid record IDs): %d", total_input)
        logger.info("Output rows (parquet): %d", total_output)

        if total_input == total_output:
            logger.info("✅ Line counts match!")
        else:
            diff = total_output - total_input
            logger.error("❌ Line count mismatch: difference = %d", diff)
            raise ValueError(f"Line count mismatch: input={total_input}, output={total_output}")

        return output_paths

    finally:
        # Cleanup temp directory
        shutil.rmtree(tmp_dir, ignore_errors=True)


def run_split_step(settings: Settings, force: bool = False) -> dict[str, Path]:
    """Run the split step of the pipeline.

    Args:
        settings: Application settings.
        force: Force re-processing even if outputs exist.

    Returns:
        Dictionary mapping table names to output parquet paths.
    """
    logger.info("Starting split step...")
    return split_raw_to_parquet(settings, force=force)
