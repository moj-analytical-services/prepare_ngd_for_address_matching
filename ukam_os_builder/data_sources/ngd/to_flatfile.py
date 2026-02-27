"""Transform NGD data to flatfile module.

Transforms the extracted parquet files into a single flatfile suitable for
UK address matching. This includes:
- Processing core feature types (Built Address, Pre-Build Address, etc.)
- Processing alternate address records
- Processing Royal Mail addresses
- Handling Welsh language variants
- Deduplication with priority rules
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import duckdb

from ukam_os_builder._exceptions import ToFlatfileError
from ukam_os_builder.api.settings import Settings, create_duckdb_connection

logger = logging.getLogger(__name__)


# Mapping of file stems to feature types
FEATURE_TYPE_BY_STEM = {
    "add_gb_builtaddress": "Built Address",
    "add_gb_builtaddress_altadd": "Built Address",
    "add_gb_nonaddressableobject": "Non-Addressable Object",
    "add_gb_nonaddressableobject_altadd": "Non-Addressable Object",
    "add_gb_prebuildaddress": "Pre-Build Address",
    "add_gb_prebuildaddress_altadd": "Pre-Build Address",
    "add_gb_royalmailaddress": "Royal Mail Address",
}

# Core feature stems (contain fulladdress and classification fields)
CORE_FEATURE_STEMS = {
    "add_gb_builtaddress",
    "add_gb_nonaddressableobject",
    "add_gb_prebuildaddress",
}

# Alternate address stems (no classification fields)
ALTADD_STEMS = {
    "add_gb_builtaddress_altadd",
    "add_gb_nonaddressableobject_altadd",
    "add_gb_prebuildaddress_altadd",
}

# Priority order for metadata lookup (lower = higher priority)
CORE_FEATURE_PRIORITY = {
    "add_gb_builtaddress": 1,
    "add_gb_prebuildaddress": 2,
    "add_gb_nonaddressableobject": 3,
}


def _create_metadata_lookup_view(
    con: duckdb.DuckDBPyConnection,
    parquet_dir: Path,
    uprn_predicate: str | None = None,
) -> None:
    """Create a lookup view with metadata from all core feature files.

    This view is used to enrich Royal Mail and alternate address records
    with metadata (classificationcode, parentuprn, etc.) by UPRN lookup.

    Uses priority ranking (Built > Pre-Build > Non-Addressable)
    to dedupe when a UPRN exists in multiple core files.

    Args:
        con: DuckDB connection.
        parquet_dir: Directory containing parquet files.
        uprn_predicate: Optional predicate for hash-based chunking.
    """
    where_clause = f"WHERE {uprn_predicate}" if uprn_predicate else ""

    # Build UNION ALL of all core files that exist
    union_parts = []
    for stem, priority in sorted(CORE_FEATURE_PRIORITY.items(), key=lambda x: x[1]):
        parquet_path = parquet_dir / f"{stem}.parquet"
        if parquet_path.exists():
            union_parts.append(f"""
                SELECT
                    CAST(uprn AS BIGINT) AS uprn,
                    CAST(classificationcode AS VARCHAR) AS classificationcode,
                    CAST(parentuprn AS BIGINT) AS parentuprn,
                    CAST(rootuprn AS BIGINT) AS rootuprn,
                    CAST(hierarchylevel AS INTEGER) AS hierarchylevel,
                    CAST(floorlevel AS VARCHAR) AS floorlevel,
                    CAST(lowestfloorlevel AS DOUBLE) AS lowestfloorlevel,
                    CAST(highestfloorlevel AS DOUBLE) AS highestfloorlevel,
                    {priority} AS source_priority
                FROM read_parquet('{parquet_path.as_posix()}')
                {where_clause}
            """)

    if not union_parts:
        # No core files found - create empty lookup
        logger.warning("No core feature files found. Metadata lookup will be empty.")
        con.execute("""
            CREATE OR REPLACE TEMP VIEW uprn_metadata_lookup AS
            SELECT
                CAST(NULL AS BIGINT) AS uprn,
                CAST(NULL AS VARCHAR) AS classificationcode,
                CAST(NULL AS BIGINT) AS parentuprn,
                CAST(NULL AS BIGINT) AS rootuprn,
                CAST(NULL AS INTEGER) AS hierarchylevel,
                CAST(NULL AS VARCHAR) AS floorlevel,
                CAST(NULL AS DOUBLE) AS lowestfloorlevel,
                CAST(NULL AS DOUBLE) AS highestfloorlevel
            WHERE 1=0
        """)
        return

    union_sql = "\nUNION ALL\n".join(union_parts)

    sql = f"""
        CREATE OR REPLACE TEMP VIEW uprn_metadata_lookup AS
        WITH core_data AS (
            {union_sql}
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY source_priority
                ) AS rn
            FROM core_data
        )
        SELECT
            uprn,
            classificationcode,
            parentuprn,
            rootuprn,
            hierarchylevel,
            floorlevel,
            lowestfloorlevel,
            highestfloorlevel
        FROM ranked
        WHERE rn = 1;
    """
    con.execute(sql)


def _create_core_feature_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    uprn_predicate: str | None = None,
) -> None:
    """Create view for core feature types (Built, Pre-Build, Non-Addressable).

    These tables have fulladdress, classification fields, and Welsh language columns.
    Produces both English and Welsh (where available) address records.
    """
    where_clause = f"WHERE {uprn_predicate}" if uprn_predicate else ""
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH src AS (
            SELECT * FROM read_parquet('{parquet_path.as_posix()}')
            {where_clause}
        )
        -- English track
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            -- Strip postcode from end of fulladdress (format: "..., POSTCODE")
            CAST(
              CASE
                WHEN postcode IS NOT NULL AND fulladdress LIKE '%, ' || postcode
                THEN RTRIM(SUBSTR(fulladdress, 1, LENGTH(fulladdress) - LENGTH(postcode)), ', ')
                ELSE fulladdress
              END AS VARCHAR
            ) AS address_concat,
            CAST(postcode AS VARCHAR) AS postcode,
            '{parquet_path.name}' AS filename,
            CAST(classificationcode AS VARCHAR) AS classificationcode,
            CAST(parentuprn AS BIGINT) AS parentuprn,
            CAST(rootuprn AS BIGINT) AS rootuprn,
            CAST(hierarchylevel AS INTEGER) AS hierarchylevel,
            CAST(floorlevel AS VARCHAR) AS floorlevel,
            CAST(lowestfloorlevel AS DOUBLE) AS lowestfloorlevel,
            CAST(highestfloorlevel AS DOUBLE) AS highestfloorlevel,
            -- Internal columns for deduplication (not in final output)
            CAST(description AS VARCHAR) AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(buildstatus AS VARCHAR) AS build_status
        FROM src
        UNION ALL
        -- Welsh (if present) track
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            CAST(
              CASE
                -- For alternatelanguagefulladdress, strip postcode from end
                WHEN alternatelanguagefulladdress IS NOT NULL AND postcode IS NOT NULL
                     AND alternatelanguagefulladdress LIKE '%, ' || postcode
                THEN RTRIM(SUBSTR(alternatelanguagefulladdress, 1,
                     LENGTH(alternatelanguagefulladdress) - LENGTH(postcode)), ', ')
                WHEN alternatelanguagefulladdress IS NOT NULL
                THEN alternatelanguagefulladdress
                -- For component-based address, exclude postcode
                ELSE TRIM(BOTH ', ' FROM
                  COALESCE(alternatelanguagesubname || ', ', '') ||
                  COALESCE(alternatelanguagename || ', ', '') ||
                  COALESCE(alternatelanguagenumber || ', ', '') ||
                  COALESCE(alternatelanguagestreetname || ', ', '') ||
                  COALESCE(alternatelanguagelocality || ', ', '') ||
                  COALESCE(alternatelanguagetownname || ', ', '') ||
                  COALESCE(alternatelanguageislandname, '')
                )
              END AS VARCHAR
            ) AS address_concat,
            CAST(postcode AS VARCHAR) AS postcode,
            '{parquet_path.name}' AS filename,
            CAST(classificationcode AS VARCHAR) AS classificationcode,
            CAST(parentuprn AS BIGINT) AS parentuprn,
            CAST(rootuprn AS BIGINT) AS rootuprn,
            CAST(hierarchylevel AS INTEGER) AS hierarchylevel,
            CAST(floorlevel AS VARCHAR) AS floorlevel,
            CAST(lowestfloorlevel AS DOUBLE) AS lowestfloorlevel,
            CAST(highestfloorlevel AS DOUBLE) AS highestfloorlevel,
            -- Internal columns for deduplication (not in final output)
            CAST(description AS VARCHAR) AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(buildstatus AS VARCHAR) AS build_status
        FROM src
        WHERE lower(coalesce(alternatelanguage,'')) IN ('wel','cym','welsh','cymraeg')
          AND (
                alternatelanguagefulladdress IS NOT NULL
             OR alternatelanguagesubname IS NOT NULL
             OR alternatelanguagename IS NOT NULL
             OR alternatelanguagenumber IS NOT NULL
             OR alternatelanguagestreetname IS NOT NULL
             OR alternatelanguagelocality IS NOT NULL
             OR alternatelanguagetownname IS NOT NULL
             OR alternatelanguageislandname IS NOT NULL
          );
    """
    con.execute(sql)


def _create_altadd_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    feature_type: str,
    uprn_predicate: str | None = None,
) -> None:
    """Create view for alternate address records.

    These tables have fewer fields - no classification columns.
    Metadata columns (classificationcode, parentuprn, etc.) are NULL here
    and will be enriched via UPRN lookup from core files.
    """
    where_clause = f"WHERE {uprn_predicate}" if uprn_predicate else ""
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            -- Strip postcode from end of fulladdress (format: "..., POSTCODE")
            CAST(
              CASE
                WHEN postcode IS NOT NULL AND fulladdress LIKE '%, ' || postcode
                THEN RTRIM(SUBSTR(fulladdress, 1, LENGTH(fulladdress) - LENGTH(postcode)), ', ')
                ELSE fulladdress
              END AS VARCHAR
            ) AS address_concat,
            CAST(postcode AS VARCHAR) AS postcode,
            '{parquet_path.name}' AS filename,
            CAST(NULL AS VARCHAR) AS classificationcode,
            CAST(NULL AS BIGINT) AS parentuprn,
            CAST(NULL AS BIGINT) AS rootuprn,
            CAST(NULL AS INTEGER) AS hierarchylevel,
            CAST(floorlevel AS VARCHAR) AS floorlevel,
            CAST(lowestfloorlevel AS DOUBLE) AS lowestfloorlevel,
            CAST(highestfloorlevel AS DOUBLE) AS highestfloorlevel,
            -- Internal columns for deduplication (not in final output)
            '{feature_type}' AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status
        FROM read_parquet('{parquet_path.as_posix()}')
        {where_clause};
    """
    con.execute(sql)


def _create_royal_mail_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    uprn_predicate: str | None = None,
) -> None:
    """Create view for Royal Mail Address records.

    Builds address from component fields. Produces both English and Welsh variants.
    Excludes records where matchedaddressfeaturetype is 'Non-Addressable Object'.
    All metadata columns are NULL here and will be enriched via UPRN lookup.
    """
    conditions = ["matchedaddressfeaturetype != 'Non-Addressable Object'"]
    if uprn_predicate:
        conditions.insert(0, uprn_predicate)
    where_clause = "WHERE " + " AND ".join(conditions)
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH src AS (
            SELECT * FROM read_parquet('{parquet_path.as_posix()}')
            {where_clause}
        )
        -- English
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            TRIM(BOTH ', ' FROM
                COALESCE(organisationname || ', ', '') ||
                COALESCE(departmentname   || ', ', '') ||
                COALESCE(subbuildingname  || ', ', '') ||
                COALESCE(buildingname     || ', ', '') ||
                COALESCE(CAST(buildingnumber AS VARCHAR) || ', ', '') ||
                COALESCE(dependentthoroughfare || ', ', '') ||
                COALESCE(thoroughfare     || ', ', '') ||
                COALESCE(doubledependentlocality || ', ', '') ||
                COALESCE(dependentlocality || ', ', '') ||
                COALESCE(posttown, '')
            ) AS address_concat,
            CAST(postcode AS VARCHAR) AS postcode,
            '{parquet_path.name}' AS filename,
            CAST(NULL AS VARCHAR) AS classificationcode,
            CAST(NULL AS BIGINT) AS parentuprn,
            CAST(NULL AS BIGINT) AS rootuprn,
            CAST(NULL AS INTEGER) AS hierarchylevel,
            CAST(NULL AS VARCHAR) AS floorlevel,
            CAST(NULL AS DOUBLE) AS lowestfloorlevel,
            CAST(NULL AS DOUBLE) AS highestfloorlevel,
            -- Internal columns for deduplication (not in final output)
            'Royal Mail Address' AS feature_type,
            CAST(NULL AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status
        FROM src
        UNION ALL
        -- Welsh
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            TRIM(BOTH ', ' FROM
                COALESCE(organisationname || ', ', '') ||
                COALESCE(departmentname   || ', ', '') ||
                COALESCE(subbuildingname  || ', ', '') ||
                COALESCE(buildingname     || ', ', '') ||
                COALESCE(CAST(buildingnumber AS VARCHAR) || ', ', '') ||
                COALESCE(welshdependentthoroughfare || ', ', '') ||
                COALESCE(welshthoroughfare     || ', ', '') ||
                COALESCE(welshdoubledependentlocality || ', ', '') ||
                COALESCE(welshdependentlocality || ', ', '') ||
                COALESCE(welshposttown, '')
            ) AS address_concat,
            CAST(postcode AS VARCHAR) AS postcode,
            '{parquet_path.name}' AS filename,
            CAST(NULL AS VARCHAR) AS classificationcode,
            CAST(NULL AS BIGINT) AS parentuprn,
            CAST(NULL AS BIGINT) AS rootuprn,
            CAST(NULL AS INTEGER) AS hierarchylevel,
            CAST(NULL AS VARCHAR) AS floorlevel,
            CAST(NULL AS DOUBLE) AS lowestfloorlevel,
            CAST(NULL AS DOUBLE) AS highestfloorlevel,
            -- Internal columns for deduplication (not in final output)
            'Royal Mail Address' AS feature_type,
            CAST(NULL AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status
        FROM src
        WHERE welshdependentthoroughfare IS NOT NULL
           OR welshthoroughfare IS NOT NULL
           OR welshdoubledependentlocality IS NOT NULL
           OR welshdependentlocality IS NOT NULL
           OR welshposttown IS NOT NULL;
    """
    con.execute(sql)


def _enrich_with_metadata(con: duckdb.DuckDBPyConnection) -> None:
    """Enrich all_full_addresses with metadata from core files.

    Uses COALESCE to preserve existing values (from core files)
    and fill in NULLs (from altadd and Royal Mail files) via
    the uprn_metadata_lookup table.
    """
    sql = """
        CREATE OR REPLACE TABLE all_full_addresses_enriched AS
        SELECT
            a.uprn,
            a.address_concat,
            a.postcode,
            a.filename,
            COALESCE(a.classificationcode, m.classificationcode) AS classificationcode,
            COALESCE(a.parentuprn, m.parentuprn) AS parentuprn,
            COALESCE(a.rootuprn, m.rootuprn) AS rootuprn,
            COALESCE(a.hierarchylevel, m.hierarchylevel) AS hierarchylevel,
            COALESCE(a.floorlevel, m.floorlevel) AS floorlevel,
            COALESCE(a.lowestfloorlevel, m.lowestfloorlevel) AS lowestfloorlevel,
            COALESCE(a.highestfloorlevel, m.highestfloorlevel) AS highestfloorlevel,
            -- Internal columns for deduplication
            a.feature_type,
            a.address_status,
            a.build_status
        FROM all_full_addresses a
        LEFT JOIN uprn_metadata_lookup m ON a.uprn = m.uprn;
    """
    con.execute(sql)


def _create_custom_level_rows(con: duckdb.DuckDBPyConnection) -> None:
    """Generate custom level-based address variants and insert into enriched table.

    Parses the ``floorlevel`` column (VARCHAR) from the enriched address table,
    maps integer floor levels to words (-1=BASEMENT … 6=SIXTH), and prepends the
    word to the existing ``address_concat`` to create additional address variants.

    These rows use ``feature_type='Custom Level'`` so they receive the lowest
    dedup priority and never override official address data.
    """
    sql = """
        INSERT INTO all_full_addresses_enriched
        WITH level_parsed AS (
            SELECT
                uprn, address_concat, postcode, filename,
                classificationcode, parentuprn, rootuprn,
                hierarchylevel, floorlevel, lowestfloorlevel, highestfloorlevel,
                address_status, build_status,
                CASE
                    WHEN split_part(floorlevel, ',', 1) ~ '^-?[0-9]+$'
                        THEN CAST(split_part(floorlevel, ',', 1) AS INTEGER)
                    ELSE NULL
                END AS level_int
            FROM all_full_addresses_enriched
            WHERE floorlevel IS NOT NULL
              AND address_concat IS NOT NULL
              AND address_concat <> ''
        ),
        level_words AS (
            SELECT
                *,
                CASE level_int
                    WHEN -1 THEN 'BASEMENT'
                    WHEN 0 THEN 'GROUND'
                    WHEN 1 THEN 'FIRST'
                    WHEN 2 THEN 'SECOND'
                    WHEN 3 THEN 'THIRD'
                    WHEN 4 THEN 'FOURTH'
                    WHEN 5 THEN 'FIFTH'
                    WHEN 6 THEN 'SIXTH'
                END AS level_word
            FROM level_parsed
            WHERE level_int BETWEEN -1 AND 6
        )
        SELECT
            uprn,
            TRIM(concat(level_word, ' ', address_concat)) AS address_concat,
            postcode,
            'CUSTOM_LEVEL' AS filename,
            classificationcode,
            parentuprn,
            rootuprn,
            hierarchylevel,
            floorlevel,
            lowestfloorlevel,
            highestfloorlevel,
            'Custom Level' AS feature_type,
            address_status,
            build_status
        FROM level_words
        WHERE level_word IS NOT NULL;
    """
    con.execute(sql)


def _create_dedup_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create deduplicated view of all addresses.

    Priority rules for deduplication:
    - Feature type: Built Address -> Pre-Build -> Royal Mail -> Non-Addressable
    - Address status: Approved -> Provisional -> Alternative -> Historical
    - Build status: Built Complete -> Under Construction -> Prebuild -> Historic -> Demolished

    Excludes Non-Addressable Objects from output.
    Selects only target output columns (drops internal ranking columns).
    """
    dedup_sql = """
        CREATE OR REPLACE TEMP VIEW all_full_addresses_dedup AS
        WITH ranked AS (
          SELECT
            *,
            CASE feature_type
              WHEN 'Built Address' THEN 1
              WHEN 'Pre-Build Address' THEN 2
              WHEN 'Royal Mail Address' THEN 3
              WHEN 'Non-Addressable Object' THEN 5
              WHEN 'Custom Level' THEN 6
              ELSE 9
            END AS feature_type_rank,
            CASE
              WHEN lower(coalesce(address_status, '')) = 'approved' THEN 1
              WHEN lower(coalesce(address_status, '')) = 'provisional' THEN 2
              WHEN lower(coalesce(address_status, '')) = 'alternative' THEN 3
              WHEN lower(coalesce(address_status, '')) = 'historical' THEN 9
              ELSE 5
            END AS address_status_rank,
            CASE
              WHEN lower(coalesce(build_status, '')) = 'built complete' THEN 1
              WHEN lower(coalesce(build_status, '')) = 'under construction' THEN 2
              WHEN lower(coalesce(build_status, '')) = 'prebuild' THEN 3
              WHEN lower(coalesce(build_status, '')) = 'historic' THEN 8
              WHEN lower(coalesce(build_status, '')) = 'demolished' THEN 9
              ELSE 5
            END AS build_status_rank,
            ROW_NUMBER() OVER (
              PARTITION BY uprn, address_concat
              ORDER BY
                feature_type_rank,
                address_status_rank,
                build_status_rank
            ) AS rn
          FROM all_full_addresses_enriched
          WHERE feature_type NOT IN ('Non-Addressable Object')
        )
        SELECT
          uprn,
          address_concat,
          postcode,
          filename,
          classificationcode,
          parentuprn,
          rootuprn,
          hierarchylevel,
          floorlevel,
          lowestfloorlevel,
          highestfloorlevel
        FROM ranked
        WHERE rn = 1;
    """
    con.execute(dedup_sql)


def _hash_partition_predicate(num_chunks: int, chunk_index: int) -> str:
    """Build a hash partition predicate for UPRN.

    Args:
        num_chunks: Total number of chunks.
        chunk_index: Zero-based chunk index.

    Returns:
        SQL predicate string for the partition.
    """
    return f"abs(hash(uprn)) % {num_chunks} = {chunk_index}"


def _ensure_uprn_column(con: duckdb.DuckDBPyConnection, parquet_paths: list[Path]) -> None:
    """Verify that UPRN exists in all required parquet schemas."""
    missing_uprn: list[str] = []

    for path in parquet_paths:
        columns = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
        ).fetchall()
        column_names = {row[0].lower() for row in columns}
        if "uprn" not in column_names:
            missing_uprn.append(path.name)

    if missing_uprn:
        missing_list = ", ".join(sorted(missing_uprn))
        raise ToFlatfileError("UPRN column missing from required parquet files: " + missing_list)


def run_flatfile_step(settings: Settings, force: bool = False) -> list[Path]:
    """Run the flatfile step of the pipeline.

    Transforms extracted parquet files into the final flatfile format
    suitable for UK address matching.

    Args:
        settings: Application settings.
        force: Force recreation even if output exists.

    Returns:
        List of output parquet file paths.

    Raises:
        ToFlatfileError: If transformation fails.
    """
    t0 = perf_counter()

    parquet_dir = settings.paths.extracted_dir / "parquet"
    output_dir = settings.paths.output_dir
    num_chunks = settings.processing.num_chunks

    # Check for existing output
    output_pattern = "ngd_for_uk_address_matcher.chunk_*.parquet"
    existing_outputs = list(output_dir.glob(output_pattern)) if output_dir.exists() else []

    if existing_outputs and not force:
        logger.info(
            "Output files already exist (%d files). Use --force to regenerate.",
            len(existing_outputs),
        )
        return existing_outputs

    # Clear existing outputs on force
    if existing_outputs and force:
        for f in existing_outputs:
            f.unlink()
            logger.debug("Removed existing output: %s", f.name)

    # Check parquet directory exists
    if not parquet_dir.exists():
        raise ToFlatfileError(
            f"Parquet directory not found: {parquet_dir}. Run --step extract first."
        )

    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        raise ToFlatfileError(f"No parquet files found in {parquet_dir}. Run --step extract first.")

    logger.info("Processing %d parquet files from %s", len(parquet_files), parquet_dir)

    # Create DuckDB connection
    con = create_duckdb_connection(settings)

    # Set temp directory for spill
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = output_dir / "duckdb_tmp"
    temp_dir.mkdir(exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{temp_dir.as_posix()}'")

    # Validate UPRN availability in all address parquet files
    address_parquet_files: list[Path] = []
    for path in sorted(parquet_files):
        stem = path.stem.lower()
        if stem in CORE_FEATURE_STEMS or stem in ALTADD_STEMS or stem == "add_gb_royalmailaddress":
            address_parquet_files.append(path)

    if not address_parquet_files:
        raise ToFlatfileError("No valid address parquet files found to process.")

    logger.info("Checking UPRN availability in %d parquet files", len(address_parquet_files))
    _ensure_uprn_column(con, address_parquet_files)

    # Export to parquet file(s)
    output_files: list[Path] = []
    total_count = 0

    def process_chunk(chunk_index: int, chunk_total: int) -> tuple[Path, int]:
        uprn_predicate = (
            None if chunk_total <= 1 else _hash_partition_predicate(chunk_total, chunk_index)
        )
        if uprn_predicate:
            logger.info("Applying chunk predicate: %s", uprn_predicate)

        # Create metadata lookup view (for enriching Royal Mail and altadd records)
        logger.debug("Creating metadata lookup view...")
        _create_metadata_lookup_view(con, parquet_dir, uprn_predicate)

        created_views: list[str] = []

        for path in sorted(parquet_files):
            stem = path.stem.lower()
            view_name = f"addr_{stem.replace('-', '_')}"

            if stem in CORE_FEATURE_STEMS:
                logger.debug("Creating core feature view for %s", path.name)
                _create_core_feature_view(con, view_name, path, uprn_predicate)
                created_views.append(view_name)

            elif stem in ALTADD_STEMS:
                feature_type = FEATURE_TYPE_BY_STEM.get(stem, "Alternate Address")
                logger.debug("Creating alternate address view for %s", path.name)
                _create_altadd_view(con, view_name, path, feature_type, uprn_predicate)
                created_views.append(view_name)

            elif stem == "add_gb_royalmailaddress":
                logger.debug("Creating Royal Mail view for %s", path.name)
                _create_royal_mail_view(con, view_name, path, uprn_predicate)
                created_views.append(view_name)

            else:
                logger.debug("Skipping %s (not a recognized address file)", path.name)
                continue

        if not created_views:
            raise ToFlatfileError("No valid address parquet files found to process.")

        logger.info("Created %d address views", len(created_views))

        # Union all views into a single table
        union_sql = " \nUNION ALL\n".join(f"SELECT * FROM {v}" for v in created_views)
        logger.info("Creating union table of all addresses...")
        con.execute(f"""
            CREATE OR REPLACE TABLE all_full_addresses AS
            {union_sql};
        """)

        # Enrich with metadata from lookup table
        logger.info("Enriching addresses with metadata from core files...")
        _enrich_with_metadata(con)

        # Generate custom level variants
        logger.info("Generating custom level address variants...")
        _create_custom_level_rows(con)

        # Create deduplicated view
        logger.info("Creating deduplicated view...")
        _create_dedup_view(con)

        # Get count
        count_result = con.execute("SELECT COUNT(*) FROM all_full_addresses_dedup").fetchone()
        chunk_count = count_result[0] if count_result else 0
        logger.info(
            "Chunk %d/%d addresses after deduplication: %d",
            chunk_index + 1,
            chunk_total,
            chunk_count,
        )

        # Export chunk
        if chunk_total <= 1:
            chunk_name = "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"
        else:
            chunk_name = f"ngd_for_uk_address_matcher.chunk_{chunk_index + 1:03d}_of_{chunk_total:03d}.parquet"
        output_path = output_dir / chunk_name

        logger.info("Exporting chunk %d/%d: %s", chunk_index + 1, chunk_total, chunk_name)

        if output_path.exists():
            output_path.unlink()

        con.execute(f"""
            COPY (
                SELECT * FROM all_full_addresses_dedup
            ) TO '{output_path.as_posix()}' (FORMAT 'PARQUET');
        """)

        return output_path, chunk_count

    if num_chunks <= 1:
        output_path, chunk_count = process_chunk(0, 1)
        output_files.append(output_path)
        total_count = chunk_count
    else:
        logger.info("Splitting output into %d chunks...", num_chunks)

        for i in range(num_chunks):
            output_path, chunk_count = process_chunk(i, num_chunks)
            output_files.append(output_path)
            total_count += chunk_count

    logger.info("Total addresses after deduplication: %d", total_count)

    # Cleanup temp directory
    try:
        import shutil

        shutil.rmtree(temp_dir)
    except Exception as e:
        logger.warning("Failed to remove temp directory %s: %s", temp_dir, e)

    con.close()

    elapsed = perf_counter() - t0
    logger.info(
        "Flatfile step completed in %.2f seconds. Output: %d file(s)", elapsed, len(output_files)
    )

    return output_files
