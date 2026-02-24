"""Transform ABP data to flatfile module.

Transforms the split parquet files into a single flatfile suitable for
UK address matching. This includes:
- Combining BLPU, LPI, Street Descriptor, Organisation, and Delivery Point data
- Building address variants (official, alternative, historical, business names)
- Deduplication and ranking of address variants
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import duckdb

from ukam_os_builder.api.settings import Settings, create_duckdb_connection

logger = logging.getLogger(__name__)


def _assert_inputs_exist(parquet_dir: Path) -> None:
    """Check that required input parquet files exist.

    Args:
        parquet_dir: Directory containing raw parquet files.

    Raises:
        FileNotFoundError: If required files are missing.
    """
    required = [
        "blpu",
        "lpi",
        "street_descriptor",
        "organisation",
        "delivery_point",
        "classification",
    ]
    missing = [name for name in required if not (parquet_dir / f"{name}.parquet").exists()]

    if missing:
        raise FileNotFoundError(
            f"Missing required parquet files: {missing}. Run --step split first."
        )


def _register_parquet_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    where_condition: str | None = None,
) -> None:
    """Register a parquet-backed view with optional WHERE predicate."""
    sql = f"SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
    if where_condition:
        sql += f" WHERE {where_condition}"
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")


def _create_macros(con: duckdb.DuckDBPyConnection) -> None:
    """Create reusable SQL macros for address rendering."""
    # Build address component (SAO or PAO)
    con.execute("""
        CREATE OR REPLACE MACRO build_component(
            comp_text, comp_start_number, comp_start_suffix, comp_end_number, comp_end_suffix
        ) AS
        TRIM(concat_ws(' ',
            NULLIF(comp_text, ''),
            CASE
                WHEN comp_start_number IS NOT NULL AND comp_end_number IS NULL
                    THEN concat(comp_start_number, COALESCE(comp_start_suffix, ''))
            END,
            CASE
                WHEN comp_start_number IS NOT NULL AND comp_end_number IS NOT NULL
                    THEN concat(
                        comp_start_number, COALESCE(comp_start_suffix, ''), '-',
                        comp_end_number, COALESCE(comp_end_suffix, '')
                    )
            END
        ))
    """)

    # Build full base address
    con.execute("""
        CREATE OR REPLACE MACRO build_base_address(
            sao_text, sao_start_number, sao_start_suffix, sao_end_number, sao_end_suffix,
            pao_text, pao_start_number, pao_start_suffix, pao_end_number, pao_end_suffix,
            street_description, locality_name, town_name
        ) AS
        TRIM(concat_ws(' ',
            NULLIF(TRIM(concat_ws(' ',
                build_component(sao_text, sao_start_number, sao_start_suffix, sao_end_number, sao_end_suffix),
                build_component(pao_text, pao_start_number, pao_start_suffix, pao_end_number, pao_end_suffix)
            )), ''),
            NULLIF(street_description, ''),
            NULLIF(locality_name, ''),
            NULLIF(town_name, '')
        ))
    """)


def _prepare_street_descriptor_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create best street descriptor views (by language and any)."""
    # Best by language
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _sd_best_by_lang AS
        SELECT *
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn, sd.language
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
        )
        WHERE rn = 1
    """)

    # Best any language
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _sd_best_any AS
        SELECT *
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
        )
        WHERE rn = 1
    """)


def _prepare_lpi_base(con: duckdb.DuckDBPyConnection) -> None:
    """Create materialised LPI base tables with address components."""
    con.execute("DROP TABLE IF EXISTS lpi_base_full")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_base_full AS
        SELECT
            l.uprn,
            l.lpi_key,
            l.language,
            l.logical_status,
            l.official_flag,
            l.start_date,
            l.end_date,
            l.last_update_date,
            b.postcode_locator AS postcode,
            b.blpu_state,
            b.addressbase_postal AS postal_address_code,
            b.parent_uprn,
            CASE
                WHEN b.parent_uprn IS NOT NULL THEN 'C'
                WHEN EXISTS (SELECT 1 FROM blpu b2 WHERE b2.parent_uprn = l.uprn) THEN 'P'
                ELSE 'S'
            END AS hierarchy_level,
            l.level,
            COALESCE(sd_lang.street_description, sd_any.street_description) AS street_description,
            COALESCE(sd_lang.locality, sd_any.locality) AS locality_name,
            COALESCE(sd_lang.town_name, sd_any.town_name) AS town_name,
            build_base_address(
                l.sao_text, l.sao_start_number, l.sao_start_suffix, l.sao_end_number, l.sao_end_suffix,
                l.pao_text, l.pao_start_number, l.pao_start_suffix, l.pao_end_number, l.pao_end_suffix,
                COALESCE(sd_lang.street_description, sd_any.street_description),
                COALESCE(sd_lang.locality, sd_any.locality),
                COALESCE(sd_lang.town_name, sd_any.town_name)
            ) AS base_address,
            CASE l.logical_status
                WHEN 1 THEN 0
                WHEN 3 THEN 1
                WHEN 6 THEN 2
                WHEN 8 THEN 3
                ELSE 9
            END AS status_rank
        FROM lpi l
        JOIN blpu b ON b.uprn = l.uprn
        LEFT JOIN _sd_best_by_lang sd_lang ON sd_lang.usrn = l.usrn AND sd_lang.language = l.language
        LEFT JOIN _sd_best_any sd_any ON sd_any.usrn = l.usrn
        WHERE (b.addressbase_postal != 'N' OR b.addressbase_postal IS NULL)
          AND l.logical_status IN (1, 3, 6, 8)
    """)

    # Deduplicated distinct addresses
    con.execute("DROP TABLE IF EXISTS lpi_base_distinct")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_base_distinct AS
        SELECT DISTINCT
            uprn,
            base_address,
            postcode,
            logical_status,
            official_flag,
            blpu_state,
            postal_address_code,
            parent_uprn,
            hierarchy_level,
            start_date,
            end_date,
            last_update_date,
            status_rank
        FROM lpi_base_full
        WHERE base_address IS NOT NULL AND base_address <> ''
    """)

    # Best current LPI per UPRN
    con.execute("DROP TABLE IF EXISTS lpi_best_current")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_best_current AS
        SELECT *
        FROM (
            SELECT
                uprn,
                base_address,
                postcode,
                logical_status,
                official_flag,
                blpu_state,
                postal_address_code,
                parent_uprn,
                hierarchy_level,
                status_rank,
                last_update_date,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY status_rank, COALESCE(last_update_date, DATE '0001-01-01') DESC
                ) AS rn
            FROM lpi_base_distinct
            WHERE logical_status IN (1, 3, 6)
        )
        WHERE rn = 1
    """)


def _prepare_delivery_point_best(con: duckdb.DuckDBPyConnection) -> None:
    """Create best delivery point per UPRN."""
    con.execute("DROP TABLE IF EXISTS delivery_point_best")
    con.execute("""
        CREATE TEMPORARY TABLE delivery_point_best AS
        SELECT *
        FROM (
            SELECT
                uprn,
                udprn,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY
                        COALESCE(end_date, DATE '9999-12-31') DESC,
                        COALESCE(last_update_date, DATE '0001-01-01') DESC
                ) AS rn
            FROM delivery_point
            WHERE udprn IS NOT NULL
        )
        WHERE rn = 1
    """)


def _prepare_classification_best(con: duckdb.DuckDBPyConnection) -> None:
    """Create best classification per UPRN."""
    con.execute("DROP TABLE IF EXISTS classification_best")
    con.execute("""
        CREATE TEMPORARY TABLE classification_best AS
        SELECT *
        FROM (
            SELECT
                uprn,
                classification_code,
                class_scheme,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY
                        CASE WHEN class_scheme = 'AddressBase Premium Classification Scheme' THEN 0 ELSE 1 END,
                        COALESCE(end_date, DATE '9999-12-31') DESC,
                        COALESCE(last_update_date, DATE '0001-01-01') DESC
                ) AS rn
            FROM classification
        )
        WHERE rn = 1
    """)


def _render_lpi_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create LPI-based address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_lpi_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_lpi_variants AS
        SELECT
            uprn,
            postcode,
            base_address AS raw_address,
            'LPI' AS source,
            logical_status,
            official_flag,
            blpu_state,
            postal_address_code,
            parent_uprn,
            hierarchy_level,
            CASE logical_status
                WHEN 1 THEN 'APPROVED'
                WHEN 3 THEN 'ALTERNATIVE'
                WHEN 6 THEN 'PROVISIONAL'
                WHEN 8 THEN 'HISTORICAL'
            END AS variant_label,
            (logical_status = 1) AS is_primary
        FROM lpi_base_distinct
    """)


def _render_business_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create organisation/business name address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_business_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_business_variants AS
        WITH organisation_clean AS (
            SELECT
                uprn,
                TRIM(organisation) AS organisation_name,
                TRIM(legal_name) AS legal_name,
                start_date,
                end_date
            FROM organisation
        ),
        organisation_candidates AS (
            SELECT uprn, 'ORGANISATION' AS name_source, organisation_name AS name_value, start_date, end_date
            FROM organisation_clean
            WHERE organisation_name IS NOT NULL AND organisation_name <> ''
            UNION ALL
            SELECT uprn, 'LEGAL_NAME' AS name_source, legal_name AS name_value, start_date, end_date
            FROM organisation_clean
            WHERE legal_name IS NOT NULL AND legal_name <> ''
              AND (organisation_name IS NULL OR organisation_name <> legal_name)
        ),
        current_variants AS (
            SELECT
                oc.uprn,
                lb.postcode,
                TRIM(concat_ws(' ', oc.name_value, lb.base_address)) AS raw_address,
                lb.logical_status,
                lb.official_flag,
                lb.blpu_state,
                lb.postal_address_code,
                lb.parent_uprn,
                lb.hierarchy_level,
                CASE WHEN oc.name_source = 'LEGAL_NAME' THEN 'BUSINESS_CURRENT_LEGAL' ELSE 'BUSINESS_CURRENT' END AS variant_label,
                FALSE AS is_primary
            FROM organisation_candidates oc
            JOIN lpi_best_current lb ON lb.uprn = oc.uprn
            WHERE oc.end_date IS NULL
        ),
        historical_variants AS (
            SELECT
                oc.uprn,
                lb.postcode,
                TRIM(concat_ws(' ', oc.name_value, lb.base_address)) AS raw_address,
                lb.logical_status,
                lb.official_flag,
                lb.blpu_state,
                lb.postal_address_code,
                lb.parent_uprn,
                lb.hierarchy_level,
                CASE WHEN oc.name_source = 'LEGAL_NAME' THEN 'BUSINESS_HISTORICAL_LEGAL' ELSE 'BUSINESS_HISTORICAL' END AS variant_label,
                FALSE AS is_primary
            FROM organisation_candidates oc
            JOIN LATERAL (
                SELECT base_address, postcode, logical_status, official_flag, blpu_state,
                       postal_address_code, parent_uprn, hierarchy_level
                FROM lpi_base_distinct lb
                WHERE lb.uprn = oc.uprn
                ORDER BY
                    CASE WHEN (lb.start_date IS NULL OR oc.end_date >= lb.start_date)
                          AND (lb.end_date IS NULL OR oc.start_date <= lb.end_date) THEN 0 ELSE 1 END,
                    status_rank,
                    COALESCE(lb.last_update_date, DATE '0001-01-01') DESC
                LIMIT 1
            ) lb ON TRUE
            WHERE oc.end_date IS NOT NULL
        )
        SELECT uprn, postcode, raw_address, 'ORGANISATION' AS source, logical_status,
               official_flag, blpu_state, postal_address_code, parent_uprn, hierarchy_level,
               variant_label, is_primary
        FROM current_variants
        UNION ALL
        SELECT uprn, postcode, raw_address, 'ORGANISATION' AS source, logical_status,
               official_flag, blpu_state, postal_address_code, parent_uprn, hierarchy_level,
               variant_label, is_primary
        FROM historical_variants
        WHERE raw_address IS NOT NULL AND raw_address <> ''
    """)


def _render_delivery_point_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create Royal Mail delivery point address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_delivery_point_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_delivery_point_variants AS
        WITH delivery_rendered AS (
            SELECT
                d.uprn,
                d.postcode AS postcode,
                TRIM(concat_ws(' ',
                    NULLIF(TRIM(concat_ws(' ',
                        d.department_name, d.organisation_name, d.sub_building_name,
                        d.building_name, d.building_number
                    )), ''),
                    NULLIF(d.dependent_thoroughfare, ''),
                    NULLIF(d.thoroughfare, ''),
                    NULLIF(d.double_dependent_locality, ''),
                    NULLIF(d.dependent_locality, ''),
                    NULLIF(d.post_town, '')
                )) AS raw_address
            FROM delivery_point d
            WHERE d.postcode IS NOT NULL
        )
        SELECT
            uprn,
            postcode,
            raw_address,
            'DELIVERY_POINT' AS source,
            CAST(NULL AS INTEGER) AS logical_status,
            CAST(NULL AS VARCHAR) AS official_flag,
            CAST(NULL AS VARCHAR) AS blpu_state,
            CAST(NULL AS VARCHAR) AS postal_address_code,
            CAST(NULL AS BIGINT) AS parent_uprn,
            CAST(NULL AS VARCHAR) AS hierarchy_level,
            'DELIVERY' AS variant_label,
            FALSE AS is_primary
        FROM delivery_rendered
        WHERE raw_address IS NOT NULL AND raw_address <> ''
    """)


def _render_custom_level_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create custom level-based address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_custom_level_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_custom_level_variants AS
        WITH level_parsed AS (
            SELECT
                uprn, postcode, base_address,
                CASE
                    WHEN split_part(level, ',', 1) ~ '^-?[0-9]+$'
                        THEN CAST(split_part(level, ',', 1) AS INTEGER)
                    ELSE NULL
                END AS level_int
            FROM lpi_base_full
            WHERE level IS NOT NULL AND base_address IS NOT NULL AND base_address <> ''
        ),
        level_words AS (
            SELECT
                uprn, postcode, base_address,
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
            postcode,
            TRIM(concat(level_word, ' ', base_address)) AS raw_address,
            'CUSTOM_LEVEL' AS source,
            CAST(NULL AS INTEGER) AS logical_status,
            CAST(NULL AS VARCHAR) AS official_flag,
            CAST(NULL AS VARCHAR) AS blpu_state,
            CAST(NULL AS VARCHAR) AS postal_address_code,
            CAST(NULL AS BIGINT) AS parent_uprn,
            CAST(NULL AS VARCHAR) AS hierarchy_level,
            'CUSTOM_LEVEL' AS variant_label,
            FALSE AS is_primary
        FROM level_words
        WHERE level_word IS NOT NULL
    """)


def _combine_and_dedupe(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Combine all variant tables and deduplicate."""
    # Combine all stage tables
    con.execute("""
        CREATE OR REPLACE VIEW _raw_address_variants AS
        SELECT * FROM _stage_lpi_variants
        UNION ALL SELECT * FROM _stage_business_variants
        UNION ALL SELECT * FROM _stage_delivery_point_variants
        UNION ALL SELECT * FROM _stage_custom_level_variants
    """)

    # Final deduplication and enrichment
    return con.sql(r"""
        WITH normalized AS (
            SELECT
                uprn, postcode,
                REGEXP_REPLACE(REPLACE(raw_address, CHR(39), ''), '\s+', ' ') AS address_concat,
                source, logical_status, blpu_state, postal_address_code,
                parent_uprn, hierarchy_level, variant_label, is_primary
            FROM _raw_address_variants
        ),
        ranked AS (
            SELECT *,
                CASE logical_status WHEN 1 THEN 0 WHEN 3 THEN 1 WHEN 6 THEN 2 WHEN 8 THEN 3 ELSE 9 END AS status_rank,
                CASE source WHEN 'LPI' THEN 0 WHEN 'ORGANISATION' THEN 1 WHEN 'DELIVERY_POINT' THEN 2 WHEN 'CUSTOM_LEVEL' THEN 3 ELSE 4 END AS source_rank
            FROM normalized
        ),
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn, address_concat
                    ORDER BY is_primary DESC, status_rank, source_rank, variant_label, source
                ) AS rn
            FROM ranked
        ),
        deduped_filtered AS (
            SELECT uprn, postcode, address_concat, source, logical_status, blpu_state,
                   postal_address_code, parent_uprn, hierarchy_level, variant_label, is_primary
            FROM deduped WHERE rn = 1
        ),
        source_ranked AS (
            SELECT *,
                SUM(CASE WHEN is_primary THEN 1 ELSE 0 END) OVER (PARTITION BY uprn) AS primary_count,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY
                        CASE source WHEN 'LPI' THEN 0 WHEN 'ORGANISATION' THEN 1 WHEN 'DELIVERY_POINT' THEN 2 WHEN 'CUSTOM_LEVEL' THEN 3 ELSE 4 END,
                        variant_label, address_concat
                ) AS uprn_rank
            FROM deduped_filtered
        )
        SELECT
            sr.uprn,
            sr.postcode,
            sr.address_concat,
            cb.classification_code,
            sr.logical_status,
            sr.blpu_state,
            sr.postal_address_code,
            dp.udprn,
            sr.parent_uprn,
            sr.hierarchy_level,
            sr.source,
            sr.variant_label,
            CASE WHEN sr.primary_count > 0 THEN sr.is_primary ELSE sr.uprn_rank = 1 END AS is_primary
        FROM source_ranked sr
        LEFT JOIN classification_best cb ON cb.uprn = sr.uprn
        LEFT JOIN delivery_point_best dp ON dp.uprn = sr.uprn
        ORDER BY sr.uprn, sr.source, sr.variant_label
    """)


def transform_to_flatfile(
    settings: Settings,
    force: bool = False,
) -> Path:
    """Transform split parquet files into a single flatfile for address matching.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        Path to the output parquet file.

    Raises:
        FileNotFoundError: If required input files are missing.
        ToFlatfileError: If transformation fails.
    """
    parquet_dir = settings.paths.parquet_dir / "raw"
    output_dir = settings.paths.output_dir
    output_path = output_dir / "abp_for_uk_address_matcher.parquet"

    # Check inputs
    _assert_inputs_exist(parquet_dir)

    # Check if output exists
    if output_path.exists() and not force:
        logger.info("Output already exists: %s. Use --force to re-process.", output_path)
        return output_path

    output_dir.mkdir(parents=True, exist_ok=True)

    total_start = perf_counter()
    logger.info("Starting flatfile transformation...")

    # Create connection and register views
    con = create_duckdb_connection(settings)

    _register_parquet_view(con, "blpu", parquet_dir / "blpu.parquet")
    _register_parquet_view(con, "lpi", parquet_dir / "lpi.parquet")
    _register_parquet_view(con, "street_descriptor", parquet_dir / "street_descriptor.parquet")
    _register_parquet_view(con, "organisation", parquet_dir / "organisation.parquet")
    _register_parquet_view(con, "delivery_point", parquet_dir / "delivery_point.parquet")
    _register_parquet_view(con, "classification", parquet_dir / "classification.parquet")

    # Prepare macros and intermediate tables
    t0 = perf_counter()
    _create_macros(con)
    _prepare_street_descriptor_views(con)
    _prepare_lpi_base(con)
    _prepare_delivery_point_best(con)
    _prepare_classification_best(con)
    logger.info("Preparation completed in %.2f seconds", perf_counter() - t0)

    # Render variants
    stages = [
        ("LPI variants", _render_lpi_variants),
        ("Business variants", _render_business_variants),
        ("Delivery point variants", _render_delivery_point_variants),
        ("Custom level variants", _render_custom_level_variants),
    ]

    for label, func in stages:
        t0 = perf_counter()
        func(con)
        logger.info("%s rendered in %.2f seconds", label, perf_counter() - t0)

    # Combine and write
    t0 = perf_counter()
    result = _combine_and_dedupe(con)
    logger.info("Combination and deduplication in %.2f seconds", perf_counter() - t0)

    # Data integrity check and statistics
    input_uprn_count = con.execute("SELECT COUNT(DISTINCT uprn) FROM lpi_base_distinct").fetchone()[
        0
    ]
    output_metrics = con.execute(
        "SELECT COUNT(DISTINCT uprn) AS output_uprn_count, COUNT(*) AS total_variants FROM result"
    ).fetchone()
    output_uprn_count = output_metrics[0]
    total_variants = output_metrics[1]

    assert input_uprn_count == output_uprn_count, (
        f"Lost UPRNs during processing! Input: {input_uprn_count}, Output: {output_uprn_count}"
    )

    variant_uplift_pct = ((total_variants - output_uprn_count) / output_uprn_count) * 100
    logger.info(
        "Address Statistics - Input UPRNs (Unique): %d | Output UPRNs (Unique): %d | Total Address Variants Generated: %d | Variant Uplift: %.1f%%",
        input_uprn_count,
        output_uprn_count,
        total_variants,
        variant_uplift_pct,
    )

    t0 = perf_counter()
    if output_path.exists():
        output_path.unlink()
    result.write_parquet(output_path.as_posix())
    logger.info("Parquet written in %.2f seconds", perf_counter() - t0)

    total_duration = perf_counter() - total_start
    logger.info("Flatfile transformation completed in %.2f seconds", total_duration)
    logger.info("Output: %s", output_path)

    return output_path


def run_flatfile_step(settings: Settings, force: bool = False) -> Path:
    """Run the flatfile step of the pipeline.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        Path to the output parquet file.
    """
    logger.info("Starting flatfile step...")
    return transform_to_flatfile(settings, force=force)
