"""Combine and deduplicate all address variants."""

from __future__ import annotations

import duckdb


def combine_and_dedupe(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Combine all variant tables and deduplicate.

    Note: Final ORDER BY is omitted to reduce memory usage during chunked
    processing. Parquet readers can sort on read if needed, or downstream
    consumers can handle ordering.
    """
    # Combine all stage tables
    con.execute("""
        CREATE OR REPLACE VIEW _raw_address_variants AS
        SELECT * FROM _stage_lpi_variants
        UNION ALL SELECT * FROM _stage_business_variants
        UNION ALL SELECT * FROM _stage_delivery_point_variants
        UNION ALL SELECT * FROM _stage_custom_level_variants
    """)

    # Final deduplication and enrichment (no ORDER BY for memory efficiency)
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
                CASE logical_status WHEN 1 THEN 0 WHEN 3 THEN 1 WHEN 6 THEN 2 ELSE 9 END AS status_rank,
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
            sr.uprn AS unique_id,
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
    """)
