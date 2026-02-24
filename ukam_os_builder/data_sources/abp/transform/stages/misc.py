"""Miscellaneous transformation stages (classification and custom levels).

==============================================================================
CONCEPTUAL OVERVIEW: Classifications & "Floor Level" Variants
==============================================================================

This script handles two distinct but useful tasks:
1.  Identifying **what** a property is (Classification).
2.  Creating extra address variants based on which **floor** a property is on.

------------------------------------------------------------------------------
Part 1: Classification ("Is it a house or a shop?")
------------------------------------------------------------------------------
An address tells you *where* a building is. A classification tells you *what*
it is.

*   **Source:** Classification Table (Record Type 32).
*   **Provenance:** Local Authorities and Ordnance Survey surveyors.
*   **The Code:** `prepare_classification_best`
*   **Concept:**
    Every UPRN has a classification code (e.g., "RD" for Residential Dwelling,
    "C" for Commercial). However, a single property might have multiple
    classification records from different schemes (legacy vs. modern).
    This function picks the "winner" for each UPRN, prioritizing the modern
    "AddressBase Premium Classification Scheme". This allows downstream users to
    easily filter for "just houses" or "just businesses".

------------------------------------------------------------------------------
Part 2: Custom Level Variants ("First Floor Flat")
------------------------------------------------------------------------------
To a non-expert, if you live on the first floor, you might write your address as
"First Floor Flat, 10 High Street". However, the official Council address might
simply be "Flat A, 10 High Street".

If the official data tracks the vertical position of the property (the Level),
we can generate a variant to match the human-written address.

*   **Source:** LPI Table (Record Type 24) -> `level` column.
*   **Provenance:** Local Authority Custodians.
*   **The Code:** `render_custom_levels`
*   **Concept:**
    The LPI record often contains a numeric `level` (e.g., "1", "2", "0").
    This script translates those numbers into words and sticks them onto the
    front of the address.

    *   **Mapping:**
        *   -1 -> "BASEMENT"
        *   0  -> "GROUND"
        *   1  -> "FIRST"
        *   (up to 6 -> "SIXTH")

    *   **Example:**
        *   Official Address: "Flat A, 10 High Street"
        *   LPI Level Data: "1"
        *   **Generated Variant:** "FIRST Flat A, 10 High Street"

    This is purely a synthetic address variant designed to catch user input where
    the floor level is written out explicitly.
"""

from __future__ import annotations

import duckdb


def prepare_classification_best(con: duckdb.DuckDBPyConnection) -> None:
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


def render_custom_levels(con: duckdb.DuckDBPyConnection) -> None:
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
