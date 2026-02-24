"""Postal (Royal Mail Delivery Point) transformation stage.

==============================================================================
CONCEPTUAL OVERVIEW: The "Postman's View" (Royal Mail PAF)
==============================================================================

This script constructs address strings based on the Royal Mail Postcode Address
File (PAF). This is the address format you would typically find on an envelope.

To a non-expert, this represents the "delivery" address. While the Local
Authority view (LPI) focuses on the legal existence of a property for taxes and
planning, this view focuses purely on the logistics of delivering mail.

------------------------------------------------------------------------------
Where does the data come from?
------------------------------------------------------------------------------
We use one specific table from AddressBase Premium:

1.  **Delivery Point Address (Record Type 28):**
    Unlike the LPI (which is a complex web of linked tables), the Delivery Point
    Address (DPA) is a "flat" record. It comes directly from Royal Mail.
    *   *Provenance:* Royal Mail.
    *   *Key Difference:* This table actually contains the text for the street,
        town, and locality. It does *not* require joining to a separate
        Street Descriptor table like the LPI does.

------------------------------------------------------------------------------
How is an address constructed? (The "Flat List" approach)
------------------------------------------------------------------------------
Because the data is already denormalized (flat), constructing the address string
is much simpler than the LPI process. It is essentially a concatenation of specific
fields in a specific order defined by Royal Mail standards:

1.  **Department / Organisation:** (e.g., "HR Dept", "Acme Corp")
2.  **Sub-building:** (e.g., "Flat 1")
3.  **Building Name:** (e.g., "The Heights")
4.  **Building Number:** (e.g., "12")
5.  **Thoroughfares:** (Dependent Street -> Main Street)
6.  **Localities:** (Double Dependent -> Dependent -> Post Town)
7.  **Postcode**

The `render_variants` function simply glues these non-empty fields together.

------------------------------------------------------------------------------
Why is this useful for matching?
------------------------------------------------------------------------------
People often use the Royal Mail version of an address rather than the official
Council version. They might omit the district name, or use a slightly different
town name preferred by the sorting office. By including this variant in our
output, we increase the chance of matching user input that follows the "Post
Office" format rather than the "Town Hall" format.

------------------------------------------------------------------------------
Key Columns Explained
------------------------------------------------------------------------------
*   `uprn`: The "Golden Key". This allows us to link this Royal Mail record
    to the Local Authority geometry and lifecycle data found in other tables.
*   `udprn`: Unique Delivery Point Reference Number. This is Royal Mail's
    internal primary key for the letterbox.
*   `postcode`: The definitive postcode for mail delivery.
"""

from __future__ import annotations

import duckdb


def prepare_best_delivery(con: duckdb.DuckDBPyConnection) -> None:
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


def render_variants(con: duckdb.DuckDBPyConnection) -> None:
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
