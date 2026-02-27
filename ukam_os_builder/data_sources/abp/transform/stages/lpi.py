"""LPI (Land and Property Identifier) transformation stage.

==============================================================================
CONCEPTUAL OVERVIEW: The "Official" Local Authority View of an Address
==============================================================================

This script constructs address strings based on data provided by Local
Authorities (City & County Councils), rather than Royal Mail.

To a non-expert, it might seem strange that "Official" addresses are separate
from "Postal" addresses, but they serve different purposes. Local Authorities
are legally responsible for naming streets and numbering buildings (for taxes,
voting, emergency services, and planning). Royal Mail is responsible for
delivering post. While usually the same, these two views often diverge
(e.g., a "Granny Annexe" might be a separate property for Council Tax, but share
a letterbox with the main house for Royal Mail).

This script assembles the Local Authority view.

------------------------------------------------------------------------------
Where does the data come from?
------------------------------------------------------------------------------
We combine three specific tables from AddressBase Premium:

1.  **LPI (Land and Property Identifier - Record Type 24):**
    This is the core "linking" table. It doesn't contain the street name itself.
    Instead, it links a physical object (UPRN) to a street (USRN) and adds the
    specific house numbers or names.
    *   *Provenance:* Local Authority Custodians.

2.  **Street Descriptor (Record Type 15):**
    This table holds the actual text names of streets (e.g., "High Street").
    We join this to the LPI using the USRN (Unique Street Reference Number).
    *   *Provenance:* Local Authority Custodians.

3.  **BLPU (Basic Land and Property Unit - Record Type 21):**
    This represents the physical existence of the property (the coordinate point).
    Crucially, it usually holds the Postcode (supplied by Royal Mail but linked
    to the council record).
    *   *Provenance:* Local Authority (geometry) + Royal Mail (postcode link).

------------------------------------------------------------------------------
How is an address constructed? (The "Lego Brick" approach)
------------------------------------------------------------------------------
AddressBase is "normalized," meaning the address isn't stored as a full line
of text. It is stored as components (SAO and PAO) that this script glues together.

*   **SAO (Secondary Addressable Object):** The "inner" part of an address.
    e.g., "Flat 3" or "First Floor".
*   **PAO (Primary Addressable Object):** The "outer" part of an address.
    e.g., "12" or "The Manor House".

The logic in `build_base_address` combines these:
    [SAO Start] [SAO Suffix] [SAO Text]  ->  "Flat 1"
             +
    [PAO Start] [PAO Suffix] [PAO Text]  ->  "12 Example House"
             +
    [Street Descriptor]                  ->  "Main Street"
             +
    [Town] + [Postcode]

------------------------------------------------------------------------------
Why does this script generate multiple rows per property?
------------------------------------------------------------------------------
A single property (UPRN) can have multiple LPI records. This is valuable for
matching messy user input. We output variants based on **Logical Status**:

1.  **Approved (1):** The current, legal address.
2.  **Alternative (3):** A valid alias (e.g., a house has a number "4" but is
    locally known as "Rose Cottage").
3.  **Provisional (6):** The address assigned during planning/construction, which
    might change before the house is built.

Historic addresses (logical_status=8) are excluded from output.

------------------------------------------------------------------------------
Key Columns Explained
------------------------------------------------------------------------------
*   `uprn`: The "Golden Key". Use this to link this address to other data.
*   `base_address`: The constructed full address string.
*   `logical_status`: 1=Current, 6=Provisional.
*   `official_flag`: 'Y' indicates this is the "official" version, 'N' suggests
    it might be an unofficial alias.
*   `language`: 'ENG' (English) or 'CYM' (Welsh). Streets in Wales often have
    two records, one in each language. We process both.
"""

from __future__ import annotations

import duckdb


def prepare_street_descriptor_views(
    con: duckdb.DuckDBPyConnection,
    usrn_filter_view: str,
) -> None:
    """Create best street descriptor views (by language and any).

    Args:
        con: DuckDB connection.
        usrn_filter_view: View name containing (usrn, language) pairs to filter
            street descriptors to. When num_chunks=1, this view contains all
            USRNs (a no-op filter), ensuring identical code paths.
    """
    # Best by language - filtered to USRNs in chunk
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _sd_best_by_lang AS
        SELECT sd_ranked.*
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn, sd.language
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
            INNER JOIN {usrn_filter_view} uf ON sd.usrn = uf.usrn AND sd.language = uf.language
        ) sd_ranked
        WHERE rn = 1
    """)

    # Best any language - filtered to USRNs in chunk
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _sd_best_any AS
        SELECT sd_ranked.*
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
            WHERE sd.usrn IN (SELECT DISTINCT usrn FROM {usrn_filter_view})
        ) sd_ranked
        WHERE rn = 1
    """)


def prepare_lpi_base(con: duckdb.DuckDBPyConnection) -> None:
    """Create materialised LPI base tables with address components.

    Note: This function requires that `parent_uprns_with_children` temp table
    exists in the connection. In chunked mode, this table is created from the
    full BLPU dataset to correctly identify parent UPRNs even when their
    children are in different chunks.
    """
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
                WHEN hc.uprn IS NOT NULL THEN 'P'
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
                ELSE 9
            END AS status_rank
        FROM lpi l
        JOIN blpu b ON b.uprn = l.uprn
        LEFT JOIN parent_uprns_with_children hc ON hc.uprn = l.uprn
        LEFT JOIN _sd_best_by_lang sd_lang ON sd_lang.usrn = l.usrn AND sd_lang.language = l.language
        LEFT JOIN _sd_best_any sd_any ON sd_any.usrn = l.usrn
        WHERE (b.addressbase_postal != 'N' OR b.addressbase_postal IS NULL)
          AND l.logical_status IN (1, 3, 6)
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


def render_variants(con: duckdb.DuckDBPyConnection) -> None:
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
            END AS variant_label,
            (logical_status = 1) AS is_primary
        FROM lpi_base_distinct
    """)
