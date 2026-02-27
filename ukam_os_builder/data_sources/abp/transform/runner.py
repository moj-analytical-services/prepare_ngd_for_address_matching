"""Pipeline runner for ABP flatfile transformation.

===============================================================================
CHUNKING CONTRACT
===============================================================================
This module implements transparent UPRN-based chunking for memory efficiency.

Behaviour:
- User sets `processing.num_chunks` in config.yaml (default: 1).
- Flatfile step produces N parquet files (one per chunk).
- Chunk assignment: `uprn IS NOT NULL AND (hash(uprn) % num_chunks) = chunk_id`

Correctness guarantees:
1. With num_chunks=1, output matches non-chunked processing exactly.
2. With num_chunks>1, union of all chunk outputs equals baseline:
   - COUNT(DISTINCT uprn) matches baseline
   - Total rows match baseline
3. Parent UPRNs are correctly marked 'P' even if children are in other chunks.

Output naming: abp_for_uk_address_matcher.chunk_NNN_of_MMM.parquet
===============================================================================
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

from ukam_os_builder.api.settings import Settings, create_duckdb_connection
from ukam_os_builder.data_sources.abp.transform.common import (
    assert_inputs_exist,
    chunk_where,
    create_macros,
    register_parquet_view,
)
from ukam_os_builder.data_sources.abp.transform.stages import business, combine, lpi, misc, postal

logger = logging.getLogger(__name__)


def _get_chunk_output_path(output_dir: Path, chunk_id: int, num_chunks: int) -> Path:
    """Generate output path for a specific chunk.

    Args:
        output_dir: Base output directory.
        chunk_id: Zero-based chunk index.
        num_chunks: Total number of chunks.

    Returns:
        Path like output_dir/abp_for_uk_address_matcher.chunk_001_of_010.parquet
    """
    return (
        output_dir
        / f"abp_for_uk_address_matcher.chunk_{chunk_id + 1:03d}_of_{num_chunks:03d}.parquet"
    )


def _transform_to_flatfile_chunk(
    settings: Settings,
    chunk_id: int,
    num_chunks: int,
    force: bool = False,
) -> Path:
    """Transform split parquet files into flatfile for a single chunk.

    This is the internal workhorse that processes one chunk of UPRNs.

    Args:
        settings: Application settings.
        chunk_id: Zero-based chunk index.
        num_chunks: Total number of chunks.
        force: Force re-processing even if output exists.

    Returns:
        Path to the chunk output parquet file.

    Raises:
        FileNotFoundError: If required input files are missing.
        ToFlatfileError: If transformation fails.
    """
    parquet_dir = settings.paths.parquet_dir / "raw"
    output_dir = settings.paths.output_dir
    output_path = _get_chunk_output_path(output_dir, chunk_id, num_chunks)

    # Check if output exists
    if output_path.exists() and not force:
        logger.info(
            "Chunk %d/%d output already exists: %s",
            chunk_id + 1,
            num_chunks,
            output_path,
        )
        return output_path

    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_start = perf_counter()

    # Create connection
    con = create_duckdb_connection(settings)

    # Build chunk filter predicate
    # When num_chunks=1: hash(uprn) % 1 = 0 is always true, so this is a no-op
    uprn_filter = chunk_where("uprn", num_chunks, chunk_id)

    # Register parquet views with chunk filtering
    # Tables with UPRN get filtered; street_descriptor is handled separately
    register_parquet_view(con, "blpu", parquet_dir / "blpu.parquet", uprn_filter)
    register_parquet_view(con, "lpi", parquet_dir / "lpi.parquet", uprn_filter)
    register_parquet_view(con, "organisation", parquet_dir / "organisation.parquet", uprn_filter)
    register_parquet_view(
        con, "delivery_point", parquet_dir / "delivery_point.parquet", uprn_filter
    )
    register_parquet_view(
        con, "classification", parquet_dir / "classification.parquet", uprn_filter
    )

    # Street descriptor: register unfiltered, then filter by USRNs present in chunk's LPI
    register_parquet_view(con, "street_descriptor", parquet_dir / "street_descriptor.parquet")

    # Create global parent_uprns_with_children lookup for hierarchy_level fix
    # This must scan ALL BLPUs to correctly identify parents whose children may be in other chunks
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE parent_uprns_with_children AS
        SELECT DISTINCT parent_uprn AS uprn
        FROM read_parquet('{(parquet_dir / "blpu.parquet").as_posix()}')
        WHERE parent_uprn IS NOT NULL
    """)

    # Log parent count (useful for debugging)
    parent_count = con.execute("SELECT COUNT(*) FROM parent_uprns_with_children").fetchone()[0]
    logger.debug("Found %d parent UPRNs with children (global)", parent_count)

    # Prepare macros and intermediate tables
    t0 = perf_counter()
    create_macros(con)

    # Create view of USRNs present in this chunk's LPI records
    # When num_chunks=1, this contains all USRNs (no-op filter)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW usrns_in_chunk AS
        SELECT DISTINCT usrn, language
        FROM lpi
        WHERE usrn IS NOT NULL
    """)
    lpi.prepare_street_descriptor_views(con, usrn_filter_view="usrns_in_chunk")

    lpi.prepare_lpi_base(con)
    postal.prepare_best_delivery(con)
    misc.prepare_classification_best(con)
    logger.debug("Preparation completed in %.2f seconds", perf_counter() - t0)

    # Render variants
    stages = [
        ("LPI variants", lpi.render_variants),
        ("Business variants", business.render_variants),
        ("Delivery point variants", postal.render_variants),
        ("Custom level variants", misc.render_custom_levels),
    ]

    for label, func in stages:
        t0 = perf_counter()
        func(con)
        logger.debug("%s rendered in %.2f seconds", label, perf_counter() - t0)

    # Combine and write
    t0 = perf_counter()
    result = combine.combine_and_dedupe(con)
    logger.debug("Combination and deduplication in %.2f seconds", perf_counter() - t0)

    # Get chunk metrics
    chunk_metrics = con.execute("SELECT COUNT(DISTINCT unique_id), COUNT(*) FROM result").fetchone()
    chunk_uprns = chunk_metrics[0]
    chunk_rows = chunk_metrics[1]

    # Write output
    t0 = perf_counter()
    if output_path.exists():
        output_path.unlink()
    result.write_parquet(output_path.as_posix())
    logger.debug("Parquet written in %.2f seconds", perf_counter() - t0)

    chunk_duration = perf_counter() - chunk_start
    logger.info(
        "Chunk %d/%d complete (rows=%d, uprns=%d) in %.2f seconds",
        chunk_id + 1,
        num_chunks,
        chunk_rows,
        chunk_uprns,
        chunk_duration,
    )

    return output_path


def transform_to_flatfile(
    settings: Settings,
    force: bool = False,
) -> list[Path]:
    """Transform split parquet files into flatfile(s) for address matching.

    This is the public entrypoint that handles chunking transparently.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        List of paths to the output parquet file(s).

    Raises:
        FileNotFoundError: If required input files are missing.
        ToFlatfileError: If transformation fails.
    """
    parquet_dir = settings.paths.parquet_dir / "raw"
    output_dir = settings.paths.output_dir
    num_chunks = settings.processing.num_chunks

    # Check inputs
    assert_inputs_exist(parquet_dir)

    # Log chunking configuration
    logger.info("Starting flatfile chunking: num_chunks=%d", num_chunks)

    total_start = perf_counter()
    output_paths: list[Path] = []

    for chunk_id in range(num_chunks):
        logger.info("Running chunk %d/%d", chunk_id + 1, num_chunks)
        chunk_path = _transform_to_flatfile_chunk(
            settings,
            chunk_id=chunk_id,
            num_chunks=num_chunks,
            force=force,
        )
        output_paths.append(chunk_path)

    total_duration = perf_counter() - total_start

    # Final summary
    if num_chunks == 1:
        # For single chunk, also report detailed statistics like before
        con = create_duckdb_connection(settings)
        output_path = output_paths[0]
        stats = con.execute(f"""
            SELECT COUNT(DISTINCT unique_id), COUNT(*)
            FROM read_parquet('{output_path.as_posix()}')
        """).fetchone()
        total_uprns = stats[0]
        total_rows = stats[1]
        variant_uplift_pct = (
            ((total_rows - total_uprns) / total_uprns * 100) if total_uprns > 0 else 0
        )
        logger.info(
            "Address Statistics - UPRNs: %d | Total Variants: %d | Variant Uplift: %.1f%%",
            total_uprns,
            total_rows,
            variant_uplift_pct,
        )
    else:
        logger.info(
            "Flatfile completed: wrote %d chunk files to %s",
            num_chunks,
            output_dir,
        )

    logger.info("Flatfile transformation completed in %.2f seconds", total_duration)

    return output_paths


def run_flatfile_step(settings: Settings, force: bool = False) -> list[Path]:
    """Run the flatfile step of the pipeline.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        List of paths to the output parquet file(s).
    """
    logger.info("Starting flatfile step...")
    return transform_to_flatfile(settings, force=force)
