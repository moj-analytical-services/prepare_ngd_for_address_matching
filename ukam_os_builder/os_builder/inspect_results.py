from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import duckdb
import yaml

SourceType = Literal["ngd", "abp"]

logger = logging.getLogger(__name__)

_DEFAULT_SELECT_COLUMNS = [
    "unique_id",
    "address_concat",
    "postcode",
    "source",
    "variant_label",
    "is_primary",
    "classification_code",
    "udprn",
]


def _resolve_path(base_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _read_config_for_output(config_path: Path) -> tuple[Path, SourceType]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid config format: {config_path}")

    source_value = str((raw.get("source") or {}).get("type") or "ngd").strip().lower()
    if source_value not in {"ngd", "abp"}:
        raise ValueError("config source.type must be one of: ngd, abp")

    paths = raw.get("paths") or {}
    if not isinstance(paths, dict):
        raise ValueError("paths must be a mapping in config")

    work_dir_raw = str(paths.get("work_dir", "./data"))
    output_dir_raw = str(paths.get("output_dir", Path(work_dir_raw) / "output"))

    output_dir = _resolve_path(config_path.parent, output_dir_raw)
    return output_dir, source_value  # type: ignore[return-value]


def _pattern_for_source(source: SourceType) -> str:
    if source == "abp":
        return "abp_for_uk_address_matcher*.parquet"
    return "ngd_for_uk_address_matcher*.parquet"


def _resolve_runtime_context(
    *,
    config_path: str | Path,
    source: SourceType | None,
    output_dir: str | Path | None,
) -> tuple[SourceType, Path, str, list[Path]]:
    config_path = Path(config_path).resolve()

    if output_dir is None or source is None:
        resolved_output_dir, resolved_source = _read_config_for_output(config_path)
        output_dir_path = Path(output_dir).resolve() if output_dir else resolved_output_dir
        source_value = source or resolved_source
    else:
        output_dir_path = Path(output_dir).resolve()
        source_value = source

    pattern = _pattern_for_source(source_value)
    output_files = sorted(output_dir_path.glob(pattern))
    if not output_files:
        raise FileNotFoundError(
            f"No flatfile outputs found in {output_dir_path} matching {pattern}"
        )

    files_sql = (output_dir_path / pattern).as_posix()
    return source_value, output_dir_path, files_sql, output_files


def _choose_select_columns(
    con: duckdb.DuckDBPyConnection,
    files_sql: str,
    requested_columns: list[str] | None = None,
) -> str:
    available_columns = con.sql(f"SELECT * FROM read_parquet('{files_sql}') LIMIT 0").columns
    desired = requested_columns or _DEFAULT_SELECT_COLUMNS
    selected = [column for column in desired if column in available_columns]
    if not selected:
        return "*"
    return ",\n            ".join(selected)


def get_flatfile(
    con: duckdb.DuckDBPyConnection,
    *,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
) -> duckdb.DuckDBPyRelation:
    """Load the full flatfile relation for the configured source."""
    _, _, files_sql, _ = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )
    return con.read_parquet(files_sql)


def get_variant_statistics(
    con: duckdb.DuckDBPyConnection,
    *,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, float | int | None]:
    """Calculate variant-count statistics grouped by UPRN."""
    _, _, files_sql, _ = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )

    stats = con.sql(f"""
        WITH variant_counts AS (
            SELECT unique_id, COUNT(*) AS variant_count
            FROM read_parquet('{files_sql}')
            GROUP BY unique_id
        )
        SELECT
            COUNT(*) AS total_uprns,
            SUM(variant_count) AS total_variants,
            AVG(variant_count) AS mean_variants,
            MEDIAN(variant_count) AS median_variants,
            MIN(variant_count) AS min_variants,
            MAX(variant_count) AS max_variants
        FROM variant_counts
    """).fetchone()

    if not stats:
        return {
            "total_uprns": 0,
            "total_variants": 0,
            "mean_variants": 0.0,
            "median_variants": 0,
            "min_variants": 0,
            "max_variants": 0,
        }

    return {
        "total_uprns": int(stats[0] or 0),
        "total_variants": int(stats[1] or 0),
        "mean_variants": round(float(stats[2] or 0), 2),
        "median_variants": int(stats[3] or 0),
        "min_variants": int(stats[4] or 0),
        "max_variants": int(stats[5] or 0),
    }


def get_random_uprn(
    con: duckdb.DuckDBPyConnection,
    *,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
    columns: list[str] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return all variants for a randomly selected UPRN."""
    _, _, files_sql, _ = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )

    select_columns = _choose_select_columns(con, files_sql, columns)
    random_uprn = con.sql(f"""
        SELECT DISTINCT unique_id
        FROM read_parquet('{files_sql}')
        ORDER BY RANDOM()
        LIMIT 1
    """).fetchone()

    if not random_uprn:
        raise RuntimeError("No records found in flatfile outputs")

    return con.sql(f"""
        SELECT
            {select_columns}
        FROM read_parquet('{files_sql}')
        WHERE unique_id = {int(random_uprn[0])}
        ORDER BY is_primary DESC NULLS LAST, source NULLS LAST, variant_label NULLS LAST
    """)


def get_random_large_uprn(
    con: duckdb.DuckDBPyConnection,
    *,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
    top_n: int = 100,
    filter_clause: str | None = None,
    columns: list[str] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return variants for a random UPRN selected from the top N by variant count."""
    _, _, files_sql, _ = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )

    where_filter = f"WHERE {filter_clause}" if filter_clause else ""
    and_filter = f"AND {filter_clause}" if filter_clause else ""
    select_columns = _choose_select_columns(con, files_sql, columns)

    selected = con.sql(f"""
        WITH variant_counts AS (
            SELECT unique_id, COUNT(*) AS variant_count
            FROM read_parquet('{files_sql}')
            {where_filter}
            GROUP BY unique_id
            ORDER BY variant_count DESC, unique_id ASC
            LIMIT {int(top_n)}
        )
        SELECT unique_id
        FROM variant_counts
        ORDER BY RANDOM()
        LIMIT 1
    """).fetchone()

    if not selected:
        raise RuntimeError("No records found for the requested top_n/filter combination")

    return con.sql(f"""
        SELECT
            {select_columns}
        FROM read_parquet('{files_sql}')
        WHERE unique_id = {int(selected[0])}
        {and_filter}
        ORDER BY is_primary DESC NULLS LAST, source NULLS LAST, variant_label NULLS LAST
    """)


def get_uprn_variants(
    con: duckdb.DuckDBPyConnection,
    *,
    uprn: int,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
    filter_clause: str | None = None,
    columns: list[str] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return all variants for the provided UPRN."""
    _, _, files_sql, _ = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )
    select_columns = _choose_select_columns(con, files_sql, columns)
    and_filter = f"AND {filter_clause}" if filter_clause else ""

    return con.sql(f"""
        SELECT
            {select_columns}
        FROM read_parquet('{files_sql}')
        WHERE unique_id = {int(uprn)}
        {and_filter}
        ORDER BY is_primary DESC NULLS LAST, source NULLS LAST, variant_label NULLS LAST
    """)


def inspect_flatfile_variants(
    *,
    config_path: str | Path = "config.yaml",
    source: SourceType | None = None,
    output_dir: str | Path | None = None,
    target_uprn: int | None = None,
    top_offset: int = 0,
    sample_limit: int = 5,
    show: bool = True,
) -> dict[str, Any]:
    """Inspect flatfile output and return summary for high-variant UPRNs.

    If ``target_uprn`` is provided, details are returned for that UPRN.
    Otherwise, the UPRN with the highest variant count at ``top_offset`` is selected.
    """
    if top_offset < 0:
        raise ValueError("top_offset must be >= 0")
    if sample_limit < 1:
        raise ValueError("sample_limit must be >= 1")

    source, output_dir, files_sql, output_files = _resolve_runtime_context(
        config_path=config_path,
        source=source,
        output_dir=output_dir,
    )
    pattern = _pattern_for_source(source)
    con = duckdb.connect()

    sample_rows = con.execute(
        f"""
        SELECT *
        FROM read_parquet('{files_sql}')
        LIMIT {sample_limit}
        """
    ).fetchall()

    if target_uprn is None:
        top = con.execute(
            f"""
            WITH data AS (
                SELECT * FROM read_parquet('{files_sql}')
            )
            SELECT unique_id, COUNT(*) AS variant_count
            FROM data
            GROUP BY unique_id
            ORDER BY variant_count DESC, unique_id ASC
            LIMIT 1 OFFSET {top_offset}
            """
        ).fetchone()
        if not top:
            raise RuntimeError("No records found in flatfile outputs")
        target_uprn = int(top[0])
        variant_count = int(top[1])
    else:
        count_row = con.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{files_sql}')
            WHERE unique_id = ?
            """,
            [target_uprn],
        ).fetchone()
        variant_count = int(count_row[0] if count_row else 0)

    detail_rows = con.execute(
        f"""
        SELECT *
        FROM read_parquet('{files_sql}')
        WHERE unique_id = ?
        ORDER BY 1
        """,
        [target_uprn],
    ).fetchall()

    if show:
        logger.info("Source: %s", source)
        logger.info("Matched files: %d", len(output_files))
        logger.info("Selected UPRN: %s (variants: %d)", target_uprn, variant_count)
        logger.info("Sample rows:")
        con.sql(f"SELECT * FROM read_parquet('{files_sql}') LIMIT {sample_limit}").show(
            max_width=10_000
        )
        logger.info("Selected UPRN rows:")
        con.sql(f"SELECT * FROM read_parquet('{files_sql}') WHERE unique_id = {target_uprn}").show(
            max_width=10_000
        )

    return {
        "source": source,
        "output_dir": output_dir,
        "pattern": pattern,
        "files": output_files,
        "selected_uprn": target_uprn,
        "variant_count": variant_count,
        "sample_rows": sample_rows,
        "uprn_rows": detail_rows,
    }
