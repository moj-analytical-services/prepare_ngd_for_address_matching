from __future__ import annotations

import logging
from typing import Literal

from ukam_os_builder.api.settings import Settings
from ukam_os_builder.data_sources.abp.split_raw import run_split_step
from ukam_os_builder.data_sources.abp.to_flatfile import run_flatfile_step as run_abp_flatfile_step
from ukam_os_builder.data_sources.ngd.to_flatfile import run_flatfile_step as run_ngd_flatfile_step
from ukam_os_builder.os_builder.extract import run_extract_step
from ukam_os_builder.os_builder.os_hub import run_download_step
from ukam_os_builder.os_builder.pipeline_factory import (
    COMMON_BLOAT_PATTERNS,
    PipelineDefinition,
    make_download_step,
    make_standard_step,
    run_pipeline,
)

SourceType = Literal["ngd", "abp"]

logger = logging.getLogger(__name__)


def run_abp_extract_step(settings: Settings, force: bool = False) -> list[object]:
    return run_extract_step(settings=settings, force=force, convert_to_parquet=False)


_NGD_PIPELINE = PipelineDefinition(
    dataset_name="ngd",
    steps=(
        make_download_step(run_download_step),
        make_standard_step("extract", run_extract_step),
        make_standard_step("flatfile", run_ngd_flatfile_step),
    ),
    clean_patterns={
        "extracted_dir": [
            "*/*.csv",
            "parquet/*.parquet",
            *COMMON_BLOAT_PATTERNS,
        ],
        "output_dir": ["*.parquet"],
    },
    step_outputs={
        "download": [],
        "extract": ["extracted_dir"],
        "flatfile": ["output_dir"],
    },
)


_ABP_PIPELINE = PipelineDefinition(
    dataset_name="abp",
    steps=(
        make_download_step(run_download_step),
        make_standard_step("extract", run_abp_extract_step),
        make_standard_step("split", run_split_step),
        make_standard_step("flatfile", run_abp_flatfile_step),
    ),
    clean_patterns={
        "extracted_dir": [
            "*_csv/*.csv",
            *COMMON_BLOAT_PATTERNS,
        ],
        "parquet_dir": [
            "raw/*.parquet",
            *COMMON_BLOAT_PATTERNS,
        ],
        "output_dir": ["*.parquet"],
    },
    step_outputs={
        "download": [],
        "extract": ["extracted_dir"],
        "split": ["parquet_dir"],
        "flatfile": ["output_dir"],
    },
)


_PIPELINES_BY_SOURCE: dict[SourceType, PipelineDefinition] = {
    "ngd": _NGD_PIPELINE,
    "abp": _ABP_PIPELINE,
}


def supported_steps_for_source(source: SourceType) -> set[str]:
    definition = _definition_for_source(source)
    return {pipeline_step.name for pipeline_step in definition.steps}


def _definition_for_source(source: SourceType) -> PipelineDefinition:
    try:
        return _PIPELINES_BY_SOURCE[source]
    except KeyError as exc:
        raise ValueError("Source must be one of: ngd, abp") from exc


def run(
    step: str,
    settings: Settings,
    force: bool = False,
    list_only: bool = False,
) -> None:
    source = settings.source.type
    definition = _definition_for_source(source)

    run_pipeline(
        definition=definition,
        step=step,
        settings=settings,
        force=force,
        list_only=list_only,
        logger=logger,
    )
