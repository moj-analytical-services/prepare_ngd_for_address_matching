from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

from ukam_os_builder._exceptions import PipelineError

COMMON_BLOAT_PATTERNS: tuple[str, ...] = (
    "**/*consolidated*.parquet",
    "**/*cleansed*.parquet",
)


@dataclass(frozen=True)
class PipelineStep:
    """Callable step in a pipeline with a stable name."""

    name: str
    runner: Callable[[Any, bool, bool], None]


def make_download_step(runner: Callable[[Any, bool, bool], None]) -> PipelineStep:
    """Create a download step which supports list-only mode."""
    return PipelineStep(name="download", runner=runner)


def make_standard_step(name: str, runner: Callable[[Any, bool], None]) -> PipelineStep:
    """Create a non-download step that ignores list-only mode."""

    def _wrapped(settings: Any, force: bool, _list_only: bool) -> None:
        runner(settings, force)

    return PipelineStep(name=name, runner=_wrapped)


@dataclass(frozen=True)
class PipelineDefinition:
    """Dataset-specific pipeline definition consumed by the shared executor."""

    dataset_name: str
    steps: tuple[PipelineStep, ...]
    clean_patterns: dict[str, list[str]]
    step_outputs: dict[str, list[str]]


def _clean_directory(directory: Path, patterns: list[str], *, logger: logging.Logger) -> int:
    if not directory.exists():
        return 0

    deleted = 0
    for pattern in patterns:
        for file_path in directory.glob(pattern):
            if file_path.is_file():
                file_path.unlink()
                deleted += 1
                logger.debug("Deleted: %s", file_path)

    return deleted


def _clean_outputs_for_step(
    *,
    step: Literal["all", "download"],
    settings: Any,
    definition: PipelineDefinition,
    logger: logging.Logger,
) -> None:
    dirs_to_clean = definition.step_outputs.get(step, [])
    if not dirs_to_clean:
        return

    total_deleted = 0
    for dir_name in dirs_to_clean:
        directory = getattr(settings.paths, dir_name)

        try:
            directory.relative_to(settings.paths.work_dir)
        except ValueError:
            logger.warning(
                "Refusing to clean %s - not under work_dir %s",
                directory,
                settings.paths.work_dir,
            )
            continue

        patterns = definition.clean_patterns.get(dir_name, [])
        if patterns:
            deleted = _clean_directory(directory, patterns, logger=logger)
            if deleted > 0:
                logger.info("Cleaned %d files from %s", deleted, directory)
            total_deleted += deleted

    if total_deleted > 0:
        logger.info("Total files cleaned: %d", total_deleted)


def run_pipeline(
    *,
    definition: PipelineDefinition,
    step: Literal["all", "download"],
    settings: Any,
    force: bool = False,
    list_only: bool = False,
    logger: logging.Logger,
) -> None:
    """Run pipeline using shared control flow and dataset-specific step factories."""
    valid_steps = {"all", "download"}
    # Only allow users to leverage "download" and "all" steps for pipeline runs
    # This simplifies the task of interacting with the pipeline and should encompass
    # most common use cases (e.g. re-running the download step, or re-running the entire pipeline)
    if step not in valid_steps:
        raise PipelineError(
            f"Invalid step: {step}. Must be one of: {', '.join(sorted(valid_steps))}"
        )

    step_runners = {pipeline_step.name: pipeline_step.runner for pipeline_step in definition.steps}
    total_start = perf_counter()

    logger.info("=" * 60)
    logger.info("%s Pipeline - Starting step: %s", definition.dataset_name.upper(), step)
    logger.info("=" * 60)
    logger.info("Config: %s", settings.config_path)
    logger.info("Work directory: %s", settings.paths.work_dir)
    logger.info("Force mode: %s", force)
    logger.info("")

    if step == "all" and list_only:
        if "download" not in step_runners:
            raise PipelineError("list_only requires a download step")
        step_runners["download"](settings, force, True)
        return

    run_order = (
        [pipeline_step.name for pipeline_step in definition.steps] if step == "all" else [step]
    )
    for step_name in run_order:
        if force:
            _clean_outputs_for_step(
                step=step_name,
                settings=settings,
                definition=definition,
                logger=logger,
            )

        t0 = perf_counter()
        step_runners[step_name](settings, force, list_only if step_name == "download" else False)
        logger.info("%s step completed in %.2f seconds", step_name.title(), perf_counter() - t0)

        if step == "all" and step_name != run_order[-1]:
            logger.info("")

    total_duration = perf_counter() - total_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline step '%s' completed in %.2f seconds", step, total_duration)
    logger.info("=" * 60)
