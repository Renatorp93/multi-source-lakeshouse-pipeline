from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from lakehouse.bronze.sales import build_bronze_datasets
from lakehouse.config.settings import Settings, resolve_path
from lakehouse.silver.sales import SilverBuildResult, build_silver_sales_datasets


@dataclass(frozen=True)
class SilverPersistedResult:
    source_name: str
    batch_id: str
    dataset_paths: dict[str, Path]
    quality_results_path: Path
    dataset_counts: dict[str, int]
    quality_results_count: int


def persist_silver_result(
    silver_result: SilverBuildResult,
    settings: Settings,
    batch_id: str,
) -> SilverPersistedResult:
    silver_root = resolve_path(settings.silver_root)
    dataset_paths: dict[str, Path] = {}
    dataset_counts: dict[str, int] = {}

    for entity, records in silver_result.datasets.items():
        target_dir = silver_root / silver_result.source_name / entity
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{batch_id}.json"
        target_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        dataset_paths[entity] = target_path
        dataset_counts[entity] = len(records)

    quality_dir = silver_root / "monitoring" / "quality_results"
    quality_dir.mkdir(parents=True, exist_ok=True)
    quality_results_path = quality_dir / f"{batch_id}.json"
    quality_results_path.write_text(
        json.dumps(silver_result.quality_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return SilverPersistedResult(
        source_name=silver_result.source_name,
        batch_id=batch_id,
        dataset_paths=dataset_paths,
        quality_results_path=quality_results_path,
        dataset_counts=dataset_counts,
        quality_results_count=len(silver_result.quality_results),
    )


def build_and_persist_sales_silver(
    settings: Settings,
    bronze_builder: Callable[[Settings], tuple[str, dict[str, dict[str, list[dict[str, Any]]]]]] = build_bronze_datasets,
    silver_builder: Callable[..., SilverBuildResult] = build_silver_sales_datasets,
    persister: Callable[[SilverBuildResult, Settings, str], SilverPersistedResult] = persist_silver_result,
) -> SilverPersistedResult:
    if bronze_builder is build_bronze_datasets:
        batch_id, bronze_datasets = bronze_builder(settings, on_source_error="skip")
    else:
        batch_id, bronze_datasets = bronze_builder(settings)
    silver_result = silver_builder(
        bronze_datasets,
        pipeline_run_id=settings.pipeline_run_id,
        preferred_source="postgres",
    )
    return persister(silver_result, settings, batch_id=batch_id)
