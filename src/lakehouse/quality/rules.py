from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class QualityResult:
    pipeline_run_id: str
    dataset_name: str
    layer: str
    rule_name: str
    rule_type: str
    status: str
    failed_records: int
    total_records: int
    failure_rate: float
    execution_timestamp: str


def validate_not_null(
    records: list[dict[str, Any]],
    field_name: str,
    dataset_name: str,
    layer: str,
    pipeline_run_id: str,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    failed_records = sum(1 for record in records if record.get(field_name) in (None, ""))
    return _build_result(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name=f"not_null_{field_name}",
        rule_type="content",
        failed_records=failed_records,
        total_records=len(records),
        execution_timestamp=execution_timestamp,
    )


def validate_unique(
    records: list[dict[str, Any]],
    field_names: list[str],
    dataset_name: str,
    layer: str,
    pipeline_run_id: str,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    seen: set[tuple[Any, ...]] = set()
    failed_records = 0

    for record in records:
        key = tuple(record.get(field_name) for field_name in field_names)
        if key in seen:
            failed_records += 1
        else:
            seen.add(key)

    return _build_result(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name=f"unique_{'_'.join(field_names)}",
        rule_type="content",
        failed_records=failed_records,
        total_records=len(records),
        execution_timestamp=execution_timestamp,
    )


def validate_allowed_reference(
    records: list[dict[str, Any]],
    field_name: str,
    allowed_values: Iterable[Any],
    dataset_name: str,
    layer: str,
    pipeline_run_id: str,
    rule_name: str,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    allowed = set(allowed_values)
    failed_records = sum(1 for record in records if record.get(field_name) not in allowed)
    return _build_result(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name=rule_name,
        rule_type="relational",
        failed_records=failed_records,
        total_records=len(records),
        execution_timestamp=execution_timestamp,
    )


def validate_positive(
    records: list[dict[str, Any]],
    field_name: str,
    dataset_name: str,
    layer: str,
    pipeline_run_id: str,
    rule_name: str | None = None,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    failed_records = sum(
        1
        for record in records
        if record.get(field_name) is None or _to_float(record.get(field_name)) <= 0
    )
    return _build_result(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name=rule_name or f"positive_{field_name}",
        rule_type="content",
        failed_records=failed_records,
        total_records=len(records),
        execution_timestamp=execution_timestamp,
    )


def validate_discount_not_greater_than_gross(
    records: list[dict[str, Any]],
    dataset_name: str,
    layer: str,
    pipeline_run_id: str,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    failed_records = 0
    for record in records:
        gross_amount = record.get("gross_amount")
        discounted_amount = record.get("discounted_amount")
        if gross_amount is None or discounted_amount is None:
            continue
        if _to_float(discounted_amount) > _to_float(gross_amount):
            failed_records += 1

    return _build_result(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name="discount_not_greater_than_gross",
        rule_type="content",
        failed_records=failed_records,
        total_records=len(records),
        execution_timestamp=execution_timestamp,
    )


def quality_result_to_dict(result: QualityResult) -> dict[str, Any]:
    return asdict(result)


def _build_result(
    pipeline_run_id: str,
    dataset_name: str,
    layer: str,
    rule_name: str,
    rule_type: str,
    failed_records: int,
    total_records: int,
    execution_timestamp: datetime | None = None,
) -> QualityResult:
    execution_timestamp = execution_timestamp or datetime.now(timezone.utc)
    failure_rate = 0.0 if total_records == 0 else failed_records / total_records
    return QualityResult(
        pipeline_run_id=pipeline_run_id,
        dataset_name=dataset_name,
        layer=layer,
        rule_name=rule_name,
        rule_type=rule_type,
        status="passed" if failed_records == 0 else "failed",
        failed_records=failed_records,
        total_records=total_records,
        failure_rate=failure_rate,
        execution_timestamp=execution_timestamp.isoformat(),
    )


def _to_float(value: Any) -> float:
    return float(value)
