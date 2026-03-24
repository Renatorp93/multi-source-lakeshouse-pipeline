from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lakehouse.config.settings import Settings, resolve_path


class QualityGateFailed(RuntimeError):
    """Raised when curated data quality does not meet the promotion criteria."""


@dataclass(frozen=True)
class QualityGateReport:
    batch_id: str
    quality_results_path: Path
    total_rules: int
    failed_rules: int
    failed_records: int
    failed_results: tuple[dict[str, Any], ...]


def build_quality_gate_report(
    settings: Settings,
    batch_id: str | None = None,
) -> QualityGateReport:
    quality_results_path = resolve_quality_results_path(settings, batch_id=batch_id)
    quality_results = load_quality_results(quality_results_path)
    failed_results = tuple(
        result
        for result in quality_results
        if str(result.get("status", "")).lower() != "passed"
    )

    return QualityGateReport(
        batch_id=quality_results_path.stem,
        quality_results_path=quality_results_path,
        total_rules=len(quality_results),
        failed_rules=len(failed_results),
        failed_records=sum(int(result.get("failed_records", 0) or 0) for result in failed_results),
        failed_results=failed_results,
    )


def enforce_quality_gate(report: QualityGateReport) -> QualityGateReport:
    if report.failed_rules:
        raise QualityGateFailed(format_quality_gate_summary(report))
    return report


def emit_quality_alert(settings: Settings, batch_id: str | None = None) -> str:
    report = build_quality_gate_report(settings, batch_id=batch_id)
    if not report.failed_rules:
        return (
            f"Alerta operacional ignorado: o batch {report.batch_id} nao possui falhas de qualidade abertas."
        )
    return format_quality_gate_summary(report, prefix="ALERTA")


def format_quality_gate_summary(
    report: QualityGateReport,
    prefix: str = "Quality gate",
) -> str:
    if not report.failed_rules:
        return f"{prefix} aprovado para o batch {report.batch_id}. Regras avaliadas: {report.total_rules}."

    lines = [
        (
            f"{prefix} bloqueou a promocao do batch {report.batch_id}. "
            f"Regras com falha: {report.failed_rules}/{report.total_rules}. "
            f"Registros impactados: {report.failed_records}."
        )
    ]
    for result in report.failed_results:
        dataset_name = result.get("dataset_name", "dataset_desconhecido")
        rule_name = result.get("rule_name", "regra_desconhecida")
        failed_records = result.get("failed_records", 0)
        lines.append(f"- {dataset_name} | {rule_name} | failed_records={failed_records}")
    return "\n".join(lines)


def resolve_quality_results_path(settings: Settings, batch_id: str | None = None) -> Path:
    quality_results_dir = resolve_path(settings.silver_root) / "monitoring" / "quality_results"
    if batch_id is not None:
        target_path = quality_results_dir / f"{batch_id}.json"
        if not target_path.exists():
            raise FileNotFoundError(
                f"Arquivo de quality_results nao encontrado para o batch {batch_id}: {target_path}"
            )
        return target_path

    candidates = sorted(path for path in quality_results_dir.glob("*.json") if path.is_file())
    if not candidates:
        raise FileNotFoundError(
            "Nenhum arquivo de quality_results foi encontrado em storage/silver/monitoring/quality_results."
        )
    return candidates[-1]


def load_quality_results(path: Path) -> list[dict[str, Any]]:
    quality_results = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(quality_results, list):
        raise ValueError(f"O arquivo de quality_results precisa conter uma lista de regras: {path}")
    return quality_results
