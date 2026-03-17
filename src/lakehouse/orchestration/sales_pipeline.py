from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PipelineTaskDefinition:
    task_id: str
    bash_command: str


@dataclass(frozen=True)
class PipelineDefinition:
    dag_id: str
    schedule: str | None
    catchup: bool
    tags: tuple[str, ...]
    default_args: dict[str, Any]
    project_root: str
    env: dict[str, str]
    tasks: tuple[PipelineTaskDefinition, ...]


def build_sales_pipeline_definition(project_root: str = "/opt/project") -> PipelineDefinition:
    quoted_project_root = shlex.quote(project_root)
    env = {"PYTHONPATH": f"{project_root}/src"}

    tasks = (
        PipelineTaskDefinition(
            task_id="sync_sales_sources",
            bash_command=f"cd {quoted_project_root} && python scripts/sync_sales_sources.py",
        ),
        PipelineTaskDefinition(
            task_id="load_sales_bronze",
            bash_command=f"cd {quoted_project_root} && python scripts/load_sales_bronze.py",
        ),
        PipelineTaskDefinition(
            task_id="build_sales_silver",
            bash_command=f"cd {quoted_project_root} && python scripts/build_sales_silver.py",
        ),
        PipelineTaskDefinition(
            task_id="build_sales_gold",
            bash_command=f"cd {quoted_project_root} && python scripts/build_sales_gold.py",
        ),
    )

    return PipelineDefinition(
        dag_id="sales_medallion_pipeline",
        schedule="@daily",
        catchup=False,
        tags=("lakehouse", "sales", "medallion"),
        default_args={
            "owner": "lakehouse",
            "retries": 1,
        },
        project_root=project_root,
        env=env,
        tasks=tasks,
    )


def build_sales_pipeline_dag(
    dag_cls: Any,
    bash_operator_cls: Any,
    project_root: str = "/opt/project",
    definition: PipelineDefinition | None = None,
) -> tuple[Any, list[Any]]:
    definition = definition or build_sales_pipeline_definition(project_root=project_root)

    if not definition.tasks:
        raise ValueError("Nenhuma tarefa foi definida para a DAG de vendas.")

    dag = dag_cls(
        dag_id=definition.dag_id,
        schedule=definition.schedule,
        start_date=datetime(2026, 3, 17),
        catchup=definition.catchup,
        default_args=definition.default_args,
        tags=list(definition.tags),
    )

    tasks = [
        bash_operator_cls(
            task_id=task.task_id,
            bash_command=task.bash_command,
            env=definition.env,
            append_env=True,
            dag=dag,
        )
        for task in definition.tasks
    ]

    for current_task, next_task in zip(tasks, tasks[1:]):
        current_task.set_downstream(next_task)

    return dag, tasks
