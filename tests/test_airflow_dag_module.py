from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def test_sales_medallion_dag_module_builds_dag_with_airflow_contract():
    dag_path = Path("dags/sales_medallion_pipeline.py").resolve()

    fake_airflow = types.ModuleType("airflow")
    fake_bash_module = types.ModuleType("airflow.operators.bash")
    fake_operators = types.ModuleType("airflow.operators")

    class FakeDag:
        def __init__(self, dag_id, schedule, start_date, catchup, default_args, tags):
            self.dag_id = dag_id
            self.schedule = schedule
            self.start_date = start_date
            self.catchup = catchup
            self.default_args = default_args
            self.tags = tags

    class FakeBashOperator:
        def __init__(self, task_id, bash_command, env, append_env, dag, trigger_rule="all_success"):
            self.task_id = task_id
            self.bash_command = bash_command
            self.env = env
            self.append_env = append_env
            self.dag = dag
            self.trigger_rule = trigger_rule

        def set_downstream(self, other):
            self.downstream = other.task_id

    fake_airflow.DAG = FakeDag
    fake_bash_module.BashOperator = FakeBashOperator

    sys.modules["airflow"] = fake_airflow
    sys.modules["airflow.operators"] = fake_operators
    sys.modules["airflow.operators.bash"] = fake_bash_module

    try:
        spec = importlib.util.spec_from_file_location("sales_medallion_pipeline", dag_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None
        assert spec.loader is not None
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop("airflow", None)
        sys.modules.pop("airflow.operators", None)
        sys.modules.pop("airflow.operators.bash", None)

    assert module.dag.dag_id == "sales_medallion_pipeline"
    assert [task.task_id for task in module.airflow_tasks] == [
        "sync_sales_sources",
        "load_sales_bronze",
        "build_sales_silver",
        "check_sales_quality",
        "build_sales_gold",
        "alert_quality_failure",
    ]
