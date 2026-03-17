from lakehouse.orchestration.sales_pipeline import build_sales_pipeline_definition, build_sales_pipeline_dag


def test_build_sales_pipeline_definition_describes_expected_sales_flow():
    definition = build_sales_pipeline_definition(project_root="/opt/project")

    assert definition.dag_id == "sales_medallion_pipeline"
    assert definition.schedule == "@daily"
    assert definition.catchup is False
    assert definition.tags == ("lakehouse", "sales", "medallion")
    assert [task.task_id for task in definition.tasks] == [
        "sync_sales_sources",
        "load_sales_bronze",
        "build_sales_silver",
        "build_sales_gold",
    ]
    assert definition.tasks[0].bash_command == "cd /opt/project && python scripts/sync_sales_sources.py"
    assert definition.tasks[-1].bash_command == "cd /opt/project && python scripts/build_sales_gold.py"
    assert definition.env["PYTHONPATH"] == "/opt/project/src"


def test_build_sales_pipeline_dag_chains_tasks_in_sequence():
    captured = {}

    class FakeDag:
        def __init__(self, dag_id, schedule, start_date, catchup, default_args, tags):
            captured["dag"] = {
                "dag_id": dag_id,
                "schedule": schedule,
                "start_date": start_date,
                "catchup": catchup,
                "default_args": default_args,
                "tags": tags,
            }

    class FakeBashOperator:
        def __init__(self, task_id, bash_command, env, append_env, dag):
            self.task_id = task_id
            self.bash_command = bash_command
            self.env = env
            self.append_env = append_env
            self.dag = dag
            self.downstream_task_ids = []
            captured.setdefault("tasks", []).append(self)

        def set_downstream(self, other):
            self.downstream_task_ids.append(other.task_id)

    dag, tasks = build_sales_pipeline_dag(FakeDag, FakeBashOperator, project_root="/opt/project")

    assert dag is not None
    assert [task.task_id for task in tasks] == [
        "sync_sales_sources",
        "load_sales_bronze",
        "build_sales_silver",
        "build_sales_gold",
    ]
    assert tasks[0].downstream_task_ids == ["load_sales_bronze"]
    assert tasks[1].downstream_task_ids == ["build_sales_silver"]
    assert tasks[2].downstream_task_ids == ["build_sales_gold"]
    assert captured["dag"]["dag_id"] == "sales_medallion_pipeline"
    assert captured["dag"]["schedule"] == "@daily"
    assert tasks[0].append_env is True
    assert tasks[0].env["PYTHONPATH"] == "/opt/project/src"
