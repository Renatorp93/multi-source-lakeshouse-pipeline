from lakehouse.orchestration.sales_pipeline import PipelineDefinition, build_sales_pipeline_definition, build_sales_pipeline_dag


def test_build_sales_pipeline_definition_quotes_project_root_with_spaces():
    definition = build_sales_pipeline_definition(project_root="/opt/project with spaces")

    assert definition.tasks[0].bash_command == "cd '/opt/project with spaces' && python scripts/sync_sales_sources.py"
    assert definition.env["PYTHONPATH"] == "/opt/project with spaces/src"


def test_build_sales_pipeline_dag_rejects_empty_task_list():
    class FakeDag:
        def __init__(self, *args, **kwargs):
            pass

    class FakeBashOperator:
        def __init__(self, *args, **kwargs):
            pass

    definition = PipelineDefinition(
        dag_id="empty_pipeline",
        schedule="@daily",
        catchup=False,
        tags=("lakehouse",),
        default_args={"owner": "lakehouse"},
        project_root="/opt/project",
        env={"PYTHONPATH": "/opt/project/src"},
        tasks=(),
    )

    try:
        build_sales_pipeline_dag(FakeDag, FakeBashOperator, definition=definition)
    except ValueError as exc:
        assert "Nenhuma tarefa" in str(exc)
    else:
        raise AssertionError("Era esperado falhar quando a DAG nao possui tarefas.")


def test_build_sales_pipeline_dag_rejects_unknown_downstream_task():
    class FakeDag:
        def __init__(self, *args, **kwargs):
            pass

    class FakeBashOperator:
        def __init__(self, task_id, *args, **kwargs):
            self.task_id = task_id

        def set_downstream(self, other):
            del other

    definition = PipelineDefinition(
        dag_id="invalid_pipeline",
        schedule="@daily",
        catchup=False,
        tags=("lakehouse",),
        default_args={"owner": "lakehouse"},
        project_root="/opt/project",
        env={"PYTHONPATH": "/opt/project/src"},
        tasks=(
            build_sales_pipeline_definition(project_root="/opt/project").tasks[0],
        ),
    )

    invalid_task = definition.tasks[0].__class__(
        task_id=definition.tasks[0].task_id,
        bash_command=definition.tasks[0].bash_command,
        trigger_rule=definition.tasks[0].trigger_rule,
        downstream_task_ids=("missing_task",),
    )
    definition = PipelineDefinition(
        dag_id=definition.dag_id,
        schedule=definition.schedule,
        catchup=definition.catchup,
        tags=definition.tags,
        default_args=definition.default_args,
        project_root=definition.project_root,
        env=definition.env,
        tasks=(invalid_task,),
    )

    try:
        build_sales_pipeline_dag(FakeDag, FakeBashOperator, definition=definition)
    except ValueError as exc:
        assert "missing_task" in str(exc)
    else:
        raise AssertionError("Era esperado falhar quando uma dependencia aponta para uma tarefa inexistente.")
