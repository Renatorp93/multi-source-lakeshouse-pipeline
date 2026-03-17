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
