from lakehouse.config.settings import get_settings, resolve_path


def test_settings_expose_default_project_and_connection_values():
    settings = get_settings()

    assert settings.project_name == "multi-source-lakehouse-pipeline"
    assert settings.spark.app_name == "lakehouse-local"
    assert settings.postgres.database == "lakehouse"


def test_resolve_path_uses_repo_root_for_relative_paths():
    resolved = resolve_path("./storage/bronze")

    assert resolved.name == "bronze"
    assert "multi-source-lakeshouse-pipeline" in str(resolved)
