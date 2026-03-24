from lakehouse.spark.session import ensure_java_available


def test_ensure_java_available_raises_when_java_is_missing(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr("lakehouse.spark.session.shutil.which", lambda _: None)

    try:
        ensure_java_available()
    except RuntimeError as error:
        assert "Java nao encontrado" in str(error)
    else:
        raise AssertionError("Era esperado um erro quando Java nao esta configurado.")
