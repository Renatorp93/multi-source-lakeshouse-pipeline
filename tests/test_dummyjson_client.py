from lakehouse.ingestion.sales.dummyjson import DummyJsonClient


def test_fetch_collection_paginates_until_total_is_reached(monkeypatch):
    responses = [
        {"users": [{"id": 1}, {"id": 2}], "total": 3},
        {"users": [{"id": 3}], "total": 3},
    ]

    def fake_get_json(_resource, _params):
        return responses.pop(0)

    monkeypatch.setattr(DummyJsonClient, "_get_json", lambda self, _resource, _params: fake_get_json(_resource, _params))
    client = DummyJsonClient("https://dummyjson.com")

    items = client.fetch_collection("users", "users", page_size=2)

    assert [item["id"] for item in items] == [1, 2, 3]
