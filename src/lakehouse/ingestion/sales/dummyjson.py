from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class DummyJsonClient:
    base_url: str
    timeout_seconds: int = 30

    def fetch_sales_payloads(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "users": self.fetch_collection("users", "users"),
            "products": self.fetch_collection("products", "products"),
            "carts": self.fetch_collection("carts", "carts"),
        }

    def fetch_collection(self, resource: str, root_key: str, page_size: int = 100) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        skip = 0
        total = None

        while total is None or skip < total:
            payload = self._get_json(resource, {"limit": page_size, "skip": skip})
            page_items = payload.get(root_key, [])
            items.extend(page_items)
            total = payload.get("total", len(items))
            if not page_items:
                break
            skip += len(page_items)

        return items

    def _get_json(self, resource: str, params: dict[str, Any]) -> dict[str, Any]:
        query = urlencode(params)
        url = f"{self.base_url.rstrip('/')}/{resource}?{query}"
        request = Request(
            url,
            headers={
                "User-Agent": "multi-source-lakehouse-pipeline/0.1",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
