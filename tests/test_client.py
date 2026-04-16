"""Tests for thyme.client — ThymeClient lifecycle SDK."""

import json

import httpx
import polars as pl
import pytest

from thyme.client import ThymeClient
from thyme.config import Config
from thyme.result import ThymeResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_transport(responses: dict[str, httpx.Response]) -> httpx.MockTransport:
    """Build a mock transport that matches on URL path."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path in responses:
            return responses[path]
        return httpx.Response(404, json={"error": "not found"})

    return httpx.MockTransport(handler)


def _make_featureset_class(name: str, features: list[dict]) -> type:
    """Create a fake featureset class with _featureset_meta attached."""
    cls = type(name, (), {})
    cls._featureset_meta = {
        "name": name,
        "features": features,
        "extractors": [],
    }
    return cls


# ---------------------------------------------------------------------------
# ThymeClient construction
# ---------------------------------------------------------------------------


class TestThymeClientConstruction:
    """Given a Config, when constructing ThymeClient, then it initializes correctly."""

    def test_construct_with_default_config(self):
        config = Config(query_url="http://localhost:8081")
        client = ThymeClient(config=config)
        assert client is not None

    def test_construct_uses_config_query_url(self):
        config = Config(query_url="http://custom:9999")
        client = ThymeClient(config=config)
        assert client._config.query_url == "http://custom:9999"


# ---------------------------------------------------------------------------
# query() — single entity online
# ---------------------------------------------------------------------------


class TestQuery:
    """Given a featureset and entity_id, when calling query(), then features are returned."""

    def test_query_returns_thyme_result(self):
        # given
        fs = _make_featureset_class("UserFeatures", [
            {"name": "age", "dtype": "int", "id": 1},
            {"name": "score", "dtype": "float", "id": 2},
        ])
        mock_response = httpx.Response(200, json={
            "entity_type": "UserFeatures",
            "entity_id": "user_1",
            "features": {"age": 30, "score": 4.5},
            "mode": "featureset",
        })
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query(fs, "user_1")

        # then
        assert isinstance(result, ThymeResult)
        assert len(result) == 1

    def test_query_returns_correct_values(self):
        # given
        fs = _make_featureset_class("UserFeatures", [
            {"name": "age", "dtype": "int", "id": 1},
            {"name": "score", "dtype": "float", "id": 2},
        ])
        mock_response = httpx.Response(200, json={
            "entity_type": "UserFeatures",
            "entity_id": "user_1",
            "features": {"age": 30, "score": 4.5},
            "mode": "featureset",
        })
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query(fs, "user_1")

        # then
        dicts = result.to_dict()
        assert dicts[0]["age"] == 30
        assert dicts[0]["score"] == 4.5

    def test_query_applies_correct_polars_schema(self):
        # given
        fs = _make_featureset_class("Flags", [
            {"name": "is_active", "dtype": "bool", "id": 1},
            {"name": "name", "dtype": "str", "id": 2},
        ])
        mock_response = httpx.Response(200, json={
            "entity_type": "Flags",
            "entity_id": "e1",
            "features": {"is_active": True, "name": "alice"},
            "mode": "featureset",
        })
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query(fs, "e1")

        # then
        df = result.to_polars()
        assert df.schema["is_active"] == pl.Boolean
        assert df.schema["name"] == pl.Utf8

    def test_query_includes_metadata(self):
        # given
        fs = _make_featureset_class("F", [
            {"name": "x", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(200, json={
            "entity_type": "F",
            "entity_id": "e1",
            "features": {"x": 1.0},
            "mode": "online",
        })
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query(fs, "e1")

        # then
        assert result.metadata["entity_id"] == "e1"
        assert result.metadata["mode"] == "online"

    def test_query_sends_correct_params(self):
        # given
        fs = _make_featureset_class("MyFS", [
            {"name": "v", "dtype": "float", "id": 1},
        ])
        captured_requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(200, json={
                "entity_type": "MyFS",
                "entity_id": "e1",
                "features": {"v": 1.0},
                "mode": "featureset",
            })

        transport = httpx.MockTransport(handler)
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        client.query(fs, "e1")

        # then
        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.url.params["featureset"] == "MyFS"
        assert req.url.params["entity_id"] == "e1"

    def test_query_raises_on_http_error(self):
        # given
        fs = _make_featureset_class("F", [
            {"name": "x", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(500, json={"error": "internal"})
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when / then
        with pytest.raises(Exception):
            client.query(fs, "e1")

    def test_query_raises_on_404_featureset(self):
        # given
        fs = _make_featureset_class("Missing", [
            {"name": "x", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(404, json={
            "error": "featureset 'Missing' not found",
        })
        transport = _mock_transport({"/features": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when / then
        with pytest.raises(Exception):
            client.query(fs, "e1")


# ---------------------------------------------------------------------------
# query_batch() — multiple entities
# ---------------------------------------------------------------------------


class TestQueryBatch:
    """Given a featureset and entity_ids, when calling query_batch(), then batch results returned."""

    def test_query_batch_returns_thyme_result(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(200, json={
            "results": [
                {"entity_type": "UF", "entity_id": "e1", "features": {"score": 1.0}, "mode": "featureset"},
                {"entity_type": "UF", "entity_id": "e2", "features": {"score": 2.0}, "mode": "featureset"},
                {"entity_type": "UF", "entity_id": "e3", "features": {"score": 3.0}, "mode": "featureset"},
            ]
        })
        transport = _mock_transport({"/features/batch": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query_batch(fs, ["e1", "e2", "e3"])

        # then
        assert isinstance(result, ThymeResult)
        assert len(result) == 3

    def test_query_batch_returns_correct_values(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(200, json={
            "results": [
                {"entity_type": "UF", "entity_id": "e1", "features": {"score": 1.5}, "mode": "featureset"},
                {"entity_type": "UF", "entity_id": "e2", "features": {"score": 2.5}, "mode": "featureset"},
            ]
        })
        transport = _mock_transport({"/features/batch": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query_batch(fs, ["e1", "e2"])

        # then
        dicts = result.to_dict()
        assert dicts[0]["score"] == 1.5
        assert dicts[1]["score"] == 2.5

    def test_query_batch_sends_post_with_body(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "v", "dtype": "float", "id": 1},
        ])
        captured_requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(200, json={
                "results": [
                    {"entity_type": "UF", "entity_id": "e1", "features": {"v": 1.0}, "mode": "featureset"},
                ]
            })

        transport = httpx.MockTransport(handler)
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        client.query_batch(fs, ["e1"])

        # then
        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.method == "POST"
        body = json.loads(req.content)
        assert body["featureset"] == "UF"
        assert body["entity_ids"] == ["e1"]

    def test_query_batch_applies_schema(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "count", "dtype": "int", "id": 1},
            {"name": "ratio", "dtype": "float", "id": 2},
        ])
        mock_response = httpx.Response(200, json={
            "results": [
                {"entity_type": "UF", "entity_id": "e1", "features": {"count": 5, "ratio": 0.8}, "mode": "featureset"},
            ]
        })
        transport = _mock_transport({"/features/batch": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query_batch(fs, ["e1"])

        # then
        df = result.to_polars()
        assert df.schema["count"] == pl.Int64
        assert df.schema["ratio"] == pl.Float64

    def test_query_batch_empty_ids_returns_empty_result(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "v", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(200, json={"results": []})
        transport = _mock_transport({"/features/batch": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query_batch(fs, [])

        # then
        assert len(result) == 0

    def test_query_batch_raises_on_http_error(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "v", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(500, json={"error": "boom"})
        transport = _mock_transport({"/features/batch": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when / then
        with pytest.raises(Exception):
            client.query_batch(fs, ["e1"])


# ---------------------------------------------------------------------------
# Auth headers
# ---------------------------------------------------------------------------


class TestAuthHeaders:
    """Given a config with api_key, when making requests, then auth headers are sent."""

    def test_query_sends_auth_header(self):
        # given
        fs = _make_featureset_class("F", [
            {"name": "x", "dtype": "float", "id": 1},
        ])
        captured_requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(200, json={
                "entity_type": "F",
                "entity_id": "e1",
                "features": {"x": 1.0},
                "mode": "featureset",
            })

        transport = httpx.MockTransport(handler)
        config = Config(query_url="http://test:8081", api_key="secret-key-123")
        client = ThymeClient(config=config, _transport=transport)

        # when
        client.query(fs, "e1")

        # then
        assert captured_requests[0].headers["authorization"] == "Bearer secret-key-123"


# ---------------------------------------------------------------------------
# query_offline() — point-in-time batch extraction
# ---------------------------------------------------------------------------


def _offline_mock_transport() -> httpx.MockTransport:
    """Mock transport that returns offline query results matching the request body."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        results = []
        for pair in body.get("queries", []):
            results.append({
                "entity_id": pair["entity_id"],
                "timestamp": pair["timestamp"],
                "features": {"score": 1.0},
            })
        return httpx.Response(200, json={"results": results})

    return httpx.MockTransport(handler)


class TestQueryOffline:
    """Given entities with timestamps, when calling query_offline(), then PIT features returned."""

    def test_query_offline_returns_thyme_result(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        transport = _offline_mock_transport()
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)
        entities = [
            {"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"},
        ]

        # when
        result = client.query_offline(
            fs, entities, entity_column="entity_id", timestamp_column="timestamp",
        )

        # then
        assert isinstance(result, ThymeResult)
        assert len(result) == 1

    def test_query_offline_returns_correct_values(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        transport = _offline_mock_transport()
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)
        entities = [
            {"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"},
            {"entity_id": "e2", "timestamp": "2026-01-02T00:00:00Z"},
        ]

        # when
        result = client.query_offline(
            fs, entities, entity_column="entity_id", timestamp_column="timestamp",
        )

        # then
        dicts = result.to_dict()
        assert len(dicts) == 2
        assert dicts[0]["score"] == 1.0
        assert dicts[1]["score"] == 1.0

    def test_query_offline_preserves_entity_columns(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        transport = _offline_mock_transport()
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)
        entities = [
            {"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"},
        ]

        # when
        result = client.query_offline(
            fs, entities, entity_column="entity_id", timestamp_column="timestamp",
        )

        # then
        df = result.to_polars()
        assert "entity_id" in df.columns
        assert "timestamp" in df.columns
        assert df["entity_id"][0] == "e1"

    def test_query_offline_accepts_polars_input(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        transport = _offline_mock_transport()
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)
        entities_df = pl.DataFrame({
            "entity_id": ["e1", "e2"],
            "timestamp": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"],
        })

        # when
        result = client.query_offline(
            fs, entities_df, entity_column="entity_id", timestamp_column="timestamp",
        )

        # then
        assert len(result) == 2

    def test_query_offline_accepts_list_of_dicts(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        transport = _offline_mock_transport()
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        result = client.query_offline(
            fs,
            [{"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"}],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then
        assert len(result) == 1

    def test_query_offline_sends_correct_request_body(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        captured = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(json.loads(request.content))
            return httpx.Response(200, json={"results": [
                {"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z", "features": {"score": 1.0}},
            ]})

        transport = httpx.MockTransport(handler)
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when
        client.query_offline(
            fs,
            [{"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"}],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then
        assert len(captured) == 1
        body = captured[0]
        assert body["featureset"] == "UF"
        assert body["queries"] == [
            {"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"},
        ]

    def test_query_offline_raises_on_http_error(self):
        # given
        fs = _make_featureset_class("UF", [
            {"name": "score", "dtype": "float", "id": 1},
        ])
        mock_response = httpx.Response(500, json={"error": "boom"})
        transport = _mock_transport({"/features/offline": mock_response})
        config = Config(query_url="http://test:8081")
        client = ThymeClient(config=config, _transport=transport)

        # when / then
        with pytest.raises(Exception):
            client.query_offline(
                fs,
                [{"entity_id": "e1", "timestamp": "2026-01-01T00:00:00Z"}],
                entity_column="entity_id",
                timestamp_column="timestamp",
            )
