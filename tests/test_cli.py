import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from thyme.cli import app
from thyme.dataset import clear_registry, get_commit_payload

runner = CliRunner()


# ---------------------------------------------------------------------------
# thyme status tests
# ---------------------------------------------------------------------------

MOCK_STATUS_RESPONSE = {
    "datasets": [{"name": "Review", "version": 1}],
    "pipelines": [{"name": "compute_stats", "version": 1, "input_datasets": ["Review"], "output_dataset": "Stats"}],
    "featuresets": [{"name": "RestaurantFeatures", "feature_count": 5}],
    "sources": [{"dataset": "Review", "connector_type": "jsonl"}],
    "jobs": [{"name": "compute_stats_job", "partition_count": 1}],
    "backfills": [],
    "latest_commit": {"id": "abc-123", "status": "committed", "created_at": "2024-01-01T00:00:00Z"},
    "recent_events": [
        {"id": "evt1", "event_type": "commit", "severity": "info", "source": "definition-service",
         "subject": "abc-123", "message": "Committed 1 dataset(s)", "detail": None, "created_at": "2024-01-01T00:00:00Z"},
    ],
}


def test_status_displays_summary():
    """thyme status displays dataset and pipeline names in a summary table."""
    # Given: a mock /api/v1/status endpoint returning definitions
    with patch("httpx.get") as mock_get:
        def side_effect(url, **kwargs):
            if "/api/v1/status" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = MOCK_STATUS_RESPONSE
                resp.raise_for_status = MagicMock()
                return resp
            if "/health" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"status": "ok"}
                return resp
            raise httpx.ConnectError("unexpected url")

        mock_get.side_effect = side_effect

        # When: we run thyme status
        result = runner.invoke(app, ["status"])

    # Then: output contains dataset and pipeline names
    assert result.exit_code == 0, result.output
    assert "Review" in result.output
    assert "compute_stats" in result.output


def test_status_json_flag():
    """thyme status --json produces valid JSON output."""
    # Given: a mock status endpoint
    with patch("httpx.get") as mock_get:
        def side_effect(url, **kwargs):
            if "/api/v1/status" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = MOCK_STATUS_RESPONSE
                resp.raise_for_status = MagicMock()
                return resp
            if "/health" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"status": "ok"}
                return resp
            raise httpx.ConnectError("unexpected url")

        mock_get.side_effect = side_effect

        # When: we run thyme status --json
        result = runner.invoke(app, ["status", "--json"])

    # Then: output is valid JSON with expected structure
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert "status" in parsed
    assert "services" in parsed
    assert parsed["status"]["datasets"][0]["name"] == "Review"


def test_status_connection_error():
    """thyme status shows a clean error when the service is unreachable."""
    # Given: the API is unreachable
    with patch("httpx.get", side_effect=httpx.ConnectError("Connection refused")):
        # When: we run thyme status
        result = runner.invoke(app, ["status"])

    # Then: clean error message, non-zero exit
    assert result.exit_code != 0
    assert "Could not connect" in result.output or "Connection" in result.output


def test_status_checks_service_health():
    """thyme status pings health endpoints and shows up/down status."""
    # Given: definition-service healthy, query-server unhealthy
    with patch("httpx.get") as mock_get:
        def side_effect(url, **kwargs):
            if "/api/v1/status" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = MOCK_STATUS_RESPONSE
                resp.raise_for_status = MagicMock()
                return resp
            if "8080/health" in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"status": "ok"}
                return resp
            if "8081/health" in url:
                raise httpx.ConnectError("refused")
            raise httpx.ConnectError("unexpected url")

        mock_get.side_effect = side_effect

        # When: we run thyme status
        result = runner.invoke(app, ["status"])

    # Then: shows health status for both services
    assert result.exit_code == 0, result.output
    assert "definition-service" in result.output.lower() or "Definition" in result.output
    assert "query-server" in result.output.lower() or "Query" in result.output


# ---------------------------------------------------------------------------
# thyme logs tests
# ---------------------------------------------------------------------------

MOCK_EVENTS_RESPONSE = [
    {"id": "evt1", "event_type": "commit", "severity": "info", "source": "definition-service",
     "subject": "abc-123", "message": "Committed 1 dataset(s)", "detail": None, "created_at": "2024-01-01T00:00:00Z"},
    {"id": "evt2", "event_type": "runner_spawn", "severity": "info", "source": "engine",
     "subject": "compute_stats_job", "message": "Spawning runner", "detail": None, "created_at": "2024-01-01T00:01:00Z"},
]


def test_logs_displays_events():
    """thyme logs displays event timeline."""
    # Given: a mock /api/v1/events endpoint
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = MOCK_EVENTS_RESPONSE
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        # When: we run thyme logs
        result = runner.invoke(app, ["logs"])

    # Then: output contains event information
    assert result.exit_code == 0, result.output
    assert "commit" in result.output.lower()
    assert "runner_spawn" in result.output.lower() or "runner" in result.output.lower()


def test_logs_limit_flag():
    """thyme logs --limit passes the limit as a query param."""
    # Given: a mock events endpoint
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = MOCK_EVENTS_RESPONSE
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        # When: we run thyme logs --limit 10
        result = runner.invoke(app, ["logs", "--limit", "10"])

    # Then: the limit is passed as a query param
    assert result.exit_code == 0, result.output
    call_kwargs = mock_get.call_args
    params = call_kwargs.kwargs.get("params", {})
    assert params.get("limit") == 10


def test_logs_severity_filter():
    """thyme logs --severity passes the filter as a query param."""
    # Given: a mock events endpoint
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        # When: we run thyme logs --severity error
        result = runner.invoke(app, ["logs", "--severity", "error"])

    # Then: the severity is passed as a query param
    assert result.exit_code == 0, result.output
    call_kwargs = mock_get.call_args
    params = call_kwargs.kwargs.get("params", {})
    assert params.get("severity") == "error"


def test_logs_json_flag():
    """thyme logs --json produces valid JSON output."""
    # Given: a mock events endpoint
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = MOCK_EVENTS_RESPONSE
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        # When: we run thyme logs --json
        result = runner.invoke(app, ["logs", "--json"])

    # Then: output is valid JSON
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert isinstance(parsed, list)
    assert len(parsed) == 2
    assert parsed[0]["event_type"] == "commit"


def test_logs_connection_error():
    """thyme logs shows a clean error when the service is unreachable."""
    # Given: the API is unreachable
    with patch("httpx.get", side_effect=httpx.ConnectError("Connection refused")):
        # When: we run thyme logs
        result = runner.invoke(app, ["logs"])

    # Then: clean error message, non-zero exit
    assert result.exit_code != 0
    assert "Could not connect" in result.output or "Connection" in result.output


def test_get_commit_payload_shape(sample_dataset):
    # Given: a registered dataset (sample_dataset fixture)
    # When: we call get_commit_payload
    payload = get_commit_payload()

    # Then: payload has all four keys (full commit request shape)
    assert "datasets" in payload
    assert isinstance(payload["datasets"], list)
    assert len(payload["datasets"]) == 1
    assert "pipelines" in payload
    assert isinstance(payload["pipelines"], list)
    assert "featuresets" in payload
    assert isinstance(payload["featuresets"], list)
    assert "sources" in payload
    assert isinstance(payload["sources"], list)
    dataset_schema = payload["datasets"][0]
    assert dataset_schema["name"] == "SampleDataset"
    assert dataset_schema["version"] == 1
    assert dataset_schema["index"] is True
    assert "fields" in dataset_schema
    assert "dependencies" in dataset_schema


def test_get_commit_payload_multiple_datasets(make_dataset):
    # Given: multiple datasets registered
    make_dataset("DatasetA", index=True, version=1)
    make_dataset("DatasetB", index=False, version=2)

    # When: we call get_commit_payload
    payload = get_commit_payload()

    # Then: both datasets are in the list; full payload shape present
    names = {d["name"] for d in payload["datasets"]}
    assert names == {"DatasetA", "DatasetB"}
    assert "pipelines" in payload and isinstance(payload["pipelines"], list)
    assert "featuresets" in payload and isinstance(payload["featuresets"], list)
    assert "sources" in payload and isinstance(payload["sources"], list)


def test_commit_dry_run_with_module_prints_json():
    # Given: a test module that registers datasets
    clear_registry()

    # When: we run thyme commit -m tests.fixtures.sample_features --dry-run
    result = runner.invoke(app, ["commit", "-m", "tests.fixtures.sample_features", "--dry-run"])

    # Then: command succeeds and stdout contains valid JSON with full commit shape
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert "datasets" in parsed
    assert len(parsed["datasets"]) == 1
    assert parsed["datasets"][0]["name"] == "Purchase"
    assert "pipelines" in parsed and isinstance(parsed["pipelines"], list)
    assert "featuresets" in parsed and isinstance(parsed["featuresets"], list)
    assert "sources" in parsed and isinstance(parsed["sources"], list)


def test_commit_dry_run_with_path_prints_json(tmp_path):
    # Given: a feature module file on disk
    feature_file = tmp_path / "features.py"
    feature_file.write_text("""\
from datetime import datetime
from thyme.dataset import dataset, field

@dataset(index=True, version=1)
class Purchase:
    user_id: int = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)
""")
    clear_registry()

    # When: we run thyme commit with file path --dry-run
    result = runner.invoke(app, ["commit", str(feature_file), "--dry-run"])

    # Then: command succeeds and stdout contains valid JSON with full commit shape
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed["datasets"][0]["name"] == "Purchase"
    assert "pipelines" in parsed and "featuresets" in parsed and "sources" in parsed


def test_commit_dry_run_with_output_writes_file(tmp_path):
    # Given: a test module and an output path
    output_file = tmp_path / "payload.json"
    clear_registry()

    # When: we run thyme commit -m tests.fixtures.sample_features --dry-run --output <path>
    result = runner.invoke(
        app,
        ["commit", "-m", "tests.fixtures.sample_features", "--dry-run", "--output", str(output_file)],
    )

    # Then: payload is written to the file with full commit shape
    assert result.exit_code == 0
    assert output_file.exists()
    parsed = json.loads(output_file.read_text())
    assert parsed["datasets"][0]["name"] == "Purchase"
    assert "pipelines" in parsed and "featuresets" in parsed and "sources" in parsed


def test_commit_requires_module_or_path():
    # Given: no module or path provided
    # When: we run thyme commit
    result = runner.invoke(app, ["commit"])

    # Then: command fails with error
    assert result.exit_code != 0
    assert "Provide either" in result.stdout or "Provide either" in result.stderr


def test_commit_rejects_both_module_and_path():
    # Given: both -m and path provided
    # When: we run thyme commit -m x path
    result = runner.invoke(app, ["commit", "-m", "foo", "bar.py"])

    # Then: command fails with error
    assert result.exit_code != 0
    assert "not both" in result.stdout or "not both" in result.stderr


def test_commit_dry_run_full_payload_restaurant_reviews():
    """Commit with restaurant review example produces datasets, pipelines, featuresets, sources."""
    examples_dir = Path(__file__).resolve().parent.parent.parent / "examples"
    features_file = examples_dir / "restaurant_reviews" / "features.py"
    if not features_file.exists():
        pytest.skip("examples/restaurant_reviews/features.py not found")
    clear_registry()

    result = runner.invoke(app, ["commit", str(features_file), "--dry-run"])

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert "datasets" in parsed
    assert len(parsed["datasets"]) == 2  # Review, RestaurantRatingStats
    assert "pipelines" in parsed
    assert len(parsed["pipelines"]) == 1  # compute_stats
    assert "featuresets" in parsed
    assert len(parsed["featuresets"]) == 1  # RestaurantFeatures
    assert "sources" in parsed
    assert len(parsed["sources"]) == 1  # Review source
    assert parsed["pipelines"][0]["name"] == "compute_stats"
    assert parsed["pipelines"][0]["output_dataset"] == "RestaurantRatingStats"
    assert parsed["featuresets"][0]["name"] == "RestaurantFeatures"


def test_compile_commit_request_produces_serializable_bytes():
    """compile_commit_request returns a proto message that serializes to non-empty bytes."""
    # Given: the restaurant_reviews fixture is loaded
    examples_dir = Path(__file__).resolve().parent.parent.parent / "examples"
    features_file = examples_dir / "restaurant_reviews" / "features.py"
    if not features_file.exists():
        pytest.skip("examples/restaurant_reviews/features.py not found")
    clear_registry()

    # When: we build the commit payload and compile to protobuf
    from thyme.cli import _import_module_by_path
    _import_module_by_path(features_file)
    payload = get_commit_payload()
    from thyme.compiler import compile_commit_request
    proto_msg = compile_commit_request(
        message="test",
        datasets=payload["datasets"],
        pipelines=payload["pipelines"],
        featuresets=payload["featuresets"],
        sources=payload["sources"],
    )
    serialized = proto_msg.SerializeToString()

    # Then: serialized bytes are non-empty and re-parse correctly
    assert isinstance(serialized, bytes)
    assert len(serialized) > 0
    from thyme.gen import services_pb2
    reparsed = services_pb2.CommitRequest()
    reparsed.ParseFromString(serialized)
    assert len(reparsed.datasets) == 2
    assert len(reparsed.pipelines) == 1


def test_cli_commit_sends_protobuf_content_type():
    """CLI commit POSTs with Content-Type: application/protobuf by default."""
    # Given: a feature module and a mock HTTP transport
    examples_dir = Path(__file__).resolve().parent.parent.parent / "examples"
    features_file = examples_dir / "restaurant_reviews" / "features.py"
    if not features_file.exists():
        pytest.skip("examples/restaurant_reviews/features.py not found")
    clear_registry()

    captured_requests = []

    def mock_transport(request):
        captured_requests.append(request)
        return httpx.Response(200, json={
            "commit_id": "00000000-0000-0000-0000-000000000000",
            "datasets": 2,
            "pipelines": 1,
            "featuresets": 1,
            "sources": 1,
            "jobs_created": 1,
            "topics_created": [],
        })

    # When: we run thyme commit with a mock HTTP client
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=200,
            raise_for_status=MagicMock(return_value=None),
        )
        result = runner.invoke(app, ["commit", str(features_file)])

    # Then: the CLI exits successfully and posts with protobuf content type
    assert result.exit_code == 0, result.output
    assert mock_post.called
    call_kwargs = mock_post.call_args
    headers = call_kwargs.kwargs.get("headers", {})
    assert headers.get("Content-Type") == "application/protobuf"
    # protobuf send uses content=, not json=
    assert "content" in call_kwargs.kwargs
    assert "json" not in call_kwargs.kwargs


def test_cli_commit_dry_run_still_outputs_json():
    """--dry-run always prints JSON, not protobuf bytes."""
    # Given: a feature module
    clear_registry()

    # When: we run thyme commit --dry-run
    result = runner.invoke(app, ["commit", "-m", "tests.fixtures.sample_features", "--dry-run"])

    # Then: output is valid JSON (not binary protobuf)
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert "datasets" in parsed
