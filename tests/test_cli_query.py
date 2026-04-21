"""Tests for the query/query-offline/lookup/inspect CLI commands."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import polars as pl
from typer.testing import CliRunner

from thyme.cli import app
from thyme.result import ThymeResult

runner = CliRunner()

REF = "tests.fixtures.sample_features:UserFeatures"
DATASET_REF = "tests.fixtures.sample_features:Purchase"


def _result(df: pl.DataFrame, run_id: str | None = "run-abc123") -> ThymeResult:
    return ThymeResult(df, metadata={"mode": "online"}, query_run_id=run_id)


class TestQueryCommand:
    """thyme query FEATURESET -e ID"""

    def test_single_entity_calls_query(self):
        # given
        with patch("thyme.cli.ThymeClient") as ClientCls:
            client = ClientCls.return_value
            client.query.return_value = _result(
                pl.DataFrame([{"user_id": 1, "total_spend": 99.5, "purchase_count": 3}])
            )

            # when
            result = runner.invoke(app, ["query", REF, "-e", "user_1"])

        # then
        assert result.exit_code == 0, result.output
        client.query.assert_called_once()
        # single-entity path, not batch
        assert client.query_batch.call_count == 0
        assert "99.5" in result.output

    def test_multiple_entities_call_batch(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            client = ClientCls.return_value
            client.query_batch.return_value = _result(
                pl.DataFrame([{"user_id": i} for i in (1, 2)])
            )

            result = runner.invoke(app, ["query", REF, "-e", "u1", "-e", "u2"])

        assert result.exit_code == 0, result.output
        client.query_batch.assert_called_once()
        assert client.query.call_count == 0

    def test_comma_split_entities_go_to_batch(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            client = ClientCls.return_value
            client.query_batch.return_value = _result(pl.DataFrame())

            result = runner.invoke(app, ["query", REF, "-e", "u1,u2,u3"])

        assert result.exit_code == 0, result.output
        # 3 entities → batch path
        args, _ = client.query_batch.call_args
        assert args[1] == ["u1", "u2", "u3"]

    def test_missing_entity_exits_1(self):
        with patch("thyme.cli.ThymeClient"):
            result = runner.invoke(app, ["query", REF])
        assert result.exit_code == 1
        assert "at least one --entity" in result.output

    def test_bad_ref_exits_1(self):
        with patch("thyme.cli.ThymeClient"):
            result = runner.invoke(app, ["query", "not-a-valid-ref", "-e", "u1"])
        assert result.exit_code == 1
        assert "expected 'module:Class'" in result.output

    def test_auth_error_exits_1(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            resp = httpx.Response(401, text="unauthorized", request=httpx.Request("GET", "http://x"))
            ClientCls.return_value.query.side_effect = httpx.HTTPStatusError("401", request=resp.request, response=resp)

            result = runner.invoke(app, ["query", REF, "-e", "u1"])

        assert result.exit_code == 1
        assert "401" in result.output

    def test_connect_error_exits_1(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.query.side_effect = httpx.ConnectError("refused")
            result = runner.invoke(app, ["query", REF, "-e", "u1"])
        assert result.exit_code == 1
        assert "could not connect" in result.output.lower()

    def test_prints_results_footer_with_frontend_url(self):
        with patch("thyme.cli.ThymeClient") as ClientCls, \
             patch("thyme.cli._resolve_config") as cfg_fn:
            cfg = MagicMock()
            cfg.query_url = "http://q"
            cfg.api_key = ""
            cfg.query_run_url.return_value = "https://ui.example/query-runs/run-abc123"
            cfg_fn.return_value = cfg

            ClientCls.return_value.query.return_value = _result(
                pl.DataFrame([{"user_id": 1}]), run_id="run-abc123"
            )

            result = runner.invoke(app, ["query", REF, "-e", "u1"])

        assert result.exit_code == 0, result.output
        assert "Query run: run-abc123" in result.output
        assert "https://ui.example/query-runs/run-abc123" in result.output

    def test_omits_url_when_frontend_url_unset(self):
        with patch("thyme.cli.ThymeClient") as ClientCls, \
             patch("thyme.cli._resolve_config") as cfg_fn:
            cfg = MagicMock()
            cfg.query_url = "http://q"
            cfg.api_key = ""
            cfg.query_run_url.return_value = None  # no URL
            cfg_fn.return_value = cfg

            ClientCls.return_value.query.return_value = _result(
                pl.DataFrame([{"user_id": 1}]), run_id="run-xyz"
            )

            result = runner.invoke(app, ["query", REF, "-e", "u1"])

        assert result.exit_code == 0
        assert "Query run: run-xyz" in result.output
        assert "Results:" not in result.output

    def test_silent_when_backend_returns_no_run_id(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.query.return_value = _result(
                pl.DataFrame([{"user_id": 1}]), run_id=None
            )
            result = runner.invoke(app, ["query", REF, "-e", "u1"])
        assert result.exit_code == 0
        assert "Query run:" not in result.output
        assert "Results:" not in result.output


class TestQueryOfflineCommand:
    def test_rejects_both_input_and_entity(self, tmp_path):
        p = tmp_path / "r.csv"
        p.write_text("entity_id,timestamp\nu1,2026-04-01T00:00:00Z\n")
        with patch("thyme.cli.ThymeClient"):
            result = runner.invoke(
                app,
                [
                    "query-offline",
                    REF,
                    "--input",
                    str(p),
                    "-e",
                    "u1",
                    "--at",
                    "2026-04-01T00:00:00Z",
                ],
            )
        assert result.exit_code == 1
        assert "either --input OR --entity" in result.output

    def test_requires_one_of_input_or_entity(self):
        with patch("thyme.cli.ThymeClient"):
            result = runner.invoke(app, ["query-offline", REF])
        assert result.exit_code == 1
        assert "--input FILE" in result.output or "--entity" in result.output

    def test_entity_count_must_match_at_count(self):
        with patch("thyme.cli.ThymeClient"):
            result = runner.invoke(
                app,
                [
                    "query-offline",
                    REF,
                    "-e",
                    "u1",
                    "-e",
                    "u2",
                    "--at",
                    "2026-04-01T00:00:00Z",
                ],
            )
        assert result.exit_code == 1
        assert "count" in result.output.lower()

    def test_with_input_file(self, tmp_path):
        p = tmp_path / "r.csv"
        p.write_text("entity_id,timestamp\nu1,2026-04-01T00:00:00Z\nu2,2026-04-01T00:00:00Z\n")

        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.query_offline.return_value = _result(
                pl.DataFrame([{"user_id": 1, "total_spend": 10.0}])
            )
            result = runner.invoke(app, ["query-offline", REF, "--input", str(p)])

        assert result.exit_code == 0, result.output
        ClientCls.return_value.query_offline.assert_called_once()

    def test_with_flags(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.query_offline.return_value = _result(pl.DataFrame())
            result = runner.invoke(
                app,
                [
                    "query-offline",
                    REF,
                    "-e",
                    "u1",
                    "--at",
                    "2026-04-01T00:00:00Z",
                ],
            )
        assert result.exit_code == 0, result.output
        ClientCls.return_value.query_offline.assert_called_once()


class TestLookupCommand:
    def test_looks_up_single_entity(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.lookup.return_value = _result(
                pl.DataFrame([{"user_id": 1, "amount": 99.5}])
            )
            result = runner.invoke(app, ["lookup", DATASET_REF, "-e", "u1"])

        assert result.exit_code == 0, result.output
        ClientCls.return_value.lookup.assert_called_once()

    def test_passes_timestamp_through(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.lookup.return_value = _result(pl.DataFrame())
            result = runner.invoke(
                app,
                ["lookup", DATASET_REF, "-e", "u1", "--at", "2026-04-01T00:00:00Z"],
            )
        assert result.exit_code == 0, result.output
        _, kwargs = ClientCls.return_value.lookup.call_args
        assert kwargs["timestamp"] == "2026-04-01T00:00:00Z"

    def test_requires_dataset_not_featureset(self):
        with patch("thyme.cli.ThymeClient"):
            # UserFeatures is a featureset, not a dataset — should error
            result = runner.invoke(app, ["lookup", REF, "-e", "u1"])
        assert result.exit_code == 1
        assert "@dataset" in result.output


class TestInspectCommand:
    def test_system_wide(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.inspect.return_value = {
                "datasets": [{"name": "Purchase", "version": 1}],
                "featuresets": [{"name": "UserFeatures", "feature_count": 3}],
            }
            result = runner.invoke(app, ["inspect"])
        assert result.exit_code == 0, result.output
        assert "Purchase" in result.output
        assert "UserFeatures" in result.output

    def test_specific_featureset(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.inspect.return_value = {
                "name": "UserFeatures",
                "version": 1,
                "features": [
                    {"name": "total_spend", "dtype": "float", "id": 2},
                ],
                "extractors": [],
            }
            result = runner.invoke(app, ["inspect", REF])
        assert result.exit_code == 0, result.output
        assert "UserFeatures" in result.output
        assert "total_spend" in result.output

    def test_json_output(self):
        with patch("thyme.cli.ThymeClient") as ClientCls:
            ClientCls.return_value.inspect.return_value = {"featuresets": []}
            result = runner.invoke(app, ["inspect", "--json"])
        assert result.exit_code == 0, result.output
        # output must be parseable JSON
        import json as _json
        _json.loads(result.output)
