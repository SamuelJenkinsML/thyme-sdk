"""Extractor execution on the offline path (TH-312).

The offline path must return the same features as `POST /features/offline`, so
the calling convention here is a faithful port of `generate_wrapper` /
`strip_decorators` in `crates/query-server/src/executor.rs`: decorators stripped,
`entry(None, None, *inputs)` positionally in declared order, a missing input
arriving as `None`, and a single output wrapped in a dict keyed by its name.

Query-server calls that wrapper **per row, with scalars**. That is why row-wise
is the default here rather than an oversight — see TestVectorisedDivergence.
"""

import polars as pl
import pytest

from thyme.offline_extractors import (
    apply_extractors,
    build_wrapper,
    strip_decorators,
)


SUSPICIOUS_SRC = '''\
@extractor
@extractor_inputs("order_count_1h", "total_spend_24h")
@extractor_outputs("is_suspicious")
def compute_suspicious(cls, ts, count_1h, spend_24h):
    if count_1h is None or spend_24h is None:
        return False
    return (count_1h > 5) | (spend_24h > 100.0)
'''

META = {
    "name": "UserFeatures",
    "features": [
        {"name": "order_count_1h", "dtype": "float"},
        {"name": "total_spend_24h", "dtype": "float"},
        {"name": "is_suspicious", "dtype": "bool", "deps": ["order_count_1h"]},
    ],
    "extractors": [
        {
            "name": "compute_suspicious",
            "kind": "PY_FUNC",
            "inputs": ["order_count_1h", "total_spend_24h"],
            "outputs": ["is_suspicious"],
            "source_code": SUSPICIOUS_SRC,
        }
    ],
}


class TestStripDecorators:
    """Ported from executor.rs; the port is only useful if it agrees."""

    def test_drops_decorator_lines_above_the_def(self):
        out = strip_decorators(SUSPICIOUS_SRC)

        assert "@extractor" not in out
        assert out.lstrip().startswith("def compute_suspicious")

    def test_keeps_decorators_that_appear_after_the_first_def(self):
        # given a nested decorator inside the body
        src = "def f(cls, ts, x):\n    @staticmethod\n    def inner():\n        pass\n    return x\n"

        # then only decorators *above* the first def are stripped
        assert "@staticmethod" in strip_decorators(src)

    def test_drops_a_multi_line_decorator_argument_list(self):
        src = (
            "@extractor_inputs(\n"
            '    "a",\n'
            '    "b",\n'
            ")\n"
            "def f(cls, ts, a, b):\n"
            "    return a\n"
        )

        out = strip_decorators(src)

        assert out.lstrip().startswith("def f")
        assert '"a"' not in out.split("def f")[0]


class TestWrapperMatchesQueryServer:
    def test_calls_the_entry_point_with_cls_and_ts_as_none(self):
        wrapper = build_wrapper(SUSPICIOUS_SRC, "compute_suspicious",
                                ["order_count_1h", "total_spend_24h"],
                                ["is_suspicious"])

        assert "compute_suspicious(None, None, _arg_0, _arg_1)" in wrapper

    def test_inputs_are_looked_up_by_name_so_a_miss_becomes_none(self):
        wrapper = build_wrapper(SUSPICIOUS_SRC, "compute_suspicious",
                                ["order_count_1h"], ["is_suspicious"])

        # `.get` rather than `[]`: query-server passes whatever the plan
        # produced, and a missing input is None rather than a KeyError
        assert 'inputs.get("order_count_1h")' in wrapper

    def test_a_single_output_is_wrapped_in_a_dict(self):
        wrapper = build_wrapper(SUSPICIOUS_SRC, "compute_suspicious",
                                ["order_count_1h"], ["is_suspicious"])

        assert 'if not isinstance(_result, dict)' in wrapper
        assert '{"is_suspicious": _result}' in wrapper


class TestApplyExtractors:
    def test_adds_the_derived_column(self):
        df = pl.DataFrame(
            {
                "entity_id": ["c123", "c456"],
                "order_count_1h": [7.0, 1.0],
                "total_spend_24h": [10.0, 10.0],
            }
        )

        out = apply_extractors(df, META)

        assert out["is_suspicious"].to_list() == [True, False]

    def test_a_null_input_matches_the_endpoint(self):
        # given a dormant entity, which a training spine produces constantly
        df = pl.DataFrame(
            {
                "entity_id": ["c123"],
                "order_count_1h": [None],
                "total_spend_24h": [10.0],
            }
        )

        out = apply_extractors(df, META)

        # then the extractor's own None-guard runs, exactly as it does behind
        # /features/offline -- False, not null
        assert out["is_suspicious"].to_list() == [False]

    def test_no_extractors_leaves_the_frame_untouched(self):
        df = pl.DataFrame({"a": [1]})

        assert apply_extractors(df, {"features": [], "extractors": []}) is df


class TestVectorisedDivergence:
    """Why row-wise is the default.

    The fraud demo's extractor guards with `if x is None`, which is scalar
    logic. Called with a Series that guard silently does nothing, nulls
    propagate through the comparison, and the answer differs from the endpoint's
    on exactly the rows a training spine produces most.
    """

    def test_vectorised_execution_is_opt_in(self):
        df = pl.DataFrame(
            {"order_count_1h": [7.0], "total_spend_24h": [10.0]}
        )

        # the default path is row-wise
        assert apply_extractors(df, META)["is_suspicious"].to_list() == [True]

    def test_vectorised_and_row_wise_disagree_on_nulls(self):
        # given a spine row that resolved to null, typed as the column would be
        df = pl.DataFrame(
            {
                "order_count_1h": pl.Series([None, 7.0], dtype=pl.Float64),
                "total_spend_24h": pl.Series([10.0, 10.0], dtype=pl.Float64),
            }
        )

        row_wise = apply_extractors(df, META)["is_suspicious"].to_list()
        vectorised = apply_extractors(df, META, vectorized=True)[
            "is_suspicious"
        ].to_list()

        # then they differ on exactly the null row -- the extractor's scalar
        # None-guard returns False, while the Series path propagates null. This
        # is the whole reason vectorisation cannot be switched on for an
        # arbitrary extractor.
        assert row_wise == [False, True]
        assert vectorised == [None, True]

    def test_an_all_null_column_makes_vectorised_execution_fail_outright(self):
        # given a column Polars types as Null because nothing in it is typed
        df = pl.DataFrame({"order_count_1h": [None], "total_spend_24h": [None]})

        # then row-wise still answers, because the guard sees scalars
        assert apply_extractors(df, META)["is_suspicious"].to_list() == [False]

        # while vectorised raises -- divergence here is not even silent
        with pytest.raises(Exception):
            apply_extractors(df, META, vectorized=True)

    def test_vectorised_agrees_where_the_extractor_is_series_safe(self):
        # given inputs with no nulls, where the guard is irrelevant
        df = pl.DataFrame(
            {"order_count_1h": [7.0, 1.0], "total_spend_24h": [10.0, 10.0]}
        )

        assert (
            apply_extractors(df, META, vectorized=True)["is_suspicious"].to_list()
            == apply_extractors(df, META)["is_suspicious"].to_list()
        )
