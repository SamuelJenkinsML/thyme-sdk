import pytest

from thyme.pipeline import pipeline, inputs, Avg, Count, Sum, Last, LastK, PipelineNode


def test_avg_op_has_correct_type():
    op = Avg(of="rating", window="30d")
    assert op.agg_type == "avg"
    assert op.of == "rating"
    assert op.window == "30d"


def test_count_op_has_correct_type():
    op = Count(window="7d")
    assert op.agg_type == "count"
    assert op.window == "7d"


def test_sum_op_has_correct_type():
    op = Sum(of="amount", window="24h")
    assert op.agg_type == "sum"


def test_pipeline_node_groupby():
    node = PipelineNode("Review")
    grouped = node.groupby("restaurant_id")
    assert grouped._group_keys == ["restaurant_id"]


def test_pipeline_node_aggregate():
    node = PipelineNode("Review")
    result = node.groupby("restaurant_id").aggregate(
        avg_rating=Avg(of="rating", window="30d"),
        review_count=Count(window="30d"),
    )
    assert len(result._agg_specs) == 2
    assert result._agg_specs[0]["type"] == "avg"
    assert result._agg_specs[0]["field"] == "rating"
    assert result._agg_specs[0]["output_field"] == "avg_rating"
    assert result._agg_specs[1]["type"] == "count"
    assert result._agg_specs[1]["output_field"] == "review_count"


def test_last_op_has_correct_type():
    op = Last(of="geo", window="180d")
    assert op.agg_type == "last"
    assert op.of == "geo"
    assert op.dropnull is False


def test_lastk_op_has_correct_type_and_defaults():
    op = LastK(of="product_code", window="3h")
    assert op.agg_type == "last_k"
    assert op.k == 100
    assert op.dedup is False
    assert op.dropnull is False


def test_lastk_rejects_k_below_one():
    with pytest.raises(ValueError):
        LastK(of="product_code", window="3h", k=0)


def test_aggregate_serializes_last_and_lastk():
    node = PipelineNode("Events")
    result = node.groupby("user_id").aggregate(
        last_geo=Last(of="geo", window="180d", dropnull=True),
        recent=LastK(of="product_code", window="3h", k=50, dedup=True),
    )
    last_spec = result._agg_specs[0]
    assert last_spec["type"] == "last"
    assert last_spec["k"] == 1  # Last is implicitly k=1
    assert last_spec["dropnull"] is True

    lastk_spec = result._agg_specs[1]
    assert lastk_spec["type"] == "last_k"
    assert lastk_spec["k"] == 50
    assert lastk_spec["dedup"] is True
    assert lastk_spec["dropnull"] is False


def test_pipeline_decorator_marks_function():
    @pipeline(version=2)
    def compute(cls, data):
        return data

    assert compute._is_pipeline is True
    assert compute._pipeline_version == 2


def test_inputs_decorator_sets_datasets():
    class Review:
        pass

    @inputs(Review)
    def compute(cls, data):
        return data

    assert compute._pipeline_inputs == ["Review"]


def test_agg_op_has_no_lateness_config():
    """Lateness is configured at the source level (max_lateness), not per-aggregation."""
    op = Avg(of="rating", window="30d")
    assert not hasattr(op, "allowed_lateness")
    assert not hasattr(op, "max_lateness")
