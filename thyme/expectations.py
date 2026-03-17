"""Pre-built data expectation functions and @expectations decorator.

Expectations are declarative and structured (not arbitrary Python callables),
so the engine can evaluate them in pure Rust without PyO3 overhead per record.
Violations are purely observational — they emit metrics and logs but never drop records.
"""


def expect_column_values_to_be_between(column, min_value=None, max_value=None, mostly=1.0):
    return {
        "type": "column_values_between",
        "column": column,
        "min_value": min_value,
        "max_value": max_value,
        "mostly": mostly,
    }


def expect_column_values_to_not_be_null(column, mostly=1.0):
    return {
        "type": "column_values_not_null",
        "column": column,
        "mostly": mostly,
    }


def expect_column_values_to_be_in_set(column, values, mostly=1.0):
    return {
        "type": "column_values_in_set",
        "column": column,
        "values": values,
        "mostly": mostly,
    }


def expect_column_values_to_be_of_type(column, type_name, mostly=1.0):
    return {
        "type": "column_values_of_type",
        "column": column,
        "type_name": type_name,
        "mostly": mostly,
    }


def expectations(func):
    """Decorator that marks a dataset method as providing expectations."""
    func._is_expectations = True
    return func
