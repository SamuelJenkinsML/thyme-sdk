"""Expectation evaluation — port of Rust engine's expectation checks."""

from dataclasses import dataclass


@dataclass
class ExpectationViolation:
    """A single expectation violation for one record."""

    expectation_type: str
    column: str
    message: str
    record: dict


def evaluate_expectation(spec: dict, record: dict) -> bool:
    """Evaluate one expectation spec against one record. Returns True if passes."""
    exp_type = spec["type"]
    column = spec["column"]
    val = record.get(column)

    if exp_type == "column_values_between":
        if val is None or not isinstance(val, (int, float)):
            return False
        min_val = spec.get("min_value")
        max_val = spec.get("max_value")
        above_min = min_val is None or val >= min_val
        below_max = max_val is None or val <= max_val
        return above_min and below_max

    elif exp_type == "column_values_not_null":
        return val is not None

    elif exp_type == "column_values_in_set":
        return val in spec.get("values", [])

    elif exp_type == "column_values_of_type":
        type_name = spec.get("type_name", "")
        type_map: dict[str, type] = {
            "int": int,
            "float": (int, float),  # type: ignore[dict-item]
            "str": str,
            "bool": bool,
        }
        expected_type = type_map.get(type_name)
        if expected_type is None:
            return True
        return isinstance(val, expected_type)

    # Unknown expectation type — pass by default
    return True


def check_expectations(
    expectations_specs: list[dict],
    events: list[dict],
) -> list[ExpectationViolation]:
    """Check all expectations against all events. Returns list of violations."""
    violations: list[ExpectationViolation] = []

    for spec in expectations_specs:
        exp_type = spec["type"]
        column = spec["column"]

        for record in events:
            if not evaluate_expectation(spec, record):
                violations.append(ExpectationViolation(
                    expectation_type=exp_type,
                    column=column,
                    message=f"Record violates {exp_type} on column '{column}': value={record.get(column)!r}",
                    record=record,
                ))

    return violations
