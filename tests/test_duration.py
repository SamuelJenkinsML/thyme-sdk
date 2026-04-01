"""Tests for the duration parsing utility."""

import pytest

from thyme.duration import parse_duration


class TestParseDuration:
    """Given valid duration strings, parse_duration returns seconds."""

    def test_seconds(self):
        assert parse_duration("30s") == 30

    def test_minutes(self):
        assert parse_duration("5m") == 300

    def test_hours(self):
        assert parse_duration("1h") == 3600

    def test_days(self):
        assert parse_duration("30d") == 30 * 86400

    def test_empty_returns_zero(self):
        assert parse_duration("") == 0

    def test_zero_value(self):
        assert parse_duration("0d") == 0

    def test_whitespace_stripped(self):
        assert parse_duration("  1h  ") == 3600


class TestParseDurationErrors:
    """Given invalid duration strings, parse_duration raises ValueError."""

    def test_no_numeric_prefix(self):
        with pytest.raises(ValueError):
            parse_duration("banana")

    def test_missing_unit(self):
        with pytest.raises(ValueError):
            parse_duration("30")

    def test_unknown_unit(self):
        with pytest.raises(ValueError):
            parse_duration("5x")


class TestBackwardCompatAlias:
    """parse_window_duration alias still works."""

    def test_alias_exists(self):
        from thyme.duration import parse_window_duration

        assert parse_window_duration("1h") == 3600
