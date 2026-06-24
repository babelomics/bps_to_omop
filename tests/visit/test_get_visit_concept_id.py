"""Tests for get_visit_concept_id."""

from datetime import date
from typing import Any

import polars as pl
import pytest

from bps_to_omop.visit import get_visit_concept_id

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def base_table() -> pl.DataFrame:
    """Minimal DataFrame with required columns."""
    return pl.DataFrame(
        {
            "person_id": [1, 2, 3, 4],
            "start_date": [
                date(2023, 1, 1),
                date(2023, 1, 1),
                date(2023, 1, 1),
                date(2023, 1, 1),
            ],
            "end_date": [
                date(2023, 1, 1),
                date(2023, 1, 3),
                date(2023, 1, 8),
                date(2023, 1, 15),
            ],
            "category": ["A", "B", "A", "C"],
        }
    )


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


def test_returns_series(base_table):
    result = get_visit_concept_id(base_table, [("single_code", 99, {})])
    assert isinstance(result, pl.Series)


def test_result_length_matches_input(base_table):
    result = get_visit_concept_id(base_table, [("single_code", 99, {})])
    assert len(result) == len(base_table)


def test_result_dtype_is_integer(base_table):
    result = get_visit_concept_id(base_table, [("single_code", 99, {})])
    assert result.dtype in (pl.Int32, pl.Int64)


# ---------------------------------------------------------------------------
# single_code
# ---------------------------------------------------------------------------


def test_single_code_assigns_code_to_all_rows(base_table):
    result = get_visit_concept_id(base_table, [("single_code", 42, {})])
    assert (result == 42).all()


def test_single_code_does_not_overwrite_existing_nonzero():
    """If a prior function already set a code, single_code should not overwrite it."""
    table = pl.DataFrame(
        {
            "start_date": [date(2023, 1, 1)],
            "end_date": [date(2023, 1, 1)],
        }
    )
    # First pass sets 10, second pass tries to set 99 via single_code
    result = get_visit_concept_id(
        table,
        [
            ("single_code", 10, {}),
            ("single_code", 99, {}),
        ],
    )
    assert result[0] == 10  # 99 should not overwrite an already-assigned 10


# ---------------------------------------------------------------------------
# duration_code
# ---------------------------------------------------------------------------


def test_duration_code_within_range(base_table):
    # rows 1 and 2 have durations 2 and 7 days — both within [1, 7]
    result = get_visit_concept_id(
        base_table, [("duration_code", 55, {"time_lims": [1, 7]})]
    )
    assert result[1] == 55
    assert result[2] == 55


def test_duration_code_outside_range_stays_zero(base_table):
    # row 0 has duration 0, row 3 has duration 14 — both outside [1, 7]
    result = get_visit_concept_id(
        base_table, [("duration_code", 55, {"time_lims": [1, 7]})]
    )
    assert result[0] == 0
    assert result[3] == 0


def test_duration_code_boundary_inclusive(base_table):
    # row 1 = 2 days, row 2 = 7 days → both on or inside [2, 7]
    result = get_visit_concept_id(
        base_table, [("duration_code", 77, {"time_lims": [2, 7]})]
    )
    assert result[1] == 77
    assert result[2] == 77


# ---------------------------------------------------------------------------
# field_code
# ---------------------------------------------------------------------------


def test_field_code_matches_string_value(base_table):
    result = get_visit_concept_id(
        base_table, [("field_code", 11, {"colname": "category", "colvalue": "A"})]
    )
    assert result[0] == 11  # category == "A"
    assert result[2] == 11  # category == "A"


def test_field_code_non_matching_rows_stay_zero(base_table):
    result = get_visit_concept_id(
        base_table, [("field_code", 11, {"colname": "category", "colvalue": "A"})]
    )
    assert result[1] == 0  # category == "B"
    assert result[3] == 0  # category == "C"


# ---------------------------------------------------------------------------
# Composability — multiple functions applied in order
# ---------------------------------------------------------------------------


def test_multiple_functions_applied_in_order(base_table):
    """duration_code sets rows 1+2, field_code sets row 3; single_code fills remaining 0s."""
    result = get_visit_concept_id(
        base_table,
        [
            ("duration_code", 10, {"time_lims": [1, 7]}),
            ("field_code", 20, {"colname": "category", "colvalue": "C"}),
            ("single_code", 99, {}),
        ],
    )
    assert result[0] == 99  # was 0 after first two → single_code fills it
    assert result[1] == 10  # duration match
    assert result[2] == 10  # duration match
    assert result[3] == 20  # field match


def test_later_function_does_not_overwrite_earlier(base_table):
    """A subsequent single_code should not overwrite codes already set."""
    result = get_visit_concept_id(
        base_table,
        [
            ("field_code", 77, {"colname": "category", "colvalue": "A"}),
            ("single_code", 99, {}),
        ],
    )
    assert result[0] == 77  # set by field_code, not overwritten
    assert result[2] == 77  # set by field_code, not overwritten


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_functions_list_returns_all_zeros(base_table):
    result = get_visit_concept_id(base_table, [])
    assert (result == 0).all()


def test_single_row_table():
    table = pl.DataFrame(
        {
            "start_date": [date(2023, 1, 1)],
            "end_date": [date(2023, 1, 5)],
            "category": ["X"],
        }
    )
    result = get_visit_concept_id(table, [("single_code", 42, {})])
    assert len(result) == 1
    assert result[0] == 42


def test_unknown_function_raises():
    table = pl.DataFrame(
        {"start_date": [date(2023, 1, 1)], "end_date": [date(2023, 1, 1)]}
    )
    with pytest.raises(KeyError):
        get_visit_concept_id(table, [("nonexistent_func", 1, {})])
