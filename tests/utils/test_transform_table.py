"""Tests for apply_transformation, melt_start_end, remove_end_date."""

from datetime import date

import polars as pl
import pytest

from bps_to_omop.utils.transform_table import (
    apply_transformation,
    melt_start_end,
    remove_end_date,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def base_table() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "start_date": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
            "end_date": [date(2023, 1, 5), date(2023, 2, 5), date(2023, 3, 5)],
            "type_concept": [10, 10, 10],
        }
    )


@pytest.fixture
def melt_table() -> pl.DataFrame:
    """Table with distinct start/end dates and a null to exercise drop_nulls."""
    return pl.DataFrame(
        {
            "person_id": [1, 2],
            "start_date": [date(2023, 1, 1), date(2023, 3, 1)],
            "end_date": [date(2023, 1, 5), None],
            "type_concept": [10, 10],
        }
    )


# ---------------------------------------------------------------------------
# apply_transformation
# ---------------------------------------------------------------------------


def test_no_transformations(base_table):
    """No transformations key in params — table returned unchanged."""
    result = apply_transformation(base_table, {}, "test_key")
    assert result.equals(base_table)


def test_no_file_transformations(base_table):
    """Transformations key present but empty — table returned unchanged."""
    result = apply_transformation(base_table, {"transformations": {}}, "test_key")
    assert result.equals(base_table)


def test_missing_key_transformations(base_table):
    """Transformations exist for other keys but not the requested one."""
    params = {"transformations": {"other_key": ["remove_end_date"]}}
    result = apply_transformation(base_table, params, "missing_key")
    assert result.equals(base_table)


def test_single_transformation(base_table):
    """remove_end_date applied via the registry produces correct output."""
    params = {"transformations": {"test_key": ["remove_end_date"]}}
    result = apply_transformation(base_table, params, "test_key")

    expected = base_table.with_columns(pl.col("start_date").alias("end_date"))
    assert result.equals(expected)


def test_multiple_transformations(base_table):
    """melt_start_end followed by remove_end_date produces correct output."""
    params = {"transformations": {"test_key": ["melt_start_end", "remove_end_date"]}}
    result = apply_transformation(base_table, params, "test_key")

    # After melt_start_end each row becomes two events (start and end);
    # after remove_end_date, end_date == start_date for all rows.
    assert (result["start_date"] == result["end_date"]).all()
    assert len(result) == 6  # 3 rows × 2 dates
    assert result.columns == ["person_id", "start_date", "end_date", "type_concept"]


# ---------------------------------------------------------------------------
# remove_end_date
# ---------------------------------------------------------------------------


def test_remove_end_date_replaces_with_start(base_table):
    result = remove_end_date(base_table)
    assert (result["end_date"] == result["start_date"]).all()


def test_remove_end_date_preserves_columns(base_table):
    result = remove_end_date(base_table)
    assert result.columns == base_table.columns


def test_remove_end_date_preserves_row_count(base_table):
    result = remove_end_date(base_table)
    assert len(result) == len(base_table)


# ---------------------------------------------------------------------------
# melt_start_end
# ---------------------------------------------------------------------------


def test_melt_start_end_output_columns(melt_table):
    result = melt_start_end(melt_table)
    assert result.columns == ["person_id", "start_date", "end_date", "type_concept"]


def test_melt_start_end_start_equals_end(melt_table):
    result = melt_start_end(melt_table)
    assert (result["start_date"] == result["end_date"]).all()


def test_melt_start_end_drops_nulls(melt_table):
    result = melt_start_end(melt_table)
    assert result["start_date"].null_count() == 0


def test_melt_start_end_type_concept_propagated(melt_table):
    result = melt_start_end(melt_table)
    assert (result["type_concept"] == 10).all()


def test_melt_start_end_deduplicates():
    """Identical start and end dates collapse to one row after unpivot."""
    table = pl.DataFrame(
        {
            "person_id": [1],
            "start_date": [date(2023, 1, 1)],
            "end_date": [date(2023, 1, 1)],
            "type_concept": [10],
        }
    )
    result = melt_start_end(table)
    assert len(result) == 1


def test_melt_start_end_row_count(melt_table):
    """2 rows × 2 dates = 4 minus 1 null = 3 distinct events."""
    result = melt_start_end(melt_table)
    assert len(result) == 3
