import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.append("../bps_to_omop/")
from bps_to_omop.general import find_overlap_index


# == TETS ==============================================================================
def test_no_overlap():
    """Test when there are no overlaps"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "end_date": ["2024-01-31", "2024-02-28", "2024-03-31"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, False, False])
    pd.testing.assert_series_equal(result, expected)


def test_simple_overlap():
    """Test when there are no overlaps"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-05", "2024-03-01"],
            "end_date": ["2024-01-31", "2024-01-05", "2024-03-31"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, True, False])
    pd.testing.assert_series_equal(result, expected)


def test_exact_dates_single_day():
    """Test handling of single-day visits"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-01", "2024-03-01"],
            "end_date": ["2024-01-01", "2024-01-01", "2024-03-31"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = find_overlap_index(df)

    # Single day visits on same day should not be marked as overlapping
    expected = pd.Series([False, False, False])
    pd.testing.assert_series_equal(result, expected)


def test_exact_dates_multiday():
    """Test behavior when visits have exactly same dates"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1],
            "start_date": ["2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-31", "2024-01-31"],
            "visit_type": ["A", "B"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, True])
    pd.testing.assert_series_equal(result, expected)


def test_different_patients():
    """Test that overlaps are only detected within same person_id"""
    df = pd.DataFrame(
        {
            "person_id": [1, 2],
            "start_date": ["2024-01-01", "2024-01-15"],
            "end_date": ["2024-01-30", "2024-01-20"],
            "visit_type": ["A", "B"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, False])
    pd.testing.assert_series_equal(result, expected)


# Edge cases
def test_empty_dataframe():
    """Test behavior with empty DataFrame"""
    df = pd.DataFrame(
        {"person_id": [], "start_date": [], "end_date": [], "visit_type": []}
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    assert len(result) == 0


def test_single_row():
    """Test behavior with single row"""
    df = pd.DataFrame(
        {
            "person_id": [1],
            "start_date": ["2024-01-01"],
            "end_date": ["2024-01-31"],
            "visit_type": ["A"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False])
    pd.testing.assert_series_equal(result, expected)


# Complex scenarios
def test_multiple_overlaps():
    """
    Test complex scenario with multiple overlapping visits
    Here we should remove the second row on first iteration. The other
    would be removed in subsequent iterations!
    """
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 1, 1],
            "start_date": ["2024-01-01", "2024-01-05", "2024-01-15", "2024-01-25"],
            "end_date": ["2024-01-31", "2024-01-20", "2024-01-25", "2024-01-28"],
            "visit_type": ["A", "B", "C", "D"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, True, False, False])
    pd.testing.assert_series_equal(result, expected)


def test_mixed_single_and_multiple_day_visits():
    """Test mix of single-day and multiple-day visits"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "end_date": ["2024-01-01", "2024-01-05", "2024-01-03"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    # First row is single-day, second is multiple-day, third is contained
    expected = pd.Series([False, False, True])
    pd.testing.assert_series_equal(result, expected)


# Error cases
def test_invalid_date_order():
    """Test behavior when end_date is before start_date"""
    df = pd.DataFrame(
        {
            "person_id": [1, 1],
            "start_date": ["2024-01-31", "2024-01-15"],
            "end_date": ["2024-01-01", "2024-01-20"],
            "visit_type": ["A", "B"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = find_overlap_index(df)
    expected = pd.Series([False, False])
    pd.testing.assert_series_equal(result, expected)
