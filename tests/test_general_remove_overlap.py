import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.append("../bps_to_omop/")
from bps_to_omop.general import remove_overlap


# == TESTS =============================================================================
def test_simple_overlap():
    """Test when there is basic overlap"""
    df_in = pd.DataFrame(
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
    df_out = pd.DataFrame(
        {
            "person_id": [1, 2],
            "start_date": ["2024-01-01", "2024-03-01"],
            "end_date": ["2024-01-31", "2024-03-31"],
            "visit_type": ["A", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = remove_overlap(df_in).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_exact_dates_single_day():
    """Test handling of overlapping single-day visits"""
    df_in = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01", "2024-01-31"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    df_out = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01", "2024-01-31"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = remove_overlap(df_in).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, df_out)


def test_exact_dates_multiday():
    """Test behavior when visits have exactly same dates"""
    df_in = pd.DataFrame(
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
    df_out = pd.DataFrame(
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
    result = remove_overlap(df_in).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, df_out)


def test_different_patients():
    """Test that overlaps are only detected within same person_id"""
    df_in = pd.DataFrame(
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
    df_out = pd.DataFrame(
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

    result = remove_overlap(df_in).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


# Edge cases
def test_empty_dataframe():
    """Test behavior with empty DataFrame"""
    df = pd.DataFrame(
        {"person_id": [], "start_date": [], "end_date": [], "visit_type": []}
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = remove_overlap(df)
    assert len(result) == 0


def test_single_row():
    """Test behavior with single row. Should leave it as is."""
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

    result = remove_overlap(df)
    pd.testing.assert_frame_equal(result, df)


# Complex scenarios
def test_multiple_overlaps():
    """
    Test complex scenario with multiple overlapping visits
    Here we should remove the second row on first iteration. The other
    would be removed in subsequent iterations!
    """
    df_in = pd.DataFrame(
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
    df_out = pd.DataFrame(
        {
            "person_id": [
                1,
            ],
            "start_date": ["2024-01-01"],
            "end_date": ["2024-01-31"],
            "visit_type": ["A"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = remove_overlap(df_in).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_mixed_single_and_multiple_day_visits():
    """Test mix of single-day and multiple-day visits"""
    df_in = pd.DataFrame(
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
    df_out = pd.DataFrame(
        {
            "person_id": [1],
            "start_date": ["2024-01-01"],
            "end_date": ["2024-01-05"],
            "visit_type": ["B"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = remove_overlap(df_in).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_provider_id_singleday():
    """
    When we have single day visits with diffeent providers, we should keep all.
    """
    df_in = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "provider_id": [0, 1, 2],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    df_out = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "provider_id": [0, 1, 2],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = remove_overlap(
        df_in, 5, ascending_order=[True, True, False, True, True]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_provider_id_singleday_and_multiday():
    """
    When we have single day visits with diffeent providers, we should keep all.
    """
    df_in = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-05", "2024-01-01", "2024-01-06"],
            "provider_id": [0, 1, 2],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    df_out = pd.DataFrame(
        {
            "person_id": [1],
            "start_date": ["2024-01-01"],
            "end_date": ["2024-01-06"],
            "provider_id": [2],
            "visit_type": ["C"],
        }
    ).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    result = remove_overlap(
        df_in, 5, ascending_order=[True, True, False, True, True]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)
