import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from bps_to_omop.general import find_visit_occurence_id

sys.path.append("../bps_to_omop/")


# == TESTS =============================================================================
def test_simple_visit():
    """Test when there is basic overlap"""
    # -- Prepare input
    events = pd.DataFrame(
        {
            "event_id": [0, 1, 2],
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-05", "2024-03-01"],
            "end_date": ["2024-01-01", "2024-01-05", "2024-03-01"],
            "visit_type": ["A", "B", "C"],
        }
    ).assign(
        start_date=lambda x: x["start_date"].astype("datetime64[ms]"),
        end_date=lambda x: x["end_date"].astype("datetime64[ms]"),
    )
    event_columns = ["person_id", "start_date", "event_id"]
    visits = pd.DataFrame(
        {
            "visit_occurrence_id": [0, 1],
            "person_id": [1, 2],
            "visit_start_date": ["2024-01-01", "2024-03-01"],
            "visit_end_date": ["2024-01-01", "2024-03-01"],
        }
    ).assign(
        visit_start_date=lambda x: x["visit_start_date"].astype("datetime64[ms]"),
        visit_end_date=lambda x: x["visit_end_date"].astype("datetime64[ms]"),
    )
    # -- Prepare output
    out = pd.DataFrame(
        {
            "event_id": [0, 1, 2],
            "person_id": [1, 1, 2],
            "start_date": ["2024-01-01", "2024-01-05", "2024-03-01"],
            "end_date": ["2024-01-01", "2024-01-05", "2024-03-01"],
            "visit_type": ["A", "B", "C"],
            "visit_occurrence_id": [0, np.nan, 1],
            "visit_start_date": ["2024-01-01", pd.NaT, "2024-03-01"],
            "visit_end_date": ["2024-01-01", pd.NaT, "2024-03-01"],
        }
    ).assign(
        start_date=lambda x: x["start_date"].astype("datetime64[ms]"),
        end_date=lambda x: x["end_date"].astype("datetime64[ms]"),
        visit_start_date=lambda x: x["visit_start_date"].astype("datetime64[ms]"),
        visit_end_date=lambda x: x["visit_end_date"].astype("datetime64[ms]"),
    )

    result = find_visit_occurence_id(events, event_columns, visits)
    pd.testing.assert_frame_equal(result, out)


def test_same_date_different_person():
    """Test when there is basic overlap"""
    # -- Prepare input
    events = pd.DataFrame(
        {
            "event_id": [0, 1],
            "person_id": [1, 2],
            "start_date": ["2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01"],
            "visit_type": ["A", "C"],
        }
    ).assign(
        start_date=lambda x: x["start_date"].astype("datetime64[ms]"),
        end_date=lambda x: x["end_date"].astype("datetime64[ms]"),
    )
    event_columns = ["person_id", "start_date", "event_id"]
    visits = pd.DataFrame(
        {
            "visit_occurrence_id": [0, 1],
            "person_id": [1, 3],
            "visit_start_date": ["2024-01-01", "2024-01-01"],
            "visit_end_date": ["2024-01-01", "2024-01-01"],
        }
    ).assign(
        visit_start_date=lambda x: x["visit_start_date"].astype("datetime64[ms]"),
        visit_end_date=lambda x: x["visit_end_date"].astype("datetime64[ms]"),
    )
    # -- Prepare output
    out = pd.DataFrame(
        {
            "event_id": [0, 1],
            "person_id": [1, 2],
            "start_date": ["2024-01-01", "2024-01-01"],
            "end_date": ["2024-01-01", "2024-01-01"],
            "visit_type": ["A", "C"],
            "visit_occurrence_id": [0, np.nan],
            "visit_start_date": ["2024-01-01", pd.NaT],
            "visit_end_date": ["2024-01-01", pd.NaT],
        }
    ).assign(
        start_date=lambda x: x["start_date"].astype("datetime64[ms]"),
        end_date=lambda x: x["end_date"].astype("datetime64[ms]"),
        visit_start_date=lambda x: x["visit_start_date"].astype("datetime64[ms]"),
        visit_end_date=lambda x: x["visit_end_date"].astype("datetime64[ms]"),
    )

    result = find_visit_occurence_id(events, event_columns, visits)
    pd.testing.assert_frame_equal(result, out)


def test_no_valid_visits():
    """Test when there is basic overlap"""
    # -- Prepare input
    events = pd.DataFrame(
        {
            "event_id": [0, 1],
            "person_id": [1, 2],
            "start_date": ["2024-01-01", "2024-02-01"],
            "end_date": ["2024-01-01", "2024-02-01"],
            "visit_type": ["A", "C"],
        }
    ).assign(
        start_date=lambda x: x["start_date"].astype("datetime64[ms]"),
        end_date=lambda x: x["end_date"].astype("datetime64[ms]"),
    )
    event_columns = ["person_id", "start_date", "event_id"]
    visits = pd.DataFrame(
        {
            "visit_occurrence_id": [0, 1],
            "person_id": [1, 1],
            "visit_start_date": ["2024-02-01", "2024-02-01"],
            "visit_end_date": ["2024-02-01", "2024-02-01"],
        }
    ).assign(
        visit_start_date=lambda x: x["visit_start_date"].astype("datetime64[ms]"),
        visit_end_date=lambda x: x["visit_end_date"].astype("datetime64[ms]"),
    )

    with pytest.raises(ValueError):
        find_visit_occurence_id(events, event_columns, visits)
