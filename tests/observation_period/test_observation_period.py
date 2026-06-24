"""
Comprehensive test suite for the group_dates function in observation_period.py.

Covers:
- Merging periods within the same person that are less than n_days apart
- Merging overlapping periods
- Keeping separate periods that are far apart
- Correct type_concept (mode) assignment after merging
- Persons are kept independent (no cross-person merging)
- Chain merging: consecutive close periods are all merged together
"""

import pandas as pd
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bps_to_omop.observation_period import group_dates

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA = {
    "person_id": pl.Int64,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "type_concept": pl.Int64,
}


def make_df(rows: list[tuple]) -> pl.DataFrame:
    """Create a Polars DataFrame from (person_id, start_date, end_date, type_concept) tuples."""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    pdf = pd.DataFrame.from_records(rows, columns=nombre_columnas)
    pdf["start_date"] = pd.to_datetime(pdf["start_date"])
    pdf["end_date"] = pd.to_datetime(pdf["end_date"])
    df = pl.from_pandas(pdf).with_columns(
        pl.col("start_date").cast(pl.Date),
        pl.col("end_date").cast(pl.Date),
    )
    return df.cast(SCHEMA)


def assert_group_dates_equal(result: pl.DataFrame, expected: pl.DataFrame) -> None:
    """Assert two DataFrames are equal after sorting by person_id and start_date."""
    sort_cols = ["person_id", "start_date"]
    assert_frame_equal(
        result.sort(sort_cols),
        expected.sort(sort_cols),
    )


# ---------------------------------------------------------------------------
# Reference dataset
# ---------------------------------------------------------------------------

N_DAYS = 365

INPUT_ROWS = [
    # -- Person 1: three periods close together (<365d apart), then one far away --
    # type_concept mode should be 2
    (1, "2020-01-01", "2020-02-01", 1),
    (1, "2020-03-01", "2020-04-01", 2),
    (1, "2020-05-01", "2020-12-01", 2),
    # This last one is far away and should NOT be merged
    (1, "2022-01-01", "2022-01-01", 2),
    # -- Person 2: overlapping periods --
    # type_concept mode should be 1
    (2, "2020-01-01", "2020-06-01", 1),
    (2, "2020-03-01", "2020-09-01", 1),
    (2, "2020-06-01", "2020-12-01", 2),
    # -- Person 3: three separate periods, none should be merged --
    (3, "2021-01-01", "2021-01-01", 1),
    (3, "2023-02-01", "2023-02-01", 2),
    (3, "2024-03-01", "2024-04-01", 3),
    # -- Persons 4 and 5: similar dates but different persons, no merging --
    (4, "2024-01-01", "2024-02-01", 1),
    (5, "2025-01-01", "2025-02-01", 2),
    # -- Person 6: chain of periods, each close to the next --
    # All should merge into one; type_concept mode should be 2
    (6, "2020-01-01", "2020-12-01", 1),
    (6, "2021-01-01", "2021-12-01", 2),
    (6, "2022-01-01", "2022-12-01", 2),
    (6, "2023-01-01", "2023-12-01", 2),
]

EXPECTED_ROWS = [
    (1, "2020-01-01", "2020-12-01", 2),
    (1, "2022-01-01", "2022-01-01", 2),
    (2, "2020-01-01", "2020-12-01", 1),
    (3, "2021-01-01", "2021-01-01", 1),
    (3, "2023-02-01", "2023-02-01", 2),
    (3, "2024-03-01", "2024-04-01", 3),
    (4, "2024-01-01", "2024-02-01", 1),
    (5, "2025-01-01", "2025-02-01", 2),
    (6, "2020-01-01", "2023-12-01", 2),
]


# ---------------------------------------------------------------------------
# 1. Full reference dataset integration test
# ---------------------------------------------------------------------------


class TestGroupDatesIntegration:

    def test_full_reference_dataset(self):
        """Full integration test against the reference input/output."""
        df_input = make_df(INPUT_ROWS)
        df_expected = make_df(EXPECTED_ROWS)
        result = group_dates(df_input, n_days=N_DAYS)
        assert_group_dates_equal(result, df_expected)

    def test_output_row_count(self):
        """Result should have exactly as many rows as the expected output."""
        df_input = make_df(INPUT_ROWS)
        result = group_dates(df_input, n_days=N_DAYS)
        assert len(result) == len(EXPECTED_ROWS)

    def test_output_columns(self):
        """Output columns should match input columns."""
        df_input = make_df(INPUT_ROWS)
        result = group_dates(df_input, n_days=N_DAYS)
        assert result.columns[:4] == df_input.columns[:4]


# ---------------------------------------------------------------------------
# 2. Chain merging
# ---------------------------------------------------------------------------


class TestChainMerging:

    def test_chain_does_not_merge_if_gap_too_large(self):
        """If any gap in the chain exceeds n_days, the chain should break there."""
        rows = [
            (6, "2020-01-01", "2020-12-01", 1),
            (6, "2021-01-01", "2021-12-01", 2),
            # gap > 365 days here
            (6, "2023-06-01", "2023-12-01", 2),
            (6, "2024-01-01", "2024-12-01", 2),
        ]
        result = group_dates(make_df(rows), n_days=N_DAYS)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# 3. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:

    def test_single_row(self):
        rows = [(1, "2020-01-01", "2020-01-01", 1)]
        result = group_dates(make_df(rows), n_days=N_DAYS)
        assert len(result) == 1
        assert str(result["start_date"][0]) == "2020-01-01"
        assert str(result["end_date"][0]) == "2020-01-01"
        assert result["type_concept"][0] == 1

    def test_empty_dataframe(self):
        result = group_dates(make_df([]), n_days=N_DAYS)
        assert len(result) == 0

    def test_gap_above_threshold_stays_separate(self):
        """A gap strictly above n_days (366d in a leap year) should start a new period."""
        rows = [
            (1, "2020-01-01", "2020-01-01", 1),
            (
                1,
                "2021-01-01",
                "2021-01-01",
                1,
            ),  # 366 days later (2020 is leap) → new period
        ]
        result = group_dates(make_df(rows), n_days=N_DAYS)
        assert len(result) == 2

    def test_output_sorted_by_person_and_start(self):
        """Output should be sorted by person_id ascending, then start_date ascending."""
        rows = [
            (2, "2020-01-01", "2020-02-01", 1),
            (1, "2021-01-01", "2021-02-01", 1),
            # Far enough from the row above to stay separate (>365d gap)
            (1, "2019-01-01", "2019-02-01", 2),
        ]
        result = group_dates(make_df(rows), n_days=N_DAYS)
        # person 1 has 2 separate periods, person 2 has 1 → 3 rows total
        assert len(result) == 3
        person_ids = result["person_id"].to_list()
        starts = result["start_date"].to_list()
        assert person_ids == sorted(person_ids)
        for pid in set(person_ids):
            person_starts = [starts[i] for i, p in enumerate(person_ids) if p == pid]
            assert person_starts == sorted(person_starts)
