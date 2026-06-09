"""
Comprehensive test suite for the VISIT_OCCURRENCE and VISIT_DETAIL building functions.

Covers:
- build_visit_detail: sorting, column renaming, ID assignment
- build_visit_detail_extended: initial state setup, join correctness
- identify/update contained rows
- identify/update partial rows
- identify/update not-contained rows
- identify_next_main_visits
- assign_visit_occurrence_id
- build_visit_occurrence (integration): all scenarios from the reference test set
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from bps_to_omop.visit import (
    assign_visit_occurrence_id,
    build_visit_detail,
    build_visit_detail_extended,
    build_visit_occurrence,
    finalize_visit_tables,
    identify_contained_rows,
    identify_next_main_visits,
    identify_not_contained_rows,
    identify_partial_rows,
    update_contained_rows,
    update_not_contained_rows,
    update_partial_rows,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA = {
    "person_id": pl.Int64,
    "start_date": pl.Datetime("us"),
    "end_date": pl.Datetime("us"),
    "type_concept": pl.Int64,
    "visit_concept_id": pl.Int64,
    "provider_id": pl.Int64,
}


def make_df(rows: list[tuple]) -> pl.DataFrame:
    """Create a DataFrame from (person_id, start, end, type_concept) tuples,
    filling visit_concept_id=9202 and provider_id=0."""
    records = []
    for row in rows:
        person_id, start, end, type_concept = row
        records.append((person_id, start, end, type_concept, 9202, 0))
    pdf = pd.DataFrame.from_records(
        records,
        columns=[
            "person_id",
            "start_date",
            "end_date",
            "type_concept",
            "visit_concept_id",
            "provider_id",
        ],
    )
    df = pl.from_pandas(pdf).with_columns(
        pl.col("start_date").str.to_datetime(),
        pl.col("end_date").str.to_datetime(),
    )
    return df.cast(SCHEMA)


def run_pipeline(rows: list[tuple]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the full pipeline on a list of (person_id, start, end, type) rows."""
    df = make_df(rows)
    df = build_visit_occurrence(df)
    visit_detail, visit_occurrence = finalize_visit_tables(df)
    return visit_detail, visit_occurrence


# ---------------------------------------------------------------------------
# Reference dataset (from the original test specification)
# ---------------------------------------------------------------------------

REFERENCE_ROWS = [
    # -- Main visit 1 --
    (1, "2020-01-01", "2020-02-01", 1),  # main
    (1, "2020-01-02", "2020-01-02", 1),  # contained, same type
    (1, "2020-01-04", "2020-01-04", 2),  # contained, diff type
    (1, "2020-01-06", "2020-02-06", 1),  # partial, same type
    (1, "2020-01-08", "2020-02-08", 2),  # partial, diff type  ← sets end to 2020-02-08
    # -- Main visit 2 --
    (1, "2020-03-01", "2020-04-01", 1),
    (1, "2020-03-02", "2020-03-02", 1),
    (1, "2020-03-04", "2020-03-04", 2),
    (1, "2020-03-06", "2020-04-06", 1),
    (1, "2020-03-08", "2020-04-08", 2),  # ← sets end to 2020-04-08
    # -- Main visit 3: latest partial is not the first one --
    (1, "2020-06-01", "2020-07-01", 1),
    (1, "2020-06-10", "2020-07-10", 1),
    (1, "2020-06-20", "2020-07-20", 1),  # ← sets end to 2020-07-20
    # -- Main visit 4: third visit contained in union of first two --
    (1, "2020-08-01", "2020-09-01", 1),
    (1, "2020-08-10", "2020-09-10", 1),  # ← sets end to 2020-09-10 initially
    (1, "2020-09-02", "2020-09-20", 1),  # contained in union → end becomes 2020-09-20
    # -- Person 2: two separate visits --
    (2, "2021-01-01", "2021-01-01", 1),
    (2, "2021-02-01", "2021-02-01", 1),
    # -- Person 3: shares a date with person 2 --
    (3, "2021-02-01", "2021-02-01", 2),
    (3, "2021-03-01", "2021-03-01", 2),
    # -- Person 4: duplicate row, only one main visit --
    (4, "2022-03-01", "2022-04-01", 1),
    (4, "2022-03-01", "2022-04-01", 2),
    # -- Person 4: 2+ consecutives visits that share start-end
    # We are considering this independent visits
    (5, "2020-01-01 01:00", "2020-02-01 01:00", 1),
    (5, "2020-02-01 01:00", "2020-03-01 01:00", 2),
    (5, "2020-03-01 01:00", "2020-04-01 01:00", 2),
]


# ---------------------------------------------------------------------------
# 1. build_visit_detail
# ---------------------------------------------------------------------------


class TestBuildVisitDetail:

    def test_output_columns(self):
        df = make_df([(1, "2020-01-01", "2020-01-02", 1)])
        out = build_visit_detail(df)
        assert "visit_detail_start_datetime" in out.columns
        assert "visit_detail_end_datetime" in out.columns
        assert "visit_detail_type_concept_id" in out.columns
        assert "visit_detail_id" in out.columns
        # Original column names should be gone
        assert "start_date" not in out.columns
        assert "end_date" not in out.columns
        assert "type_concept" not in out.columns

    def test_sorted_by_person(self):
        df = make_df(
            [
                (1, "2020-03-01", "2020-03-02", 1),
                (1, "2020-01-01", "2020-01-02", 1),
                (2, "2019-01-01", "2019-01-02", 1),
            ]
        )
        out = build_visit_detail(df)
        person_ids = out["person_id"].to_list()
        starts = out["visit_detail_start_datetime"].to_list()
        assert person_ids == sorted(person_ids), "Should be sorted by person_id"

    def test_end_date_descending_within_same_start(self):
        """Within same person+start, end_date should be descending."""
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-03", 1),
                (1, "2020-01-01", "2020-01-10", 1),
                (1, "2020-01-01", "2020-01-05", 1),
            ]
        )
        out = build_visit_detail(df)
        ends = out["visit_detail_end_datetime"].to_list()
        assert ends == sorted(ends, reverse=True)

    def test_type_concept_ascending_within_same_start_end(self):
        """Within same person+start+end, type_concept should be ascending."""
        df = make_df(
            [
                (4, "2022-03-01", "2022-04-01", 2),
                (4, "2022-03-01", "2022-04-01", 1),
            ]
        )
        out = build_visit_detail(df)
        types = out["visit_detail_type_concept_id"].to_list()
        assert types == sorted(types)

    def test_visit_detail_id_is_sequential(self):
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-02-01", "2020-02-02", 1),
                (2, "2020-01-01", "2020-01-02", 1),
            ]
        )
        out = build_visit_detail(df)
        ids = out["visit_detail_id"].to_list()
        assert ids == list(range(len(ids)))

    def test_single_row(self):
        df = make_df([(1, "2020-01-01", "2020-01-01", 1)])
        out = build_visit_detail(df)
        assert len(out) == 1
        assert out["visit_detail_id"][0] == 0

    def test_empty_dataframe(self):
        df = make_df([])
        out = build_visit_detail(df)
        assert len(out) == 0
        assert "visit_detail_id" in out.columns


# ---------------------------------------------------------------------------
# 2. build_visit_detail_extended
# ---------------------------------------------------------------------------


class TestBuildVisitDetailExtended:

    def _get_extended(self, rows):
        df = make_df(rows)
        detail = build_visit_detail(df)
        return build_visit_detail_extended(detail)

    def test_output_columns_present(self):
        ext = self._get_extended([(1, "2020-01-01", "2020-01-02", 1)])
        for col in [
            "visit_start_datetime",
            "visit_end_datetime",
            "visit_detail_id_original",
            "main_visit",
            "is_contained",
            "is_partial",
            "not_contained",
            "parent_visit_detail_id",
        ]:
            assert col in ext.columns, f"Missing column: {col}"

    def test_all_main_visit_unknown(self):
        ext = self._get_extended(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
            ]
        )
        assert (ext["main_visit"] == "Unknown").all()

    def test_flags_all_false(self):
        ext = self._get_extended([(1, "2020-01-01", "2020-01-02", 1)])
        assert ext["is_contained"].to_list() == [False]
        assert ext["is_partial"].to_list() == [False]
        assert ext["not_contained"].to_list() == [False]

    def test_visit_detail_id_original_points_to_first_row_per_person(self):
        """The original visit_detail_id should be the first (earliest) row per person."""
        ext = self._get_extended(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
                (2, "2020-06-01", "2020-06-02", 1),
            ]
        )
        p1_rows = ext.filter(pl.col("person_id") == 1)
        assert (p1_rows["visit_detail_id_original"] == 0).all()
        p2_rows = ext.filter(pl.col("person_id") == 2)
        assert (p2_rows["visit_detail_id_original"] == 2).all()

    def test_start_datetime_is_earliest_per_person(self):
        ext = self._get_extended(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
            ]
        )
        p1_rows = ext.filter(pl.col("person_id") == 1)
        expected = pl.Series(["2020-01-01"]).str.to_datetime()
        assert (p1_rows["visit_start_datetime"] == expected[0]).all()

    def test_multiple_persons_independent(self):
        ext = self._get_extended(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (2, "2021-05-01", "2021-05-02", 1),
            ]
        )
        p1_start = ext.filter(pl.col("person_id") == 1)["visit_start_datetime"][0]
        p2_start = ext.filter(pl.col("person_id") == 2)["visit_start_datetime"][0]
        assert str(p1_start)[:10] == "2020-01-01"
        assert str(p2_start)[:10] == "2021-05-01"


# ---------------------------------------------------------------------------
# 3. identify_contained_rows / update_contained_rows
# ---------------------------------------------------------------------------


class TestContainedRows:

    def _setup(self, rows):
        df = make_df(rows)
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        return df

    def test_contained_visit_flagged(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),  # main
                (1, "2020-01-05", "2020-01-10", 1),  # contained
            ]
        )
        df = identify_contained_rows(df)
        flags = df["is_contained"].to_list()
        assert flags[0] is False  # main visit not self-contained
        assert flags[1] is True

    def test_not_contained_when_outside(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-01-10", 1),
                (1, "2020-02-01", "2020-02-10", 1),  # entirely outside
            ]
        )
        df = identify_contained_rows(df)
        assert df["is_contained"].to_list() == [False, False]

    def test_update_sets_main_visit_no(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-05", "2020-01-10", 1),
            ]
        )
        df = identify_contained_rows(df)
        df = update_contained_rows(df)
        visits = df["main_visit"].to_list()
        assert visits[0] == "Yes"  # main stays
        assert visits[1] == "No"  # contained becomes No

    def test_parent_visit_detail_id_assigned(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-05", "2020-01-10", 1),
            ]
        )
        df = identify_contained_rows(df)
        df = update_contained_rows(df)
        parent = df["parent_visit_detail_id"].to_list()
        assert parent[0] is None  # main has no parent
        assert parent[1] is not None

    def test_exactly_coincident_dates_is_contained(self):
        """A visit with same start and end as the main visit is contained."""
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-01", "2020-02-01", 2),  # same dates, different type
            ]
        )
        df = identify_contained_rows(df)
        # Both endpoints match → contained
        assert df["is_contained"].to_list()[1] is True


# ---------------------------------------------------------------------------
# 4. identify_partial_rows / update_partial_rows
# ---------------------------------------------------------------------------


class TestPartialRows:

    def _setup(self, rows):
        df = make_df(rows)
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        return df

    def test_partial_visit_flagged(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-15", "2020-02-15", 1),  # starts inside, ends after
            ]
        )
        df = identify_partial_rows(df)
        assert df["is_partial"].to_list()[1] is True

    def test_fully_before_not_partial(self):
        df = self._setup(
            [
                (1, "2020-03-01", "2020-04-01", 1),
                (1, "2020-01-01", "2020-01-10", 1),  # entirely before
            ]
        )
        # After sort the order will be reversed
        df = identify_partial_rows(df)
        assert not any(df["is_partial"].to_list())

    def test_update_partial_extends_end_datetime(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-15", "2020-03-01", 1),
            ]
        )
        df = identify_partial_rows(df)
        df = update_partial_rows(df)
        main_row = df.filter(pl.col("main_visit") == "Yes")
        assert str(main_row["visit_end_datetime"][0])[:10] == "2020-03-01"

    def test_update_partial_picks_latest_end(self):
        """When multiple partial visits exist, end should extend to the furthest."""
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-15", "2020-03-01", 1),
                (1, "2020-01-20", "2020-04-01", 1),  # latest
            ]
        )
        df = identify_partial_rows(df)
        df = update_partial_rows(df)
        main_row = df.filter(pl.col("main_visit") == "Yes")
        assert str(main_row["visit_end_datetime"][0])[:10] == "2020-04-01"

    def test_partial_rows_set_to_no(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-15", "2020-03-01", 1),
            ]
        )
        df = identify_partial_rows(df)
        df = update_partial_rows(df)
        partial_row = df.filter(pl.col("is_partial"))
        assert (partial_row["main_visit"] == "No").all()


# ---------------------------------------------------------------------------
# 5. identify_not_contained_rows / update_not_contained_rows
# ---------------------------------------------------------------------------


class TestNotContainedRows:

    def _setup(self, rows):
        df = make_df(rows)
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        return df

    def test_visit_starting_at_end_flagged_not_contained(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-02-01", "2020-03-01", 1),  # starts exactly at end
            ]
        )
        df = identify_not_contained_rows(df)
        assert df["not_contained"].to_list()[1] is True

    def test_visit_starting_after_end_flagged(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-03-01", "2020-04-01", 1),  # starts well after end
            ]
        )
        df = identify_not_contained_rows(df)
        assert df["not_contained"].to_list()[1] is True

    def test_update_not_contained_resets_window(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-03-01", "2020-04-01", 1),
            ]
        )
        df = identify_not_contained_rows(df)
        df = update_not_contained_rows(df)
        not_contained_row = df.filter(pl.col("not_contained"))
        new_start = not_contained_row["visit_start_datetime"][0]
        assert str(new_start)[:10] == "2020-03-01"

    def test_update_not_contained_sets_new_original(self):
        df = self._setup(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-03-01", "2020-04-01", 1),
            ]
        )
        df = identify_not_contained_rows(df)
        before_id = df.filter(pl.col("not_contained"))["visit_detail_id"][0]
        df = update_not_contained_rows(df)
        after_id = df.filter(pl.col("not_contained"))["visit_detail_id_original"][0]
        assert before_id == after_id


# ---------------------------------------------------------------------------
# 6. identify_next_main_visits
# ---------------------------------------------------------------------------


class TestIdentifyNextMainVisits:

    def test_first_call_sets_yes_for_first_per_person(self):
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
            ]
        )
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        first_row = df.filter(pl.col("visit_detail_id") == 0)
        assert first_row["main_visit"][0] == "Yes"

    def test_non_first_rows_stay_unknown(self):
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
            ]
        )
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        second_row = df.filter(pl.col("visit_detail_id") == 1)
        assert second_row["main_visit"][0] == "Unknown"

    def test_flags_reset_for_yes_and_unknown(self):
        df = make_df([(1, "2020-01-01", "2020-01-02", 1)])
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        # Manually set flags to True to check reset
        df = df.with_columns(
            is_contained=pl.lit(True),
            is_partial=pl.lit(True),
            not_contained=pl.lit(True),
        )
        df = identify_next_main_visits(df)
        assert df["is_contained"][0] is False
        assert df["is_partial"][0] is False
        assert df["not_contained"][0] is False


# ---------------------------------------------------------------------------
# 7. assign_visit_occurrence_id
# ---------------------------------------------------------------------------


class TestAssignVisitOccurrenceId:

    def test_ids_sequential_from_zero(self):
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (2, "2020-01-01", "2020-01-02", 1),
            ]
        )
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        df = assign_visit_occurrence_id(df)
        ids = df.filter(pl.col("main_visit") == "Yes")["visit_occurrence_id"].to_list()
        assert ids == list(range(len(ids)))

    def test_non_main_rows_get_forwarded_id(self):
        """Non-main rows should get the same visit_occurrence_id as their main visit."""
        df = make_df(
            [
                (1, "2020-01-01", "2020-02-01", 1),
                (1, "2020-01-05", "2020-01-10", 1),
            ]
        )
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        df = identify_contained_rows(df)
        df = update_contained_rows(df)
        df = identify_next_main_visits(df)
        df = assign_visit_occurrence_id(df)
        ids = df["visit_occurrence_id"].to_list()
        assert ids[0] == ids[1] == 0

    def test_no_null_ids_after_assignment(self):
        df = make_df(
            [
                (1, "2020-01-01", "2020-01-02", 1),
                (1, "2020-03-01", "2020-03-02", 1),
            ]
        )
        df = build_visit_detail(df)
        df = build_visit_detail_extended(df)
        df = identify_next_main_visits(df)
        df = assign_visit_occurrence_id(df)
        assert df["visit_occurrence_id"].null_count() == 0


# ---------------------------------------------------------------------------
# 8. build_visit_occurrence — integration tests
# ---------------------------------------------------------------------------


class TestBuildVisitOccurrenceIntegration:

    @pytest.fixture(scope="class")
    def results(self):
        df = make_df(REFERENCE_ROWS)
        df = build_visit_occurrence(df)
        visit_detail, visit_occurrence = finalize_visit_tables(df)
        return visit_detail, visit_occurrence

    # --- Shape ---

    def test_visit_occurrence_row_count(self, results):
        _, visit_occurrence = results
        assert visit_occurrence.shape[0] == 12

    def test_visit_detail_row_count(self, results):
        visit_detail, _ = results
        assert visit_detail.shape[0] == len(REFERENCE_ROWS)

    # --- visit_occurrence_id propagation in visit_detail ---

    def test_visit_detail_occurrence_id_assignment(self, results):
        visit_detail, _ = results
        expected = pl.Series(
            [
                0,
                0,
                0,
                0,
                0,
                1,
                1,
                1,
                1,
                1,
                2,
                2,
                2,
                3,
                3,
                3,
                4,
                5,
                6,
                7,
                8,
                8,
                9,
                10,
                11,
            ]
        )
        assert (visit_detail["visit_occurrence_id"] == expected).all()

    # --- visit_occurrence person_id ---

    def test_visit_occurrence_person_ids(self, results):
        _, visit_occurrence = results
        expected = pl.Series([1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 5, 5])
        assert (visit_occurrence["person_id"] == expected).all()

    # --- visit_occurrence start datetimes ---

    def test_visit_occurrence_start_datetimes(self, results):
        _, visit_occurrence = results
        expected = pl.Series(
            [
                "2020-01-01 00:00:00",
                "2020-03-01 00:00:00",
                "2020-06-01 00:00:00",
                "2020-08-01 00:00:00",
                "2021-01-01 00:00:00",
                "2021-02-01 00:00:00",
                "2021-02-01 00:00:00",
                "2021-03-01 00:00:00",
                "2022-03-01 00:00:00",
                "2020-01-01 01:00:00",
                "2020-02-01 01:00:00",
                "2020-03-01 01:00:00",
            ]
        ).str.to_datetime()
        assert (visit_occurrence["visit_start_datetime"] == expected).all()

    # --- visit_occurrence end datetimes (key: partial visit extension) ---

    def test_visit_occurrence_end_datetimes(self, results):
        _, visit_occurrence = results
        expected = pl.Series(
            [
                "2020-02-08 00:00:00",
                "2020-04-08 00:00:00",
                "2020-07-20 00:00:00",
                "2020-09-20 00:00:00",
                "2021-01-01 00:00:00",
                "2021-02-01 00:00:00",
                "2021-02-01 00:00:00",
                "2021-03-01 00:00:00",
                "2022-04-01 00:00:00",
                "2020-02-01 01:00:00",
                "2020-03-01 01:00:00",
                "2020-04-01 01:00:00",
            ]
        ).str.to_datetime()
        assert (visit_occurrence["visit_end_datetime"] == expected).all()

    # --- Columns ---

    def test_visit_occurrence_has_required_columns(self, results):
        _, visit_occurrence = results
        for col in [
            "visit_occurrence_id",
            "person_id",
            "visit_start_datetime",
            "visit_end_datetime",
            "visit_type_concept_id",
            "visit_concept_id",
            "provider_id",
        ]:
            assert col in visit_occurrence.columns, f"Missing: {col}"

    def test_visit_detail_has_required_columns(self, results):
        visit_detail, _ = results
        for col in [
            "visit_detail_id",
            "visit_occurrence_id",
            "person_id",
            "visit_detail_start_datetime",
            "visit_detail_end_datetime",
            "visit_detail_type_concept_id",
            "parent_visit_detail_id",
        ]:
            assert col in visit_detail.columns, f"Missing: {col}"

    def test_visit_detail_no_helper_columns(self, results):
        visit_detail, _ = results
        for col in [
            "main_visit",
            "is_contained",
            "is_partial",
            "not_contained",
            "visit_detail_id_original",
        ]:
            assert col not in visit_detail.columns, f"Should have been dropped: {col}"

    # --- Contained visits ---

    def test_contained_visits_have_correct_parent_id(self, results):
        visit_detail, _ = results
        contained = visit_detail.filter(pl.col("visit_occurrence_id") == 0).filter(
            pl.col("visit_detail_id")
            != visit_detail.filter(pl.col("visit_occurrence_id") == 0)[
                "visit_detail_id"
            ].min()
        )
        # Rows 1 and 2 (0-indexed) are fully contained in visit_detail_id 0
        assert contained["parent_visit_detail_id"].to_list()[:2] == [0, 0]
        # Rows 3 and 4 (0-indexed) are not fully contained in visit_detail_id 0
        assert contained["parent_visit_detail_id"].to_list()[2:] == [None, None]

    # --- Duplicate rows (person 4) ---

    def test_duplicate_rows_produce_one_main_visit(self, results):
        _, visit_occurrence = results
        p4 = visit_occurrence.filter(pl.col("person_id") == 4)
        assert p4.shape[0] == 1

    def test_duplicate_rows_type_concept_1_wins(self, results):
        """After sort, type_concept=1 comes before type_concept=2, so the main visit
        should have type_concept=1."""
        _, visit_occurrence = results
        p4 = visit_occurrence.filter(pl.col("person_id") == 4)
        assert p4["visit_type_concept_id"][0] == 1

    # --- Cross-person isolation ---

    def test_persons_do_not_share_visit_occurrence_ids(self, results):
        visit_detail, _ = results
        # Each visit_occurrence_id should map to exactly one person_id
        mapping = (
            visit_detail.select(["visit_occurrence_id", "person_id"])
            .unique()
            .group_by("visit_occurrence_id")
            .agg(pl.col("person_id").n_unique().alias("n_persons"))
        )
        assert (mapping["n_persons"] == 1).all()

    def test_person2_has_two_independent_visits(self, results):
        _, visit_occurrence = results
        p2 = visit_occurrence.filter(pl.col("person_id") == 2)
        assert p2.shape[0] == 2

    def test_person3_has_two_independent_visits(self, results):
        _, visit_occurrence = results
        p3 = visit_occurrence.filter(pl.col("person_id") == 3)
        assert p3.shape[0] == 2


# ---------------------------------------------------------------------------
# 9. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:

    def test_single_row_produces_one_main_visit(self):
        _, visit_occurrence = run_pipeline([(1, "2020-01-01", "2020-01-01", 1)])
        assert visit_occurrence.shape[0] == 1

    def test_all_same_person_same_date(self):
        """Many rows on the same date: only first becomes main, rest are contained."""
        rows = [(1, "2020-01-01", "2020-01-01", i) for i in range(1, 6)]
        _, visit_occurrence = run_pipeline(rows)
        assert visit_occurrence.shape[0] == 1

    def test_many_persons_single_visit_each(self):
        rows = [(i, "2020-01-01", "2020-01-01", 1) for i in range(1, 11)]
        _, visit_occurrence = run_pipeline(rows)
        assert visit_occurrence.shape[0] == 10

    def test_chained_partial_visits_extend_correctly(self):
        """A → B → C where each starts before the previous ends; end should reach C."""
        rows = [
            (1, "2020-01-01", "2020-02-01", 1),
            (1, "2020-01-15", "2020-03-01", 1),
            (1, "2020-02-15", "2020-04-01", 1),
        ]
        _, visit_occurrence = run_pipeline(rows)
        assert visit_occurrence.shape[0] == 1
        assert str(visit_occurrence["visit_end_datetime"][0])[:10] == "2020-04-01"

    def test_visit_occurrence_ids_are_zero_indexed_and_contiguous(self):
        rows = [
            (1, "2020-01-01", "2020-01-01", 1),
            (1, "2020-06-01", "2020-06-01", 1),
            (2, "2020-01-01", "2020-01-01", 1),
        ]
        _, visit_occurrence = run_pipeline(rows)
        ids = sorted(visit_occurrence["visit_occurrence_id"].to_list())
        assert ids == list(range(len(ids)))

    def test_visit_detail_start_end_dates_preserved(self):
        """visit_detail should keep the original row-level start/end datetimes."""
        rows = [(1, "2020-01-01", "2020-01-05", 1)]
        visit_detail, _ = run_pipeline(rows)
        assert str(visit_detail["visit_detail_start_datetime"][0])[:10] == "2020-01-01"
        assert str(visit_detail["visit_detail_end_datetime"][0])[:10] == "2020-01-05"

    def test_n_iter_max_warning_raised(self):
        """Artificially low n_iter_max should trigger a warning when rows remain."""
        rows = [(1, f"2020-0{m}-01", f"2020-0{m+1}-28", 1) for m in range(1, 8)]
        df = make_df(rows)
        with pytest.warns(UserWarning, match="unresolved"):
            build_visit_occurrence(df, n_iter_max=2)
