"""
Tests for omop_format utilities: fill_omop_table, reorder_omop_table, format_table.
"""

from datetime import date, datetime

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from pyarrow import date32, float64, int64, schema, string, timestamp

from bps_to_omop.utils.format_to_omop import (
    fill_omop_table,
    format_table,
    reorder_omop_table,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_SCHEMA = schema(
    [
        ("person_id", int64(), False),  # non-nullable int
        ("name", string(), True),  # nullable string
        ("score", float64(), True),  # nullable float
        ("birth_date", date32(), True),  # nullable date
        ("recorded_at", timestamp("us"), True),  # nullable datetime
    ]
)

REQUIRED_ONLY_SCHEMA = schema(
    [
        ("id", int64(), False),
        ("code", string(), False),
        ("value", float64(), False),
    ]
)


# ---------------------------------------------------------------------------
# _arrow_type_to_polars (indirectly tested via fill/format)
# (!) person_id shoud never be zero, but we're just using it here to check
#     that the function works.
#     Currently, it is not the job of this function to check that there
#     are no empty person_id
# ---------------------------------------------------------------------------


class TestFillOmopTable:

    def test_adds_missing_nullable_columns_as_null(self):
        df = pl.DataFrame({"person_id": [1, 2]})
        result = fill_omop_table(df, SIMPLE_SCHEMA)

        assert result["name"].is_null().all()
        assert result["score"].is_null().all()
        assert result["birth_date"].is_null().all()
        assert result["recorded_at"].is_null().all()

    def test_adds_missing_non_nullable_int_with_default(self):
        df = pl.DataFrame({"name": ["alice"]})
        result = fill_omop_table(df, SIMPLE_SCHEMA)

        assert result["person_id"][0] == 0

    def test_adds_missing_non_nullable_string_with_default(self):
        df = pl.DataFrame({"id": [1]})
        result = fill_omop_table(df, REQUIRED_ONLY_SCHEMA)

        assert result["code"][0] == ""

    def test_adds_missing_non_nullable_float_with_default(self):
        df = pl.DataFrame({"id": [1]})
        result = fill_omop_table(df, REQUIRED_ONLY_SCHEMA)

        assert result["value"][0] == 0.0

    def test_existing_columns_are_not_overwritten(self):
        df = pl.DataFrame({"person_id": [99], "name": ["bob"]})
        result = fill_omop_table(df, SIMPLE_SCHEMA)

        assert result["person_id"][0] == 99
        assert result["name"][0] == "bob"

    def test_all_columns_present_returns_unchanged(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "name": ["alice"],
                "score": [9.5],
                "birth_date": [date(1990, 1, 1)],
                "recorded_at": [datetime(2020, 6, 1)],
            }
        )
        result = fill_omop_table(df, SIMPLE_SCHEMA)
        assert result.columns == df.columns

    def test_correct_polars_dtypes_assigned(self):
        df = pl.DataFrame({"person_id": [1]})
        result = fill_omop_table(df, SIMPLE_SCHEMA)

        assert result["name"].dtype == pl.String
        assert result["score"].dtype == pl.Float64
        assert result["birth_date"].dtype == pl.Date
        assert result["recorded_at"].dtype == pl.Datetime("us")

    def test_verbose_prints(self, capsys):
        df = pl.DataFrame({"person_id": [1]})
        fill_omop_table(df, SIMPLE_SCHEMA, verbose=1)
        captured = capsys.readouterr()
        assert "Adding missing columns" in captured.out
        assert "name" in captured.out


# ---------------------------------------------------------------------------
# reorder_omop_table
# ---------------------------------------------------------------------------


class TestReorderOmopTable:

    def test_reorders_columns_to_schema_order(self):
        df = pl.DataFrame(
            {
                "score": [1.0],
                "name": ["x"],
                "birth_date": [date(2000, 1, 1)],
                "recorded_at": [datetime(2020, 1, 1)],
                "person_id": [1],
            }
        )
        result = reorder_omop_table(df, SIMPLE_SCHEMA)
        assert result.columns == [f.name for f in SIMPLE_SCHEMA]

    def test_drops_extra_columns(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "name": ["x"],
                "score": [1.0],
                "birth_date": [date(2000, 1, 1)],
                "recorded_at": [datetime(2020, 1, 1)],
                "extra_col": ["drop me"],
            }
        )
        result = reorder_omop_table(df, SIMPLE_SCHEMA)
        assert "extra_col" not in result.columns


# ---------------------------------------------------------------------------
# format_table
# ---------------------------------------------------------------------------


class TestFormatTable:

    def test_fills_reorders_and_casts(self):
        df = pl.DataFrame({"name": ["alice", "bob"]})
        result = format_table(df, SIMPLE_SCHEMA)

        assert result.columns == [f.name for f in SIMPLE_SCHEMA]
        assert result["person_id"].to_list() == [0, 0]
        assert result["name"].to_list() == ["alice", "bob"]
        assert result["score"].is_null().all()

    def test_casts_to_correct_types(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "name": ["x"],
                "score": [2],
                "birth_date": [date(2000, 1, 1)],
                "recorded_at": [datetime(2020, 1, 1)],
            }
        )
        result = format_table(df, SIMPLE_SCHEMA)

        assert result["person_id"].dtype == pl.Int64
        assert result["score"].dtype == pl.Float64
        assert result["birth_date"].dtype == pl.Date
        assert result["recorded_at"].dtype == pl.Datetime("us")

    def test_empty_dataframe(self):
        df = pl.DataFrame()
        result = format_table(df, SIMPLE_SCHEMA)

        assert result.columns == [f.name for f in SIMPLE_SCHEMA]
        assert len(result) == 0

    def test_returns_pyarrow_table_when_input_is_pyarrow(self):
        arrow_table = pa.table({"person_id": pa.array([1, 2], type=pa.int64())})
        result = format_table(arrow_table, SIMPLE_SCHEMA)
        assert isinstance(result, pa.Table)

    def test_returns_pandas_dataframe_when_input_is_pandas(self):
        pd_df = pd.DataFrame({"person_id": [1, 2]})
        result = format_table(pd_df, SIMPLE_SCHEMA)
        assert isinstance(result, pd.DataFrame)

    def test_returns_polars_dataframe_when_input_is_polars(self):
        pl_df = pl.DataFrame({"person_id": [1, 2]})
        result = format_table(pl_df, SIMPLE_SCHEMA)
        assert isinstance(result, pl.DataFrame)
