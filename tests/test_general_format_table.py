import sys

import pyarrow as pa
import pytest
from pandas import to_datetime
from pandas.testing import assert_frame_equal

sys.path.append("../bps_to_omop/")
from bps_to_omop.general import fill_omop_table, format_table, reorder_omop_table


# == Fixtures =========================================================
@pytest.fixture
def test_schema():
    """Create a temporary schema structure for testing."""
    return pa.schema(
        [
            ("var_date", pa.date32(), False),
            ("var_float", pa.float64(), False),
            ("var_int", pa.int64(), False),
            ("var_string", pa.string(), False),
            ("var_timestamp", pa.timestamp(), False),
            ("var_date_not_null", pa.date32(), True),
            ("var_float_not_null", pa.float64(), True),
            ("var_int_not_null", pa.int64(), True),
            ("var_string_not_null", pa.string(), True),
            ("var_timestamp_not_null", pa.timestamp(), True),
        ]
    )


# == TESTS ==============================================================================
def test_table_fills():
    """Test that columns are filled"""
    input_table = pa.table(
        {
            "var_int": [1, 1, 2],
            "var_date": pa.array(
                to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                type=pa.date32(),
            ),
            "var_timestamp": pa.array(
                to_datetime(["2024-01-31", "2024-02-28", "2024-03-31"]),
                type=pa.timestamp("us"),
            ),
            "var_string": ["A", "B", "C"],
        }
    )

    expected_table = pa.table(
        {
            "var_int": [1, 1, 2],
            "var_date": pa.array(
                to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                type=pa.date32(),
            ),
            "var_timestamp": pa.array(
                to_datetime(["2024-01-31", "2024-02-28", "2024-03-31"]),
                type=pa.timestamp("us"),
            ),
            "var_string": ["A", "B", "C"],
            "var_date_not_null": pa.array(["", "", ""], type=pa.date32()),
            "var_float_not_null": pa.array([0, 0, 0], type=pa.float64()),
            "var_int_not_null": pa.array([0, 0, 0], type=pa.int64()),
            "var_string_not_null": pa.array(["", "", ""], type=pa.string()),
            "var_timestamp_not_null": pa.array(["", "", ""], type=pa.timestamp("us")),
        }
    )

    output_table = fill_omop_table(input_table)
    assert_frames_equal(output_table.to_pandas(), expected_table.to_pandas())
