import sys

import pyarrow as pa
import pytest
from pandas import to_datetime
from pandas.testing import assert_frame_equal

sys.path.append("../bps_to_omop/")
from utils.format_to_omop import fill_omop_table, format_table, reorder_omop_table


# == Fixtures =========================================================
@pytest.fixture
def test_schema():
    """Create a temporary schema structure for testing."""
    return pa.schema(
        [
            ("var_int", pa.int64(), False),
            ("var_float", pa.float64(), False),
            ("var_string", pa.string(), False),
            ("var_date", pa.date32(), False),
            ("var_timestamp", pa.timestamp("us"), False),
            ("var_int_nullable", pa.int64(), True),
            ("var_float_nullable", pa.float64(), True),
            ("var_string_nullable", pa.string(), True),
        ]
    )


# == TESTS ==============================================================================
def test_table_fills(test_schema):
    """Test that columns are filled"""
    input_table = pa.table(
        {
            "var_int": [1, 1, 2],
            "var_string": ["A", "B", "C"],
            "var_date": pa.array(
                to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                type=pa.date32(),
            ),
            "var_timestamp": pa.array(
                to_datetime(["2024-01-31", "2024-02-28", "2024-03-31"]),
                type=pa.timestamp("us"),
            ),
        }
    )

    output_table = fill_omop_table(input_table, test_schema)

    expected_table = pa.table(
        {
            "var_int": [1, 1, 2],
            "var_string": ["A", "B", "C"],
            "var_date": pa.array(
                to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                type=pa.date32(),
            ),
            "var_timestamp": pa.array(
                to_datetime(["2024-01-31", "2024-02-28", "2024-03-31"]),
                type=pa.timestamp("us"),
            ),
            "var_float": pa.array([0, 0, 0], type=pa.float64()),
            "var_int_nullable": pa.nulls(3, type=pa.int64()),
            "var_float_nullable": pa.nulls(3, type=pa.float64()),
            "var_string_nullable": pa.nulls(3, type=pa.string()),
        }
    )

    assert_frame_equal(output_table.to_pandas(), expected_table.to_pandas())
