import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

sys.path.append("../bps_to_omop/")
from bps_to_omop.pyarrow_utils import create_uniform_int_array


@given(
    length=st.integers(min_value=0, max_value=1000),
    value=st.integers(min_value=-1000, max_value=1000),
)
def test_array_properties(length, value):
    """Test properties that should hold true for any valid input"""
    result = create_uniform_int_array(length, value)

    # Check array properties
    assert len(result) == length
    assert pa.types.is_integer(result.type)
    assert result.type == pa.int64()

    # Check all values are equal to the input value
    if length > 0:
        assert pc.min(result).as_py() == value
        assert pc.max(result).as_py() == value
        assert pc.mean(result).as_py() == value
