import pyarrow as pa
from utils.transform_table import apply_transformation


def test_no_transformations():
    """Test function when no transformations are specified."""
    # Create a sample table
    data = {
        "person_id": pa.array([1, 2, 3]),
        "start_date": pa.array([1, 2, 3]),
        "end_date": pa.array([4, 5, 6]),
        "type_concept": pa.array([1, 1, 1]),
    }
    original_table = pa.Table.from_pydict(data)

    # Prepare params with no transformations
    params = {}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Assert the table remains unchanged
    assert result.equals(original_table)


def test_no_file_transformations():
    """Test function when transformations key is present but none are
    specified for any file."""
    # Create a sample table
    data = {
        "person_id": pa.array([1, 2, 3]),
        "start_date": pa.array([1, 2, 3]),
        "end_date": pa.array([4, 5, 6]),
        "type_concept": pa.array([1, 1, 1]),
    }
    original_table = pa.Table.from_pydict(data)

    # Prepare params with no transformations
    params = {"transformations": {}}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Assert the table remains unchanged
    assert result.equals(original_table)


def test_single_transformation():
    """Test applying a single transformation function."""
    # Create a sample table
    data = {
        "person_id": pa.array([1, 2, 3]),
        "start_date": pa.array([1, 2, 3]),
        "end_date": pa.array([4, 5, 6]),
        "type_concept": pa.array([1, 1, 1]),
    }
    original_table = pa.Table.from_pydict(data)

    # Prepare params with transformation
    params = {"transformations": {"test_key": ["remove_end_date"]}}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Expected result
    expected_data = {
        "person_id": pa.array([1, 2, 3]),
        "start_date": pa.array([1, 2, 3]),
        "end_date": pa.array([1, 2, 3]),
        "type_concept": pa.array([1, 1, 1]),
    }
    expected_table = pa.Table.from_pydict(expected_data)

    # Assert the table is transformed correctly
    assert result.equals(expected_table)


def test_multiple_transformations():
    """Test applying multiple transformation functions."""
    # Create a sample table
    data = {
        "person_id": pa.array([1, 2, 3]),
        "start_date": pa.array([1, 2, 3]),
        "end_date": pa.array([4, 5, 6]),
        "type_concept": pa.array([1, 1, 1]),
    }
    original_table = pa.Table.from_pydict(data)

    # Prepare params with multiple transformations
    params = {"transformations": {"test_key": ["melt_start_end", "remove_end_date"]}}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Expected result
    expected_data = {
        "person_id": pa.array([1, 2, 3, 1, 2, 3]),
        "start_date": pa.array([1, 2, 3, 4, 5, 6]),
        "end_date": pa.array([1, 2, 3, 4, 5, 6]),
        "type_concept": pa.array([1, 1, 1, 1, 1, 1]),
    }
    expected_table = pa.Table.from_pydict(expected_data)

    # Assert the table is transformed correctly
    assert result.equals(expected_table)
