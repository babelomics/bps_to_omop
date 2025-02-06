import pyarrow as pa

from bps_to_omop.table_transformer import apply_transformation


def test_no_transformations():
    """Test function when no transformations are specified."""
    # Create a sample table
    data = {"column1": pa.array([1, 2, 3]), "column2": pa.array(["a", "b", "c"])}
    original_table = pa.Table.from_pydict(data)

    # Prepare params with no transformations
    params = {}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Assert the table remains unchanged
    assert result.equals(original_table)


def test_single_transformation():
    """Test applying a single transformation function."""
    # Create a sample table
    data = {"column1": pa.array([1, 2, 3]), "column2": pa.array(["a", "b", "c"])}
    original_table = pa.Table.from_pydict(data)

    # Define a sample transformation function
    def double_first_column(table):
        # Create a new column by doubling the first column
        new_column = pa.array(table.column("column1").to_numpy() * 2)
        return table.set_column(
            table.column_names.index("column1"), "column1", new_column
        )

    # Prepare params with transformation
    params = {"transformations": {"test_key": [double_first_column]}}

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Expected result
    expected_data = {
        "column1": pa.array([2, 4, 6]),
        "column2": pa.array(["a", "b", "c"]),
    }
    expected_table = pa.Table.from_pydict(expected_data)

    # Assert the table is transformed correctly
    assert result.equals(expected_table)


def test_multiple_transformations():
    """Test applying multiple transformation functions."""
    # Create a sample table
    data = {"column1": pa.array([1, 2, 3]), "column2": pa.array(["a", "b", "c"])}
    original_table = pa.Table.from_pydict(data)

    # Define transformation functions
    def double_first_column(table):
        new_column = pa.array(table.column("column1").to_numpy() * 2)
        return table.set_column(
            table.column_names.index("column1"), "column1", new_column
        )

    def add_constant_to_column(table):
        new_column = pa.array(table.column("column1").to_numpy() + 10)
        return table.set_column(
            table.column_names.index("column1"), "column1", new_column
        )

    # Prepare params with multiple transformations
    params = {
        "transformations": {"test_key": [double_first_column, add_constant_to_column]}
    }

    # Apply transformation
    result = apply_transformation(original_table, params, "test_key")

    # Expected result
    expected_data = {
        "column1": pa.array([22, 44, 66]),
        "column2": pa.array(["a", "b", "c"]),
    }
    expected_table = pa.Table.from_pydict(expected_data)

    # Assert the table is transformed correctly
    assert result.equals(expected_table)
