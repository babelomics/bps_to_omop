"""
General utilities to format tables into OMOP-CDM.
"""

import pyarrow as pa

import bps_to_omop.pyarrow_utils as pa_utils


def fill_omop_table(
    table: pa.Table, omop_schema: pa.Schema, verbose: int = 0
) -> pa.Table:
    """
    Fill missing columns in a PyArrow table to match the OMOP Common Data Model schema.

    This function adds missing columns to the input table based on the provided OMOP schema.
    It handles both nullable and non-nullable fields, creating appropriate default values.

    Parameters
    ----------
    table : pa.Table
        The input PyArrow table to be filled.
    omop_schema : pa.Schema
        The target OMOP schema to conform to.
    verbose : int, optional, default 0
        Verbosity level for function output.
        0: No output
        1+: Prints information about added columns.

    Returns
    -------
    pa.Table
        A PyArrow table with all required columns as per the OMOP schema.

    Notes
    -----
    - For nullable fields, null values are used.
    - For non-nullable fields, default values are used (0 for int64, '' for string).
    - Warnings are issued for field types not explicitly handled (other than int64 and string).
    """
    if verbose > 0:
        print("Adding missing columns...")

    table_size = len(table)
    missing_fields = [
        field for field in omop_schema if field.name not in table.column_names
    ]

    for field in missing_fields:
        if verbose > 0:
            print(
                f"  Adding: {field.name}, Type: {field.type}, Nullable: {field.nullable}"
            )

        if field.type not in [pa.int64(), pa.string(), pa.float64()]:
            print(
                f"Unhandled field type {field.type} for field {field.name}. "
                f"Defaulting to string type."
            )
            field = field.with_type(pa.string())

        default_value = (
            None
            if field.nullable
            else (
                0
                if field.type == pa.int64()
                else 0.0 if field.type == pa.float64() else ""
            )
        )

        if field.nullable:
            array = (
                pa_utils.create_null_int_array(table_size)
                if field.type == pa.int64()
                else (
                    pa_utils.create_null_double_array(table_size)
                    if field.type == pa.float64()
                    else pa_utils.create_null_str_array(table_size)
                )
            )
        else:
            array = (
                pa_utils.create_uniform_int_array(table_size, default_value)
                if field.type == pa.int64()
                else (
                    pa_utils.create_uniform_double_array(table_size, default_value)
                    if field.type == pa.float64()
                    else pa_utils.create_uniform_str_array(table_size, default_value)
                )
            )

        table = table.append_column(field.name, array)

    return table


def reorder_omop_table(table: pa.Table, omop_schema: pa.Schema) -> pa.Table:
    """
    Reorder columns of a PyArrow table to match the OMOP Common Data Model schema.

    Parameters
    ----------
    table : pa.Table
        The input PyArrow table to be reordered.
    omop_schema : pa.Schema
        The target OMOP schema that defines the desired column order.

    Returns
    -------
    pa.Table
        A new PyArrow table with columns reordered to match the OMOP schema.

    Notes
    -----
    - This function assumes that all columns in the OMOP schema are present in the input table.
    - Columns in the input table that are not in the OMOP schema will be excluded from the output.
    """
    column_order = [field.name for field in omop_schema]
    return table.select(column_order)


def format_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Formats table to provided schema, adding, removing and renaming
    columns as necessary.

    Parameters
    ----------
    df : pa.Table
        Input table to be formatted
    schema : dict
        Schema information

    Returns
    -------
    pa.Table
        Formatted table
    """
    # -- Finishing up
    # Fill other fields
    table = fill_omop_table(table, schema)
    table = reorder_omop_table(table, schema)
    # Cast to schema
    table = table.cast(schema)

    return table


def rename_table_columns(table: pa.Table, col_map: dict) -> pa.Table:
    """
    Rename columns in a pyarrow Table based on a mapping dictionary.

    Columns not included in the mapping dictionary will be left as is.

    Parameters
    ----------
    table : pa.Table
        The input pyarrow Table whose columns need to be renamed.
    col_map : dict
        Dictionary mapping old column names to new column names.

    Returns
    -------
    pa.Table
        A new pyarrow Table with renamed columns.

    Raises
    ------
    ValueError
        If col_map contains columns that don't exist in the table.

    Examples
    --------
    >>> import pyarrow as pa
    >>> data = pa.table({'a': [1, 2], 'b': [3, 4]})
    >>> col_map = {'a': 'x', 'b': 'y'}
    >>> renamed_table = rename_table_columns(data, col_map)
    >>> renamed_table.column_names
    ['x', 'y']
    """
    # Validate that all columns in col_map exist in the table
    invalid_cols = set(col_map.keys()) - set(table.column_names)
    if invalid_cols:
        raise ValueError(f"Column(s) {invalid_cols} not found in table")

    # Create a mapping for all columns, using original names for unmapped columns
    renamed_cols = {col: col_map.get(col, col) for col in table.column_names}

    # Return the table with renamed columns
    return table.rename_columns([renamed_cols[col] for col in table.column_names])
