"""
General utilities to format tables into an OMOP-CDM structure.
"""

from datetime import date, datetime

import polars as pl
import pyarrow as pa

POLARS_NON_NULLABLE_DEFAULTS = {
    pl.Int64: 0,
    pl.Float64: 0.0,
    pl.String: "",
    pl.Date: date(1970, 1, 1),
    pl.Datetime("us"): datetime(1970, 1, 1),
}


def _arrow_type_to_polars(arrow_type: pa.DataType) -> pl.DataType:
    return pl.from_arrow(pa.array([], type=arrow_type)).dtype


def fill_omop_table(
    df: pl.DataFrame, omop_schema: pa.Schema, verbose: int = 0
) -> pl.DataFrame:
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
    pl.DataFrame
        A PyArrow table with all required columns as per the OMOP schema.

    Notes
    -----
    - For nullable fields, null values are used.
    - For non-nullable fields, default values are used (0 for int64, '' for string).
    - Warnings are issued for field types not explicitly handled (other than int64 and string).
    """
    if verbose > 0:
        print("Adding missing columns...")

    if len(df) == 0:
        polars_schema = {
            field.name: _arrow_type_to_polars(field.type) for field in omop_schema
        }
        return pl.DataFrame(schema=polars_schema)

    new_cols = []
    for field in omop_schema:
        if field.name in df.columns:
            continue

        polars_type = _arrow_type_to_polars(field.type)

        if verbose > 0:
            print(
                f"  Adding: {field.name}, Type: {field.type}, Nullable: {field.nullable}"
            )

        if field.nullable:
            col = pl.lit(None, dtype=polars_type).alias(field.name)
        else:
            default = POLARS_NON_NULLABLE_DEFAULTS[polars_type]
            col = pl.lit(default, dtype=polars_type).alias(field.name)

        new_cols.append(col)

    if new_cols:
        df = df.with_columns(new_cols)

    return df


def reorder_omop_table(df: pl.DataFrame, omop_schema: pa.Schema) -> pl.DataFrame:
    """
    Reorder columns  to match the OMOP Common Data Model schema.

    Parameters
    ----------
    df : pl.DataFrame
        The input dataframe to be reordered.
    omop_schema : pa.Schema
        The target OMOP schema that defines the desired column order.

    Returns
    -------
    pl.DataFrame
        A new datframe with columns reordered to match the OMOP schema.

    Notes
    -----
    - This function assumes that all columns in the OMOP schema are present in the input table.
    - Columns in the input table that are not in the OMOP schema will be excluded from the output.
    """
    return df.select([field.name for field in omop_schema])


def format_table(df: pl.DataFrame, omop_schema: pa.Schema) -> pl.DataFrame:
    """Formats table to provided schema, adding, removing and renaming
    columns as necessary.

    Parameters
    ----------
    df : pl.DataFrame
        Input table to be formatted
    schema : dict
        Schema information

    Returns
    -------
    pl.DataFrame
        Formatted table
    """
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)

    df = fill_omop_table(df, omop_schema)
    df = reorder_omop_table(df, omop_schema)
    df = df.cast(
        {field.name: _arrow_type_to_polars(field.type) for field in omop_schema}
    )
    return df


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
