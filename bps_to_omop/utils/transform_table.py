"""
Functions to help with common transformations to tables before
incorporating them to an OMOP-CDM instance.
"""

import polars as pl


# -- Main function --
def apply_transformation(table: pl.DataFrame, params: dict, key: str) -> pl.DataFrame:
    """
    Apply transformations to a PyArrow table based on provided parameters.

    Parameters
    ----------
    table : pl.DataFrame
        Input polars dataframe table to be transformed
    params : dict
        Dictionary containing transformation parameters
    key : str
        Specific key to identify transformations

    Returns
    -------
    pa.Table
        Transformed PyArrow table

    Notes
    -----
    - Skips transformation if no transformations are specified for the given key
    - Applies each transformation function sequentially
    """
    # If no transformations, return the original table
    if not params.get("transformations", {}):
        return table
    elif not params.get("transformations", {}).get(key, []):
        return table

    # Apply each transformation function
    transformed_table = table
    for func in params.get("transformations", {}).get(key, []):
        transformed_table = transformations[func](transformed_table)

    return transformed_table


# -- Helper functions --
def melt_start_end(table: pl.DataFrame) -> pl.DataFrame:
    """This table does not reflect a time period between start_date and
    end_date, but rather specific events at the beginning and the end.
    Before proceeding, we want to separate these columns into two independent
    events."""
    type_concept = table["type_concept"][0]

    return (
        table.select(["person_id", "start_date", "end_date"])
        .unpivot(index="person_id", value_name="fecha")
        .select(["person_id", "fecha"])
        .drop_nulls()
        .with_columns(
            [
                pl.col("fecha").alias("start_date"),
                pl.col("fecha").alias("end_date"),
                pl.lit(type_concept).alias("type_concept"),
            ]
        )
        .select(["person_id", "start_date", "end_date", "type_concept"])
        .unique()
    )


def remove_end_date(table: pl.DataFrame) -> pl.DataFrame:
    """
    Remove the end_date column and use start_date as the new end_date.

    Parameters
    ----------
    table : pl.DataFrame
        Input Polars DataFrame.

    Returns
    -------
    pl.DataFrame
        DataFrame with end_date replaced by start_date.
    """
    return table.with_columns(pl.col("start_date").alias("end_date"))


# -- Definition of transformations
transformations = {
    "melt_start_end": melt_start_end,
    "remove_end_date": remove_end_date,
}
