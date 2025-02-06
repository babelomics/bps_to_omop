"""Functions to help with common transformations to tables before
incorporating them to an OMOP-CDM instance."""

import pyarrow as pa


# -- Main function --
def apply_transformation(table: pa.Table, params: dict, key: str) -> pa.Table:
    """
    Apply transformations to a PyArrow table based on provided parameters.

    Parameters
    ----------
    table : pa.Table
        Input PyArrow table to be transformed
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
    # Check if transformations exist for the specific key
    transformations = params.get("transformations", {}).get(key, [])

    # If no transformations, return the original table
    if not transformations:
        return table

    # Apply each transformation function
    for transform_func in transformations:
        transformed_table = transform_func(transformed_table)

    return transformed_table


# -- Helper functions --
def melt_start_end(table: pa.Table) -> pa.Table:
    """This table does not reflect a time period between start_date and
    end_date, but rather specific events at the begining and the end.
    Before proceeding, we want to separate this columns in two independent
    events."""
    df_raw = table.to_pandas()
    # nos quedamos solo con las columnas que queremos
    df_clean = df_raw[["person_id", "start_date", "end_date"]]
    # Hacemos un melt para pasar de dataframe ancho a largo
    df_melt = df_clean.melt(id_vars=["person_id"], value_name="fecha")
    # Eliminamos las columnas sobrantes y quitamos los nan
    df_melt = df_melt[["person_id", "fecha"]]
    df_melt = df_melt.dropna()
    # Reasignamos la nueva columna fecha al principio y al final
    df_melt["start_date"] = df_melt["fecha"]
    df_melt["end_date"] = df_melt["fecha"]
    # Añadimos el type_concept
    df_melt["type_concept"] = df_raw["type_concept"][0]
    # Nos quedamos sólo con lo que queremos una vez más
    df_melt = df_melt[["person_id", "start_date", "end_date", "type_concept"]]
    # Quitamos duplicados, que puede ver
    df_melt = df_melt.drop_duplicates(ignore_index=True)
    # Pasamos a pyarrow table y devolvemos
    return pa.Table.from_pandas(df_melt, preserve_index=False)
