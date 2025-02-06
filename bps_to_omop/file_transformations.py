"""Functions to help with common transformations to BPS files before
incorporating them to an OMOP-CDM instance."""

import pyarrow as pa


def melt_start_end(table: pa.Table) -> pa.Table:
    """This file does not reflect a time period between start_date and
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


##
# Define a final dict for easy access
transformations = {
    "melt_start_end": melt_start_end,
}
