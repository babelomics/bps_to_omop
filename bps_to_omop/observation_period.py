""" 
This script gathers useful functions to generate the OBSERVATION_PERIOD
table in an OMOP-CDM instance."

See:
https://ohdsi.github.io/CommonDataModel/cdm54.html#observation_period
http://omop-erd.surge.sh/omop_cdm/tables/OBSERVATION_PERIOD.html
"""

import os

import pandas as pd
import pyarrow as pa
from pyarrow import parquet

from bps_to_omop import extract as ext
from bps_to_omop import general as gen
from bps_to_omop import person as per


def ad_hoc_read(filename: str) -> pa.Table:
    """Wrapper for ad_hoc functions to deal with specific files
    if needed.

    Contains an inner dict that maps the basename of the filepath
    as a key and the function to prepare that file before processing.

    Applies the specific process and return a table ready to be process
    by the usual procedure.

    If basename is not in the dict, returns the first three columns:
    'person_id', 'start_date', 'end_date.

    Parameters
    ----------
    filename : str
        Path to file

    Returns
    -------
    pa.Table or None
        table prepared for standard processing or None
    """

    def melt_start_end(filename: str) -> pa.Table:
        """This file does not reflect a time period between start_date and
        end_date, but rather specific events at the begining and the end.
        Before proceeding, we want to separate this columns in two independent
        events."""
        df_raw = pd.read_parquet(filename)
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

    # == Define dict ==
    ad_hoc_dict = {
        "01b_Sociodemograficos_fase4.parquet": (melt_start_end, []),
    }
    # Check basename
    basename = os.path.basename(filename)
    if basename in ad_hoc_dict:
        func = ad_hoc_dict[basename][0]
        return func(filename, *ad_hoc_dict[basename][1])
    # Return normal table if not in dict
    return parquet.read_table(
        filename, columns=["person_id", "start_date", "end_date", "type_concept"]
    )
