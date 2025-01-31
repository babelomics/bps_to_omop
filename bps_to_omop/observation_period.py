# This script gathers useful functions to generate the OBSERVATION_PERIOD
# table in an OMOP-CDM instance.
#
# See:
# https://ohdsi.github.io/CommonDataModel/cdm54.html#observation_period
# http://omop-erd.surge.sh/omop_cdm/tables/OBSERVATION_PERIOD.html

import os

import pandas as pd
import pyarrow as pa
from pyarrow import parquet

from bps_to_omop import extract as ext
from bps_to_omop import general as gen
from bps_to_omop import person as per


def prepare_table_raw_to_rare(
    table_raw: pa.Table, period_type: str, date_cols: list[str]
) -> pa.Table:
    """Prepare a just read table (table_raw) to be grouped by
    observation dates.

    Parameters
    ----------
    table_raw : pa.Table
        Table read from the file. Includes at least a column with
        person_id and the columns with date information.
    period_type : str
        period_type code to be assigned to the table
    date_cols : list[str]
        column names to be used to group by dates

    Returns
    -------
    pa.Table
        table ready to be grouped by person and dates
    """

    # Build person_id and period_type
    person_id, _ = per.transform_person_id(table_raw, table_raw.column_names[0])
    period_type_concept_id = gen.create_uniform_int_array(
        len(person_id), value=period_type
    )

    # "Buscamos" la fecha inicial y final
    start_dates_0, end_dates_0 = ext.find_start_end_dates(table_raw, date_cols)

    # -- Make sure every variable has the same lenght
    if len(person_id) != len(start_dates_0):
        raise ValueError("person_id and dates lenghts are not equal.")

    # -- Create the table
    table_rare = pa.Table.from_arrays(
        [person_id, start_dates_0, end_dates_0, period_type_concept_id],
        names=[
            "person_id",
            "observation_period_start_date",
            "observation_period_end_date",
            "period_type_concept_id",
        ],
    )
    # Sort it
    table_rare = table_rare.sort_by("observation_period_start_date")
    return table_rare


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
