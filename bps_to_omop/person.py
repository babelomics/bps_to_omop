"""
This file contains usual transformations to generate the PERSON
table of an OMOP-CDM database instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#person

http://omop-erd.surge.sh/omop_cdm/tables/PERSON.html

"""

import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import format_to_omop, map_to_omop, pyarrow_utils


def transform_person_id(
    input_table: pa.Table, input_fieldname: str
) -> tuple[pa.array, pa.array]:
    """Transform field_name from input_table to fit into
    person_id et al fields in an OMOP-CDM instance.

    person_id tiene must be integer, we removed letters and
    changed to int32.

    Parameters
    ----------
    input_table : pa.Table
        Input table to extract the field from.
    input_fieldname : str
        field name of the input table

    Returns
    -------
    tuple[pa.array,pa.Array]
        tuple with two pyarrow arrays for the OMOP fields:
        ('person_id','person_source_value')
    """
    person_source_value_tmp = input_table[input_fieldname]
    # Remove first letters and switch to int32
    person_id_tmp = pc.utf8_slice_codeunits(
        person_source_value_tmp, 2
    )  # pylint: disable=E1101
    person_id_tmp = person_id_tmp.cast(pa.int64())

    return (person_id_tmp, person_source_value_tmp)


# == year_of_birth et al ==
# Esta columna se extrae del campo FECNAC
# De la columna BPS FECNAC con formato YYYYMMDD se extraen
# 3 grupos de enteros, year_of_birth, month_of_birth y day_of_birth
def transform_person_dates(
    input_table: pa.Table, input_fieldname: str
) -> tuple[pa.array, pa.array, pa.array]:
    """Transforms from FECNAC column in a BPS instance
    to year_of_birth, month_of_birth and day_of_birth
    fields compatible with an OMOP-CDM instance.

    Parameters
    ----------
    input_table : pa.Table
        Input table to extract the field from.
    input_fieldname : str
        field name of the input table

    Returns
    -------
    tuple[pa.array, pa.array, pa.array]
        tuple with two pyarrow arrays for the OMOP fields:
        ('year_of_birth','month_of_birth','day_of_birth)
    """

    # Sacamos el array que transformaremos
    date = input_table[input_fieldname]
    # Año: Nos quedamos los 4 primeros numeros y cambiamos a int32
    try:
        year_of_birth = pc.year(date).cast(pa.int32())  # pylint: disable=E1101
        month_of_birth = pc.month(date).cast(pa.int32())  # pylint: disable=E1101
        day_of_birth = pc.day(date).cast(pa.int32())  # pylint: disable=E1101
    except Exception as exc:
        raise ValueError(
            "Could not infer dates. Check column type is correct."
        ) from exc

    # Acabar con person_id
    return (year_of_birth, month_of_birth, day_of_birth)


def build_date_columns(input_table: pa.table):
    """
    Extracts year, month, and day components from a start_date column and appends them
    as new columns to the input table.

    Parameters
    ----------
    input_table : pyarrow.Table
        Input table containing a 'start_date' column.

    Returns
    -------
    pyarrow.Table
        A new table with three additional columns:
        - 'year_of_birth': Extracted year from start_date
        - 'month_of_birth': Extracted month from start_date
        - 'day_of_birth': Extracted day from start_date

    Examples
    --------
    >>> table = pa.table({'start_date': ['2000-01-01', '1995-12-31']})
    >>> result = build_date_columns(table)
    >>> print(result.column_names)
    ['start_date', 'year_of_birth', 'month_of_birth', 'day_of_birth']
    """

    year, month, day = transform_person_dates(input_table, "start_date")
    output_table = input_table.append_column("year_of_birth", year)
    output_table = output_table.append_column("month_of_birth", month)
    output_table = output_table.append_column("day_of_birth", day)

    return output_table


def transform_gender(
    input_table: pa.Table, input_fieldname: tuple[str, str], mapping: dict
) -> tuple[pa.array, pa.array, pa.array]:
    """
    Generates gender relate field for an OMOP-CDM instance.

    Parameters
    ----------
    input_table : pa.Table
        Table from wich to retrieve gender data
    input_fieldname : tuple[str, str]
        Field names where gender code and natural language values are
        stored. First item is the code, second item is natural language.
    mapping : dict
        mapping to transform from source to OMOP

    Returns
    -------
    tuple[pa.array, pa.array, pa.array]
        _description_
    """
    # TODO: Simplify this. Sometimes you only have one field. Just do the mapping.
    # -- Sacamos el array que transformaremos
    # Por convenio, el primer campo, por defecto CODSEXO,
    # representa el código del género, que definirá el
    # gender_concept_id y al gender_source_concept_id.
    # El segundo campo representa el valor en lenguage natural,
    # que irá al gender_source_value
    inp_field_code = input_table[input_fieldname[0]]
    inp_field_lang = input_table[input_fieldname[1]]

    # Guardamos ya los sources values
    gender_source_value_tmp = inp_field_lang
    gender_source_concept_id_tmp = inp_field_code
    # Pasamos a numpy
    tmp = inp_field_code.to_numpy()
    try:
        gender_concept_id_tmp = np.vectorize(mapping.get)(tmp)
    except TypeError as exc:
        raise TypeError("Mapping was unsuccesful. Check mapping completeness.") from exc
    return (
        gender_concept_id_tmp,
        gender_source_concept_id_tmp,
        gender_source_value_tmp,
    )


def process_person_table(data_dir: Path, params_person: dict):
    """
    Process sociodemo data files and create an OMOP-formatted PERSON table.

    Parameters
    ----------
    data_dir : Path
        Base directory containing input and output subdirectories.
    params_location : dict
        Configuration dictionary with keys: 'input_dir', 'output_dir',
        'input_files', and optional 'column_name_map', 'column_values_map',
        'constant_values', 'location_table_path', 'source_to_location'.

    Returns
    -------
    None
        Writes LOCATION.parquet file to the specified output directory.
    """
    # -- Load parameters --------------------------------------------------
    # -- Load yaml file and related info
    input_dir = params_person["input_dir"]
    output_dir = params_person["output_dir"]
    input_files = params_person["input_files"]
    column_name_map = params_person.get("column_name_map", {})
    column_values_map = params_person.get("column_values_map", {})
    constant_values = params_person.get("constant_values", {})
    location_table_path = params_person.get("location_table_path", False)
    source_to_location = params_person.get("source_to_location", {})

    # == Prepare dirs and files
    # Create output_dir
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)
    os.makedirs(data_dir / output_dir, exist_ok=True)

    if location_table_path:
        location_table = parquet.read_table(data_dir / location_table_path)

    # == Get the list of all relevant files ====================================
    # Get files
    table_person = []

    for f in input_files:
        tmp_table = parquet.read_table(data_dir / input_dir / f)

        # -- Build date columns ---------------------------------------
        tmp_table = build_date_columns(tmp_table)

        # -- Rename columns -------------------------------------------
        # First ensure we have a dict with the relevant info
        tmp_colmap = column_name_map.get(f, {})
        # Add the mapping from start_date to birth_datetime
        tmp_colmap = {**tmp_colmap, "start_date": "birth_datetime"}
        # Transform the column names
        tmp_table = format_to_omop.rename_table_columns(tmp_table, tmp_colmap)

        # -- Apply values mapping -------------------------------------
        tmp_valmap = column_values_map.get(f, {})
        if tmp_valmap:
            tmp_table = map_to_omop.apply_source_mapping(tmp_table, tmp_valmap)

        # -- Add Constant values --------------------------------------
        tmp_cteval = constant_values.get(f, {})
        if tmp_cteval:
            for col_name, col_value in tmp_cteval.items():
                if isinstance(col_value, (int, float)):
                    col_values = pyarrow_utils.create_uniform_double_array(
                        tmp_table.shape[0], col_value
                    )
                    tmp_table = tmp_table.append_column(col_name, col_values)
                else:
                    col_values = pyarrow_utils.create_uniform_str_array(
                        tmp_table.shape[0], col_value
                    )
                    tmp_table = tmp_table.append_column(col_name, col_values)

        # -- Add link to LOCATION table -------------------------------
        tmp_location = source_to_location.get(f, {})
        if tmp_location:
            # Retrieve the col that link to the location_id
            ((source_colname, location_colname),) = tmp_location.items()

            # Build the dict that links current table to location_id
            mapping = dict(
                zip(
                    location_table[location_colname].to_numpy(),
                    location_table["location_id"].to_numpy(),
                )
            )

            # Vectorize the mapping function with a default value of None
            vectorized_map = np.vectorize(lambda x: mapping.get(x, None))

            # Make sure the dtype the location_table is the same as in the source
            source_col = (
                tmp_table[source_colname]
                .cast(location_table[location_colname].type)
                .to_numpy()
            )

            # Apply mapping and convert to PyArrow array
            mapped_values = pa.array(vectorized_map(source_col))

            # -- Append to table
            # Delete previous location_id
            if "location_id" in tmp_table.columns:
                raise ValueError(f" File {f} already has a location_id column")
            tmp_table = tmp_table.append_column("location_id", mapped_values)

        # -- Format the table -----------------------------------------
        tmp_table = format_to_omop.format_table(tmp_table, omop_schemas["PERSON"])

        # -- Append to list -------------------------------------------
        table_person.append(tmp_table)

    # Concat and save
    table_person = pa.concat_tables(table_person)
    # Save
    parquet.write_table(table_person, data_dir / output_dir / "PERSON.parquet")
