"""
This module contains neccesary functions to build the VISIT_OCCURRENCE
table of an OMOP-CDM instance.

See:
https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
http://omop-erd.surge.sh/omop_cdm/tables/VISIT_OCCURRENCE.html
"""

import os
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

from bps_to_omop import extract as ext
from bps_to_omop import general as gen
from bps_to_omop.omop_schemas import omop_schemas

# Aquí definimos las funciones que se usarán más adelante.
# Para que tengan la misma sintaxis, todas piden:
# - la tabla original
# - el array con los valores de la columna donde aplicar los cambios
# - el código a aplicar en el array anterior
# - kwargs! Algunas funciones tienen parámetros extras que se añaden como
#           un diccionario.
# La idea es partir de un array con 0s, (concept_id = not defined concept)
# Luego, cada función aplica una condición determinada y cambia aquellas filas
# que la verifiquen, dejando el array original donde no se verifique.
# Para esto se usa np.where(), por eso se pide la tabla y el array con los valores
# => Este array se usa luego para hacer la tabla final, sin cambiar la tabla inicial.


def get_visit_concept_id(
    table_raw: pa.Table, functions: list[dict], verbose: int = 0
) -> pa.Array:
    """Given a pyarrow table and a list of functions, this function
    will apply the codification contained within the dict.

    Parameters
    ----------
    f : str
        filename.
    table_raw : pa.Table
        pyarrow table with at least person_id, start_date and end_date
        columns.
    functions : list
        contains the function and its parameters in the following order:
            - str,      name of the function to apply. e.g. 'single_code'
            - int,      code to apply
            - dict,     dictionary with parameters necessary for the function.
    verbose : int, optional
        Verbosity level for logging. If > 1, prints information about applied
        transformations. Default is 0 (no verbose output).

        The possible subfunctions are contained within this function for
        coherence and repeatability.

    Returns
    -------
    pa.Array
        array with the visit_concept_id for table_raw.
    """

    def single_code(
        _table: pa.Table, array: np.ndarray, code: int  # pylint: disable=W0613
    ) -> np.ndarray:
        """This file only has one visit type,
        so we assign the same code to every row."""
        # Get the index of every 0 in array
        idx = array == 0
        # Assign code to every True
        return np.where(idx, code, array)

    def duration_code(
        table: pa.Table, array: np.ndarray, code: int, time_lims: list[int, int]
    ) -> np.ndarray:
        """This file codes depend on the interval between start_date
        and end_date, ie the duration of the appointment. The arguments
        relate to the timespan in days that the interval has to be to
        apply the code."""
        # Compute the interval
        interval = pc.days_between(  # pylint: disable=E1101
            table["start_date"], table["end_date"]
        ).to_numpy(zero_copy_only=False)
        # Get the bool index
        idx = (interval >= time_lims[0]) & (interval <= time_lims[1])
        # Assign code to every True
        return np.where(idx, code, array)

    def field_code(
        table: pa.Table, array: np.ndarray, code: int, colname: str, colvalue: Any
    ) -> np.ndarray:
        """This file codes depend on the values of a field in table.
        The arguments relate to the name of the column and the value
        that column has to have to apply the code."""
        # Get the bool index
        idx = pc.equal(table[colname], colvalue)  # pylint: disable=E1101
        # Assign code to every True
        return np.where(idx, code, array)

    # -- Parameters --------------------------------------------------------------------------
    func_dict = {
        "single_code": single_code,
        "duration_code": duration_code,
        "field_code": field_code,
    }

    # -- Function assignment ----------------------------------------------------------------
    # Create array of zeros (not defined concept by default)
    visit_concept_id = np.zeros(len(table_raw), dtype=np.int64)
    # Apply the codes
    for func_str, code, kwargs in functions:
        # Pass from string name to actual function
        func = func_dict[func_str]
        # Apply the function and the paramters
        if verbose > 1:
            print(f"- Applying {func.__name__}({code}, {kwargs})")
        visit_concept_id = func(table_raw, visit_concept_id, code, **kwargs)
    return visit_concept_id


def ad_hoc_read(data_dir: Path, filename: str, transformations: dict) -> pa.Table:
    """
    Read a Parquet file and apply custom transformations if needed.

    This function reads a Parquet file and checks if any specific transformations
    need to be applied based on the filename. If a transformation is defined for
    the file, it is applied before returning the data.

    Parameters
    ----------
    data_dir : Path
        Common directory for the file.
    filename : str
        Path to the Parquet file to be read.
    transformations : dict
        A dictionary mapping filenames to tuples containing a transformation
        function and its arguments. The structure should be:
        {filename: [transformation_function, arg1, arg2, ...]}

    Returns
    -------
    pa.Table
        A PyArrow table containing the data, with any necessary transformations applied.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the transformation function is not callable.

    Examples
    --------
    >>> def remove_end_date(filename):
    ...     table = pq.read_table(filename)
    ...     table = table.drop('end_date')
    ...     return table.add_column(2, 'end_date', table['start_date'])
    >>> transformations = {'example.parquet': [remove_end_date,]}
    >>> result = ad_hoc_read('example.parquet', transformations)
    """
    if not os.path.exists(data_dir / filename):
        raise FileNotFoundError(f"The file {filename} does not exist.")

    func_dict = {"remove_end_date": remove_end_date}

    if not isinstance(transformations, dict):
        return parquet.read_table(data_dir / filename)

    elif filename in transformations:
        transform_func, *args = transformations[filename]
        if not callable(transform_func):
            transform_func = func_dict[transform_func]
            if not callable(transform_func):
                raise ValueError(
                    f"The transformation for {filename}" + "is not a callable function."
                )
        return transform_func(data_dir / filename, *args)

    else:
        return parquet.read_table(data_dir / filename)


def remove_end_date(filename: str) -> pa.Table:
    """
    Remove the end_date column and use start_date as the new end_date.

    This function is designed to handle files where the end_date is not relevant,
    treating the event as a single-day occurrence.

    Parameters
    ----------
    filename : str
        Path to the Parquet file to be processed.

    Returns
    -------
    pa.Table
        A PyArrow table with the end_date column removed and replaced by start_date.
    """
    table = parquet.read_table(filename)
    table = table.drop("end_date")
    return table.add_column(2, "end_date", table["start_date"])


def gather_tables(data_dir: Path, params: dict, verbose: int = 0) -> pa.Table:
    """Gather and process tables for creating the VISIT_OCCURRENCE table
    based on configuration.

    This function receives a dictionary from a YAML configuration file,
    processes specified input files, applies transformations, and assigns
    visit concept IDs to create a consolidated VISIT_OCCURRENCE table.

    Parameters
    ----------
    data_dir : Path
        Common directory for all files.
    params : dict
        dict containig the parameters.
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show file being processed
        - 2 Show an example of the first row being removed and
            the row that contains it.

    Returns
    -------
    pa.Table
        A PyArrow Table containing the processed and consolidated visit occurrence data.

    Raises
    ------
    KeyError
        If no visit_concept_id is assigned to a file in the configuration.

    Notes
    -----
    The configuration file must contain the following sections:
    - 'input_dir': Path to directory inside data_dir that contains all input_files paths.
    - 'input_files': List of input files to process.
    - 'visit_concept_dict': Dict mapping files to concept ID functions.
        Every file should have a visit_concept_dict logic assigned.
    """
    if verbose > 0:
        print("Gathering tables...")
    # Load configuration
    input_dir = params["input_dir"]
    input_files = params["input_files"]
    concept_id_functions = params["visit_concept_dict"]

    # Prepare possible parameters
    possible_labels = [
        "transformations",
        "provider_cols",
        "provider_table_path",
    ]

    for lbl in possible_labels:
        try:
            _ = params[lbl]
        except KeyError:
            params[lbl] = None
            print(f" {lbl} not found. Moving on...")

    processed_tables = []

    # Process each input file
    print("Processing:")
    for input_file in input_files:
        if verbose > 0:
            print(f"- File: {input_file}")

        # Read and transform the input table
        raw_table = ad_hoc_read(
            data_dir / input_dir, input_file, params["transformations"]
        )

        # Assign visit concept ID
        concept_id = get_visit_concept_id(raw_table, concept_id_functions[input_file])

        if concept_id is None:
            raise KeyError(f"No visit concept ID assigned to file: {input_file}")

        final_columns = [
            "person_id",
            "start_date",
            "end_date",
            "type_concept",
        ]
        tmp_schema = pa.schema(
            [
                ("person_id", pa.int64()),
                ("start_date", pa.timestamp("us")),
                ("end_date", pa.timestamp("us")),
                ("type_concept", pa.int64()),
            ]
        )

        # -- PROVIDER -------------------------------------------------
        if params["provider_table_path"]:
            # Read params
            provider_cols = params["provider_cols"]
            provider_table_path = params["provider_table_path"]
            # Read PROVIDER table
            provider_table = parquet.read_table(provider_table_path).to_pandas()
            provider_map = dict(
                zip(provider_table["provider_name"], provider_table["provider_id"])
            )
            # Generate the mapping
            try:
                # Retrieve provider values
                provider_id = raw_table.to_pandas()[provider_cols[input_file]]
                # normalize content
                provider_id = provider_id.apply(gen.normalize_text)
                # Apply mapping
                provider_id = provider_id.map(provider_map)
            except KeyError:
                # Create an array of nuls
                provider_id = gen.create_null_int_array(len(raw_table))
            # Append a new column with the provider_id
            raw_table = raw_table.append_column("provider_id", [provider_id])
            final_columns.append("provider_id")
            tmp_schema = tmp_schema.append(pa.field("provider_id", pa.int64()))

        # Select relevant columns and add visit_concept_id
        processed_table = raw_table.select(final_columns).append_column(
            "visit_concept_id", [concept_id]
        )
        tmp_schema = tmp_schema.append(pa.field("visit_concept_id", pa.int64()))

        processed_tables.append(processed_table)

    # Combine all processed tables
    processed_tables = pa.concat_tables(processed_tables)
    # Cast to force timestamp
    # It is quicker and keeps rows with hour information
    processed_tables = processed_tables.cast(tmp_schema)

    return processed_tables


def clean_tables(gathered_table: pa.Table, params: dict, verbose: int = 0) -> pa.Table:
    """
    Clean and process a table of medical visit records.

    This receives a dict with paramaters, validates visit concept IDs,
    converts them to a categorical type based on a specified order, and
    removes overlapping records.

    Parameters
    ----------
    gathered_table : pa.Table
        A PyArrow Table containing the raw visit records.
    params : dict
        dictionary with the parameters from the YAML configuration file.
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show number of iterations
        - 2 Show an example of the first row being removed and
            the row that contains it.
        Will be passes to remove_overlap. Check definition to see output.

    Returns
    -------
    pa.Table
        A PyArrow Table with cleaned and processed records.

    Notes
    -----
    The function expects the configuration file to contain a 'visit_occurrence'
    key with a 'visit_concept_order' subkey specifying the order of visit concepts.
    """
    if verbose > 0:
        print("Cleaning records...")
    # Load configuration
    visit_concept_order = params["visit_concept_order"]
    sorting_columns = params["remove_overlap"]["sorting_columns"]
    ascending_order = params["remove_overlap"]["ascending_order"]

    # Convert to dataframe
    df_raw = gathered_table.to_pandas()
    df_raw = df_raw.drop_duplicates()

    # Validate visit concept IDs
    unique_concept_ids = df_raw["visit_concept_id"].unique()
    missing_concepts = set(unique_concept_ids) - set(visit_concept_order)
    if missing_concepts:
        errs = ", ".join(map(str, missing_concepts))
        raise KeyError(f"visit_concept(s) {errs} are not in visit_concept_order")

    # Convert to categorical
    df_raw["visit_concept_id"] = pd.Categorical(
        df_raw["visit_concept_id"], categories=visit_concept_order, ordered=True
    )

    # -- Remove overlap
    df_done = gen.remove_overlap(
        df_raw, sorting_columns, ascending_order, verbose=verbose
    )

    # Convert back to PyArrow Table
    return pa.Table.from_pandas(df_done, preserve_index=False)


def format_to_omop(table: pa.Table, verbose: int = 0) -> pa.Table:
    """
    Format a PyArrow table to conform to the OMOP Common Data Model for visit occurrences.

    This function starts with a pyarrow table returned by clean_tables() and performs
    the following operations:
    1. Renames and reorders columns
    2. Formats dates to create date fields
    3. Creates a primary key (visit_occurrence_id)
    4. Fills in any missing columns required by the OMOP schema
    5. Reorders columns to match the OMOP schema
    6. Casts the table to the OMOP schema

    Parameters
    ----------
    table : pa.Table
        The input PyArrow table to be formatted.
    verbose : int, optional
        Verbosity level for logging, by default 0.
        - 0 No info
        - 1 Tell that function was called

    Returns
    -------
    pa.Table
        A PyArrow table formatted according to the OMOP VISIT_OCCURRENCE schema.
    """
    omop_schema = omop_schemas["VISIT_OCCURRENCE"]
    if verbose > 0:
        print("Formatting to OMOP...")

    # Rename columns
    table = gen.rename_table_columns(
        table,
        {
            "start_date": "visit_start_datetime",
            "end_date": "visit_end_datetime",
            "type_concept": "visit_type_concept_id",
        },
    )

    # Format dates to remove times
    visit_start_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["visit_start_datetime"], unit="day"
        ),
        pa.date32(),
    )
    visit_end_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["visit_end_datetime"], unit="day"
        ),
        pa.date32(),
    )
    table = table.add_column(1, "visit_start_date", visit_start_date)
    table = table.add_column(2, "visit_end_date", visit_end_date)

    # Create the primary key
    visit_occurrence_id = pa.array(range(len(table)))
    table = table.add_column(0, "visit_occurrence_id", visit_occurrence_id)

    # Fill all other columns required by the OMOP schema
    table = gen.fill_omop_table(table, omop_schema, verbose)

    # Reorder columns and cast to OMOP schema
    table = gen.reorder_omop_table(table, omop_schema)
    table = table.cast(omop_schema)

    return table
