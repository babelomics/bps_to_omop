# 07/10/2024
#
# Este archivo agrupa las transformaciones necesarias para generar
# la tabla CONDITION_OCURRENCE en una instancia OMOP-CDM.
#
# https://ohdsi.github.io/CommonDataModel/cdm54.html#condition_occurrence
#
# http://omop-erd.surge.sh/omop_cdm/tables/CONDITION_OCCURRENCE.html
#

import os
from typing import Any, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

from bps_to_omop import extract as ext
from bps_to_omop import format_omop
from bps_to_omop import general as gen
from bps_to_omop.omop_schemas import omop_schemas


def gather_tables(config_file_path: str, verbose: int = 0) -> pa.Table:
    """Gather and process tables for creating the CONDITION_OCCURRENCE table
    based on configuration.

    This function reads a YAML configuration file, processes specified input files,
    and applies initial transformations to files to conform to a common pattern.

    Parameters
    ----------
    config_file_path : str
        Path to the YAML configuration file.
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show file being processed
        - 2 Show an example of the first row being removed and
            the row that contains it.

    Returns
    -------
    pa.Table
        A PyArrow Table containing the processed condition occurrence data.

    Raises
    ------
    KeyError
        If no visit_concept_id is assigned to a file in the configuration.

    Notes
    -----
    The configuration file should contain the following sections:
    - 'condition_occurrence.files_to_use': List of input files to process.
    - 'condition_occurrence.transformations': Dict of transformations to apply to files
        that need one.
    """
    if verbose > 0:
        print("Gathering tables...")
    # Load configuration
    config = ext.read_yaml_params(config_file_path)
    input_files = config["condition_occurrence"]["files_to_use"]
    read_transformations = config["condition_occurrence"]["read_transformations"]
    concept_id_functions = config["condition_occurrence"]["condition_concept_dict"]

    processed_tables = []

    # Process each input file
    for input_file in input_files:
        if verbose > 0:
            print(f" Processing file: {input_file}")

        base_name = os.path.basename(input_file)
        # Read and transform the input table
        table_raw = ad_hoc_read(
            input_file, read_transformations[base_name], verbose=verbose
        )

        # Assign visit concept ID
        concept_id = apply_concept_id(table_raw, concept_id_functions[base_name])

        if concept_id is None:
            raise KeyError(f"No visit concept ID assigned to file: {base_name}")

        # Select relevant columns and add visit_concept_id
        processed_table = table_raw.select(
            ["person_id", "start_date", "end_date", "type_concept"]
        ).add_column(3, "condition_concept_id", [concept_id])

        processed_tables.append(processed_table)

    # Combine all processed tables
    processed_tables = pa.concat_tables(processed_tables)
    # Force timestamp datatype
    # It is quicker and keeps rows with hour information
    processed_tables = processed_tables.cast(
        pa.schema(
            [
                ("person_id", pa.int64()),
                ("start_date", pa.timestamp("us")),
                ("end_date", pa.timestamp("us")),
                ("visit_concept_id", pa.int64()),
                ("type_concept", pa.int64()),
            ]
        )
    )
    return processed_tables


def ad_hoc_read(
    filename: str, read_transformations: list, verbose: int = 0
) -> pa.Table:
    """
    Read a Parquet file and apply custom transformations if needed.

    This function reads a Parquet file and checks if any specific transformations
    need to be applied based on the filename. If any transformations are defined for
    the file, they are applied in order before returning the data.

    Parameters
    ----------
    filename : str
        Path to the Parquet file to be read.
    transformations : list
        A list of transformations functions and its arguments. The structure should be:
        [
            [transformation_function_1, arg1_1, arg2_1, ...],
            [transformation_function_2, arg1_2, arg2_2, ...],
        ]

    Returns
    -------
    pa.Table
        A PyArrow table containing the data, with any necessary transformations applied.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.

    Examples
    --------
    >>> def remove_end_date(filename):
    ...     table = pq.read_table(filename)
    ...     table = table.drop('end_date')
    ...     return table.add_column(2, 'end_date', table['start_date'])
    >>> transformations = {'example.parquet': [rename_table_columns,]}
    >>> result = ad_hoc_read('example.parquet', transformations)
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f"The file {filename} does not exist.")

    read_transformations_dict = {
        "rename_table_columns": format_omop.rename_table_columns,
        "filter_table_by_column_value": filter_table_by_column_value,
    }

    table = parquet.read_table(filename)
    # Apply the codes
    for func_str, kwargs in read_transformations:
        # Pass from string name to actual function
        func = read_transformations_dict[func_str]
        # Apply the function and the paramters
        if verbose > 1:
            print(f" => Applying {func.__name__}({kwargs})")
        table = func(table, **kwargs)
    return table


def filter_table_by_column_value(
    table: pa.Table, filter_column: str, filter_value: Any
) -> pa.Table:
    """
    Filter a pyarrow table to retrieve rows matching a specific column value.

    This function receives a table and returns a new table containing only
    the rows where the specified column matches the given value. It's particularly
    useful for extracting relevant subsets from large datasets.

    Parameters
    ----------
    table : pa.Table
        Table to be processed.
    filter_column : str
        Name of the column to filter on. Must be an existing column in the
        Parquet file.
    filter_value : Any
        Value to filter by. Rows will be kept where the specified column
        equals this value. The type should match the column's data type.

    Returns
    -------
    pa.Table
        A PyArrow table containing only the rows where filter_column equals
        filter_value. The table structure (schema) remains unchanged.

    Raises
    ------
    FileNotFoundError
        If the specified parquet_path does not exist.
    KeyError
        If filter_column is not present in the Parquet file.
    TypeError
        If filter_value's type is incompatible with the column's data type.

    Examples
    --------
    >>> # Filter a users table to get only active users
    >>> active_users = filter_parquet_table_by_column(
    ...     parquet_path='users.parquet',
    ...     filter_column='status',
    ...     filter_value='active'
    ... )

    >>> # Filter a transactions table to get only successful transactions
    >>> successful_txns = filter_parquet_table_by_column(
    ...     parquet_path='transactions.parquet',
    ...     filter_column='status',
    ...     filter_value='success'
    ... )
    """
    # Create a boolean mask where the column matches the filter value
    matching_rows_mask = pc.equal(  # pylint: disable=E1101
        table[filter_column], filter_value
    )

    # Apply the mask to filter the table
    filtered_table = table.filter(matching_rows_mask)

    return filtered_table


def apply_concept_id(
    table_raw: pa.Table, functions: list, verbose: int = 0
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
        table: pa.Table, array: np.ndarray, code: int, colname: str, colvalue: str | int
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
            print(f"=> Applying {func.__name__}({code}, {kwargs})")
        visit_concept_id = func(table_raw, visit_concept_id, code, **kwargs)
    return visit_concept_id


def clean_tables(
    gathered_table: pa.Table, config_path: str, verbose: int = 0
) -> pa.Table:
    """
    Clean and process a table of medical visit records.

    This function loads a configuration file, validates visit concept IDs,
    converts them to a categorical type based on a specified order, and
    removes overlapping records.

    Parameters
    ----------
    gathered_table : pa.Table
        A PyArrow Table containing the raw visit records.
    config_path : str
        Path to the YAML configuration file.
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

    Raises
    ------
    KeyError
        If a visit_concept_id in the data is not present in the configuration.

    Notes
    -----
    The function expects the configuration file to contain a 'visit_occurrence'
    key with a 'visit_concept_order' subkey specifying the order of visit concepts.
    """
    if verbose > 0:
        print("Cleaning records...")
    # Load configuration
    config: Dict[str, Any] = ext.read_yaml_params(config_path)
    visit_concept_order = config["visit_occurrence"]["visit_concept_order"]

    # Convert to dataframe
    df_raw = gathered_table.to_pandas()

    # Validate visit concept IDs
    unique_concept_ids = df_raw["visit_concept_id"].unique()
    missing_concepts = set(unique_concept_ids) - set(visit_concept_order)
    if missing_concepts:
        raise KeyError(
            f"visit_concept(s) {', '.join(
            map(str, missing_concepts))} are not in visit_concept_order"
        )

    # Convert to categorical
    df_raw["visit_concept_id"] = pd.Categorical(
        df_raw["visit_concept_id"], categories=visit_concept_order, ordered=True
    )

    # Remove overlap
    df_done = gen.remove_overlap(df_raw, verbose=verbose)

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
    # Rename and reorder columns
    new_column_names = [
        "person_id",
        "visit_start_datetime",
        "visit_end_datetime",
        "visit_concept_id",
        "visit_type_concept_id",
    ]
    table = table.rename_columns(new_column_names)

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
    table = format_omop.fill_omop_table(table, omop_schema, verbose)

    # Reorder columns and cast to OMOP schema
    table = format_omop.reorder_omop_table(table, omop_schema)
    table = table.cast(omop_schema)

    return table
