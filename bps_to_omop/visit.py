"""
This module contains neccesary functions to build the VISIT_OCCURRENCE
and VISIT_DETAIL tables of an OMOP-CDM instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_detail

http://omop-erd.surge.sh/omop_cdm/tables/VISIT_OCCURRENCE.html
http://omop-erd.surge.sh/omop_cdm/tables/VISIT_DETAIL.html
"""

from os import makedirs
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import (
    common,
    format_to_omop,
    process_dates,
    pyarrow_utils,
    transform_table,
)


def preprocess_files(params: dict, data_dir: Path, verbose: int = 0) -> pa.Table:
    """Gather and preprocess tables for creating the VISIT_OCCURRENCE table
    based on configuration.

    Parameters
    ----------
    params : dict
        dict containig the parameters.
    data_dir : Path
        Path to the upstream location of the data files
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
    # -- Prepare parameters -------------------------------------------
    if verbose > 0:
        print("Gathering tables...")
    # Load configuration
    input_dir = params["input_dir"]
    input_files = params["input_files"]
    concept_id_functions = params["visit_concept_dict"]

    # Prepare possible parameters
    possible_labels = [
        "transformations",
        "provider_table_path",
        "col_to_provider_id",
    ]

    for lbl in possible_labels:
        try:
            _ = params[lbl]
        except KeyError:
            params[lbl] = None
            print(f" {lbl} not found. Moving on...")

    # -- Define some internal parameters ------------------------------
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

    # -- Loop through files -------------------------------------------
    processed_tables = []

    # Process each input file
    print("Processing:")
    for input_file in input_files:
        if verbose > 0:
            print(f"- File: {input_file}")

        # Read and transform the input table
        table = parquet.read_table(data_dir / input_dir / input_file)
        table = transform_table.apply_transformation(table, params, input_file)

        # -- Assign visit_concept_id ----------------------------------
        # Select relevant columns and add visit_concept_id
        processed_table = table.select(final_columns).append_column(
            "visit_concept_id", [concept_id]
        )
        tmp_schema = tmp_schema.append(pa.field("visit_concept_id", pa.int64()))
        # Assign visit concept ID
        concept_id = get_visit_concept_id(table, concept_id_functions[input_file])

        if concept_id is None:
            raise KeyError(f"No visit concept ID assigned to file: {input_file}")

        # -- PROVIDER -------------------------------------------------
        provider_id = generate_provider_id(table, input_file, params, data_dir)

        # Append a new column with the provider_id
        table = table.append_column("provider_id", [provider_id])
        final_columns.append("provider_id")
        tmp_schema = tmp_schema.append(pa.field("provider_id", pa.int64()))

        # -- Append at end of loop ------------------------------------
        processed_tables.append(processed_table)

    # -- Combine and return -------------------------------------------
    # Combine all processed tables
    processed_tables = pa.concat_tables(processed_tables)
    # Cast to force timestamp
    # It is quicker and keeps rows with hour information
    processed_tables = processed_tables.cast(tmp_schema)

    return processed_tables


def generate_provider_id(
    table: pa.Table,
    input_file: str,
    params: dict,
    data_dir: Path,
):
    """_summary_

    Parameters
    ----------
    table : pa.Table
        Table currently being processed
    input_file : str
        filename of the table to be processed
    params : dict
        dictionary with the parameters for the preprocessing
    data_dir : Path
        Path to the upstream location of the data files
    """

    params_provider = params.get("provider_params", {})
    if params_provider.get(input_file, False):
        # Read PROVIDER table
        provider_table = parquet.read_table(
            data_dir / params["provider_table_path"]
        ).to_pandas()

        # Retrieve the col that link to the provider_id
        ((source_col, provider_col),) = params["source_to_provider_id"][
            input_file
        ].items()

        # Build the dict that links current table to provider_id
        provider_map = dict(
            zip(provider_table[provider_col], provider_table["provider_id"])
        )

        # Retrieve provider values and apply mapping
        provider_id = table.to_pandas()[source_col].map(provider_map)

    else:
        provider_id = pyarrow_utils.create_null_int_array(len(table))

    return provider_id


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
        Will be passed to remove_overlap. Check definition to see output.

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
    df_done = process_dates.remove_overlap(
        df_raw, sorting_columns, ascending_order, verbose=verbose
    )

    # Convert back to PyArrow Table
    return pa.Table.from_pandas(df_done, preserve_index=False)


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


def to_omop(table: pa.Table, verbose: int = 0) -> pa.Table:
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
    table = format_to_omop.rename_table_columns(
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
    table = format_to_omop.format_table(table, omop_schema)

    return table


def process_visit_table(data_dir: str | Path, params_visit: dict):

    # -- Load parameters --------------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    output_dir = params_visit["output_dir"]

    # Convert to Path
    data_dir = Path(data_dir)
    # Create directory
    makedirs(data_dir / output_dir, exist_ok=True)

    # -- Load each file and prepare it --------------------------------
    df = preprocess_files(params_visit, data_dir, verbose=1)

    # == Apply functions ==================================================
    df = clean_tables(df, params_visit, verbose=2)
    df = to_omop(df, verbose=1)

    # == Save to parquet ==================================================
    print("Saving... ", end="")
    parquet.write_table(df, data_dir / output_dir / "VISIT_OCCURRENCE.parquet")
    print("Done!")
