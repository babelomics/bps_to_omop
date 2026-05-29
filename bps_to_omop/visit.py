"""
This module contains neccesary functions to build the VISIT_OCCURRENCE
and VISIT_DETAIL tables of an OMOP-CDM instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_detail

http://omop-erd.surge.sh/omop_cdm/tables/VISIT_OCCURRENCE.html
http://omop-erd.surge.sh/omop_cdm/tables/VISIT_DETAIL.html
"""

# %%
from os import makedirs
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
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


# %%
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

    # Prepare optional parameters
    optional_labels = [
        "transformations",
        "provider_params",
        "col_to_provider_id",
    ]

    for lbl in optional_labels:
        lbl_params = params.get(lbl, {})
        if lbl_params:
            print(f" {lbl}:")
            for k, v in lbl_params.items():
                print(f"  - {k}: {v}")
        else:
            print(f" {lbl} not found. Moving on...")

    # -- Define the initial schema ------------------------------------
    # We force cast to force timestamp because it is quicker and keeps
    # rows with hour information
    columns_schema = pl.Schema(
        {
            "person_id": pl.Int64,
            "start_date": pl.Datetime("us"),
            "end_date": pl.Datetime("us"),
            "type_concept": pl.Int64,
            "visit_concept_id": pl.Int64,
            "provider_id": pl.Int64,
        }
    )

    # -- Loop through files -------------------------------------------
    processed_tables = []

    # Process each input file
    print("Processing:")
    for input_file in input_files:
        if verbose > 0:
            print(f"- File: {input_file}")

        # Read and transform the input table
        table = pl.read_parquet(data_dir / input_dir / input_file)
        # table = transform_table.apply_transformation(table, params, input_file)

        # -- Assign visit_concept_id ----------------------------------
        # Assign visit concept ID
        concept_id = get_visit_concept_id(table, concept_id_functions[input_file])
        # append visit_concept_id
        table = table.with_columns(concept_id.alias("visit_concept_id"))

        # TODO: fix this check
        if concept_id is None:
            raise KeyError(f"No visit concept ID assigned to file: {input_file}")

        # -- PROVIDER -------------------------------------------------
        provider_id = generate_provider_id(table, input_file, params, data_dir)
        # Append a new column with the provider_id
        table = table.append_column("provider_id", [provider_id])

        # -- Append at end of loop ------------------------------------
        table = table.select(columns_schema.names).cast(columns_schema)
        processed_tables.append(table)

    # -- Combine and return -------------------------------------------
    # Combine all processed tables
    processed_tables = pa.concat_tables(processed_tables)

    return processed_tables


def generate_provider_id(
    table: pl.DataFrame,
    input_file: str,
    params: dict,
    data_dir: Path,
) -> pl.Series:
    """Generate a provider_id Series for the given table by mapping a source
    column to provider IDs via a reference provider table. If no provider
    mapping is configured for the given file, returns a null integer Series.

    Parameters
    ----------
    table : pl.DataFrame
        Table currently being processed.
    input_file : str
        Filename of the table to be processed. Used to look up provider
        mapping configuration in `params`.
    params : dict
        Dictionary with preprocessing parameters. Expected keys:
        - "provider_params": dict mapping filenames to a truthy value when
          a provider mapping should be applied.
        - "provider_table_path": path (relative to `data_dir`) of the
          Parquet file containing the provider reference table.
        - "source_to_provider_id": dict mapping filenames to a
          {source_col: provider_col} dict that defines which column in
          `table` links to which column in the provider reference table.
    data_dir : Path
        Path to the upstream location of the data files.

    Returns
    -------
    pl.Series
        A Series of Int64 provider IDs aligned to the rows of `table`.
        Rows with no match in the provider table will have a null value.
        If no provider mapping is configured for `input_file`, all values
        will be null.
    """

    params_provider = params.get("provider_params", {})
    if params_provider.get(input_file, False):
        # Read PROVIDER table
        provider_table = pl.read_parquet(data_dir / params["provider_table_path"])

        # Retrieve the col that links to the provider_id
        ((source_col, provider_col),) = params["source_to_provider_id"][
            input_file
        ].items()

        # Join to map source column to provider_id
        provider_id = (
            table.select(pl.col(source_col))
            .join(
                provider_table.select([provider_col, "provider_id"]),
                left_on=source_col,
                right_on=provider_col,
                how="left",
            )
            .get_column("provider_id")
        )

    else:
        provider_id = pl.Series("provider_id", [None] * len(table), dtype=pl.Int64)

    return provider_id


def get_visit_concept_id(
    table_raw: pl.DataFrame, functions: list[dict], verbose: int = 0
) -> pl.Series:
    """Given a polars DataFrame and a list of functions, this function
    will apply the codification contained within the dict.

    Parameters
    ----------
    table_raw : pl.DataFrame
        polars DataFrame with at least person_id, start_date and end_date
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
    pl.Series
        series with the visit_concept_id for table_raw.
    """

    def single_code(
        _table: pl.DataFrame, array: pl.Series, code: int  # pylint: disable=W0613
    ) -> pl.Series:
        """This file only has one visit type,
        so we assign the same code to every row."""
        return pl.select(pl.when(array == 0).then(code).otherwise(array)).to_series()

    def duration_code(
        table: pl.DataFrame, array: pl.Series, code: int, time_lims: list[int, int]
    ) -> pl.Series:
        """This file codes depend on the interval between start_date
        and end_date, ie the duration of the appointment. The arguments
        relate to the timespan in days that the interval has to be to
        apply the code."""
        # Compute the interval using polars date_diff
        interval = (table["end_date"] - table["start_date"]).dt.total_days()
        # Get the bool index
        mask = (interval >= time_lims[0]) & (interval <= time_lims[1])
        # Assign code to every True
        return pl.select(pl.when(mask).then(code).otherwise(array)).to_series()

    def field_code(
        table: pl.DataFrame, array: pl.Series, code: int, colname: str, colvalue: Any
    ) -> pl.Series:
        """This file codes depend on the values of a field in table.
        The arguments relate to the name of the column and the value
        that column has to have to apply the code."""
        # Get the bool index
        mask = table[colname] == colvalue
        # Assign code to every True
        return pl.select(pl.when(mask).then(code).otherwise(array)).to_series()

    # -- Parameters --------------------------------------------------------------------------
    func_dict = {
        "single_code": single_code,
        "duration_code": duration_code,
        "field_code": field_code,
    }

    # -- Function assignment ----------------------------------------------------------------
    # Create array of zeros (not defined concept by default)
    visit_concept_id = pl.zeros(len(table_raw), dtype=pl.Int64, eager=True).alias(
        "visit_concept_id"
    )
    # Apply the codes
    for func_str, code, kwargs in functions:
        # Pass from string name to actual function
        func = func_dict[func_str]
        # Apply the function and the paramters
        if verbose > 1:
            print(f"- Applying {func.__name__}({code}, {kwargs})")
        visit_concept_id = func(table_raw, visit_concept_id, code, **kwargs)
    return visit_concept_id


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
    sorting_columns = ["person_id", "start_date", "end_date", "visit_concept_id"]
    ascending_order = [True, True, False, True]

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


def create_visit_occurrence_table(table: pa.Table, verbose: int = 0) -> pa.Table:
    """
    Format a PyArrow table to conform to the VISIT_OCCURRENCE table from thj OMOP Common Data Model.

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
        print("Formatting VISIT_OCCURRENCE to OMOP...")

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


def create_visit_detail_table(table: pa.Table, verbose: int = 0) -> pa.Table:
    """
    Format a PyArrow table to conform to the VISIT_DETAIL table from thj OMOP Common Data Model.

    This function starts with a pyarrow table returned by clean_tables() and performs
    the following operations:
    1. Renames and reorders columns
    2. Formats dates to create date fields
    3. Creates a primary key (visit_detail_id)
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
        A PyArrow table formatted according to the OMOP VISIT_DETAIL schema.
    """
    omop_schema = omop_schemas["VISIT_DETAIL"]
    if verbose > 0:
        print("Formatting VISIT_DETAIL to OMOP...")

    # Rename columns
    table = format_to_omop.rename_table_columns(
        table,
        {
            "start_date": "visit_detail_start_datetime",
            "end_date": "visit_detail_end_datetime",
            "type_concept": "visit_detail_type_concept_id",
        },
    )

    # Format dates to remove times
    visit_start_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["visit_detail_start_datetime"], unit="day"
        ),
        pa.date32(),
    )
    visit_end_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["visit_detail_end_datetime"], unit="day"
        ),
        pa.date32(),
    )
    table = table.add_column(1, "visit_detail_start_date", visit_start_date)
    table = table.add_column(2, "visit_detail_end_date", visit_end_date)

    # Create the primary key
    visit_occurrence_id = pa.array(range(len(table)))
    table = table.add_column(0, "visit_detail_id", visit_occurrence_id)

    # Fill all other columns required by the OMOP schema
    table = format_to_omop.format_table(table, omop_schema)

    return table


def process_visit_table(data_dir: str | Path, params_visit: dict):

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    output_dir = params_visit["output_dir"]

    # Convert to Path
    data_dir = Path(data_dir)
    # Create directory
    makedirs(data_dir / output_dir, exist_ok=True)

    # -- Load each file and prepare it --------------------------------
    table = preprocess_files(params_visit, data_dir, verbose=1)

    # The preprocessed table is basically the VISIT_DETAIL table
    visit_detail = table

    # == Apply functions ==============================================
    table = clean_tables(table, params_visit, verbose=2)

    # Cast the tables to the omop schemas
    visit_detail = create_visit_detail_table(visit_detail, verbose=1)
    visit_occurrence = create_visit_occurrence_table(visit_occurrence, verbose=1)

    # == Save to parquet ==============================================
    print("Saving... ", end="")
    parquet.write_table(
        visit_detail,
        data_dir / output_dir / "VISIT_DETAIL.parquet",
    )
    parquet.write_table(
        visit_occurrence,
        data_dir / output_dir / "VISIT_OCCURRENCE.parquet",
    )
    print("Done!")
