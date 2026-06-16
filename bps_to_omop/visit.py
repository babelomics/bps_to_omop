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
import os
import warnings
from pathlib import Path
from typing import Any

import polars as pl
from joblib import Parallel, delayed
from tqdm import tqdm

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import format_to_omop, transform_table


# %%
def preprocess_files(params: dict, data_dir: Path, verbose: int = 0) -> pl.DataFrame:
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
    pl.DataFrame
        A polars DataFrame containing the processed and consolidated visit occurrence data.

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
        "source_to_provider_id",
        "provider_table_path",
    ]

    if verbose > 0:
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
        table = transform_table.apply_transformation(table, params, input_file)

        # -- Assign visit_concept_id ----------------------------------
        # Assign visit concept ID
        concept_id = get_visit_concept_id(table, concept_id_functions[input_file])
        # append visit_concept_id
        table = table.with_columns(concept_id.alias("visit_concept_id"))

        # -- PROVIDER -------------------------------------------------
        table = generate_provider_id(table, input_file, params, data_dir)

        # -- Append at end of loop ------------------------------------
        table = table.select(columns_schema.names()).cast(columns_schema)
        processed_tables.append(table)

    # -- Combine and return -------------------------------------------
    # Combine all processed tables
    processed_tables = pl.concat(processed_tables)

    return processed_tables


def generate_provider_id(
    table: pl.DataFrame,
    input_file: str,
    params: dict,
    data_dir: Path,
) -> pl.DataFrame:
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
        - "provider_table_path": path (relative to `data_dir`) of the
          Parquet file containing the provider reference table.
        - "source_to_provider_id": dict mapping filenames to a
          {source_col: provider_col} dict that defines which column in
          `table` links to which column in the provider reference table.
    data_dir : Path
        Path to the upstream location of the data files.

    Returns
    -------
    pl.DataFrame
        The original dataframe with the Int64 provider IDs.
        Rows with no match in the provider table will have a null value.
        If no provider mapping is configured for `input_file`, all values
        will be null.
    """

    source_to_provider_id = params.get("source_to_provider_id", {})
    if source_to_provider_id.get(input_file, False):
        # Read PROVIDER table
        provider_table = pl.read_parquet(data_dir / params["provider_table_path"])

        # Retrieve the col that links to the provider_id
        ((source_col, provider_col),) = source_to_provider_id[input_file].items()

        # Join to map source column to provider_id
        table_with_provider = table.join(
            provider_table.select([provider_col, "provider_id"]),
            left_on=source_col,
            right_on=provider_col,
            how="left",
        ).get_column("provider_id")

    else:
        table_with_provider = table.with_columns(pl.lit(None).alias("provider_id"))

    return table_with_provider


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


def build_visit_detail(df):
    return (
        # First we do the sorting
        df.sort(
            ["person_id", "start_date", "end_date", "type_concept"],
            descending=[False, False, True, False],
        )
        # Assign the visit_detail_id
        .with_columns(visit_detail_id=pl.int_range(pl.len()))
        # Rename columns
        .rename(
            {
                "start_date": "visit_detail_start_datetime",
                "end_date": "visit_detail_end_datetime",
                "type_concept": "visit_detail_type_concept_id",
            }
        )
    )


def build_visit_detail_extended(visit_detail):
    # Get the first date of every person
    visit_occurrence_dates = visit_detail.group_by(
        "person_id", maintain_order=True
    ).agg(
        visit_start_datetime=pl.col("visit_detail_start_datetime").first(),
        visit_end_datetime=pl.col("visit_detail_end_datetime").first(),
        visit_detail_id_original=pl.col("visit_detail_id").first(),
    )

    # Join and return
    return visit_detail.join(
        visit_occurrence_dates, on="person_id", how="left"
    ).with_columns(
        main_visit=pl.lit("Unknown").cast(pl.Enum(["Yes", "No", "Unknown"])),
        is_contained=pl.lit(False),
        is_partial=pl.lit(False),
        not_contained=pl.lit(False),
        parent_visit_detail_id=pl.lit(None),
    )


def identify_next_main_visits(df):
    return df.with_columns(
        # Set new possible main visit as "Yes"
        main_visit=(
            pl.when(
                (pl.col("main_visit") == "Unknown")
                & (pl.col("visit_detail_id_original") == pl.col("visit_detail_id"))
            )
            .then(pl.lit("Yes"))
            .otherwise(pl.col("main_visit"))
        )
    ).with_columns(
        # Reset flags
        is_contained=pl.when(pl.col("main_visit").is_in(["Unknown", "Yes"]))
        .then(pl.lit(False))
        .otherwise(pl.col("is_contained")),
        is_partial=pl.when(pl.col("main_visit").is_in(["Unknown", "Yes"]))
        .then(pl.lit(False))
        .otherwise(pl.col("is_partial")),
        not_contained=pl.when(pl.col("main_visit").is_in(["Unknown", "Yes"]))
        .then(pl.lit(False))
        .otherwise(pl.col("not_contained")),
    )


def identify_contained_rows(df):
    return df.with_columns(
        is_contained=pl.when(
            (pl.col("main_visit") == "Unknown")
            & (pl.col("visit_start_datetime") <= pl.col("visit_detail_start_datetime"))
            & (pl.col("visit_end_datetime") >= pl.col("visit_detail_end_datetime"))
        )
        .then(True)
        .otherwise(pl.col("is_contained"))
    )


def update_contained_rows(df):

    return df.with_columns(
        # Update main_visit to mark contained visits
        main_visit=(
            pl.when((pl.col("is_contained") == True))
            .then(pl.lit("No"))
            .otherwise(pl.col("main_visit"))
        ),
        # Build the parent_visit_detail_id, since we are here
        parent_visit_detail_id=(
            pl.when(
                (pl.col("is_contained") == True)
                & (pl.col("visit_detail_id") != pl.col("visit_detail_id_original"))
            )
            .then(pl.col("visit_detail_id_original"))
            .otherwise(pl.col("parent_visit_detail_id"))
            .cast(pl.Int32())
        ),
    )


def identify_partial_rows(df):
    return df.with_columns(
        is_partial=pl.when(
            (pl.col("main_visit") == "Unknown")
            & (pl.col("visit_detail_start_datetime") < pl.col("visit_end_datetime"))
            & (pl.col("visit_detail_end_datetime") > pl.col("visit_end_datetime"))
        )
        .then(True)
        .otherwise(pl.col("is_partial"))
    )


def update_partial_rows(df):

    latest_date = (
        df.filter(pl.col("is_partial") == True)
        .group_by(["person_id", "visit_detail_id_original"], maintain_order=True)
        .agg(latest_end_datetime=pl.col("visit_detail_end_datetime").max())
    )
    return (
        # Join back to the main dataframe and update visit_end_datetime
        df.join(
            latest_date,
            on=["person_id", "visit_detail_id_original"],
            how="left",
        )
        .with_columns(
            main_visit=(
                pl.when((pl.col("is_partial") == True))
                .then(pl.lit("No"))
                .otherwise(pl.col("main_visit"))
            ),
            visit_end_datetime=pl.when(
                pl.col("visit_end_datetime") != pl.col("latest_end_datetime")
            )
            .then(
                pl.coalesce(
                    [pl.col("latest_end_datetime"), pl.col("visit_detail_end_datetime")]
                )
            )
            .otherwise(pl.col("visit_end_datetime")),
        )
        .drop("latest_end_datetime")
    )


def identify_not_contained_rows(df):
    return df.with_columns(
        not_contained=pl.when(
            (pl.col("main_visit") == "Unknown")
            & (pl.col("visit_detail_start_datetime") >= pl.col("visit_end_datetime"))
        )
        .then(True)
        .otherwise(pl.col("not_contained"))
    )


def update_not_contained_rows(df):

    newest_not_contained = (
        df.filter(pl.col("not_contained") == True)
        .group_by("person_id", maintain_order=True)
        .agg(
            visit_detail_id_newest=pl.col("visit_detail_id").first(),
            visit_detail_start_datetime_newest=pl.col(
                "visit_detail_start_datetime"
            ).first(),
            visit_detail_end_datetime_newest=pl.col(
                "visit_detail_end_datetime"
            ).first(),
        )
    )

    return (
        # Join back to the main dataframe and update visit_end_datetime
        df.join(
            newest_not_contained,
            on="person_id",
            how="left",
        )
        # Update the values on visit_detail_id_original, visit_start_datetime and visit_end_datetime
        .with_columns(
            visit_detail_id_original=pl.when((pl.col("not_contained") == True))
            .then(pl.col("visit_detail_id_newest"))
            .otherwise(pl.col("visit_detail_id_original")),
            visit_start_datetime=pl.when((pl.col("not_contained") == True))
            .then(pl.col("visit_detail_start_datetime_newest"))
            .otherwise(pl.col("visit_start_datetime")),
            visit_end_datetime=pl.when((pl.col("not_contained") == True))
            .then(pl.col("visit_detail_end_datetime_newest"))
            .otherwise(pl.col("visit_end_datetime")),
        ).drop(
            pl.col(
                "visit_detail_id_newest",
                "visit_detail_start_datetime_newest",
                "visit_detail_end_datetime_newest",
            )
        )
    )


def assign_visit_occurrence_id(visit_occurrence):

    return (
        # Create a helper column to track main visit sequence
        visit_occurrence.with_columns(is_main_visit=(pl.col("main_visit") == "Yes"))
        # Assign a unique identifier only to main visits using row_number and clean the helper
        .with_columns(
            visit_occurrence_id=pl.when(pl.col("is_main_visit"))
            .then(pl.col("is_main_visit").cast(pl.Int32).cum_sum() - 1)
            .otherwise(None)
        ).drop("is_main_visit")
        # Fill the rest using forward fill (ffill)
        .with_columns(visit_occurrence_id=pl.col("visit_occurrence_id").forward_fill())
    )


def remap_visit_detail_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Remap visit_detail_id and parent_visit_detail_id to globally unique values.

    If IDs are assigned per-person during parallel processing, they are
    only unique within a person. This function builds a composite mapping on
    (person_id, old_visit_detail_id) -> new_global_id and applies it to both
    columns, preserving null parent_visit_detail_id values for main visits.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing person_id, visit_detail_id, and
        parent_visit_detail_id columns.

    Returns
    -------
    pl.DataFrame
        DataFrame with globally unique visit_detail_id and
        parent_visit_detail_id values.
    """
    # Build mapping: (person_id, old visit_detail_id) -> new global id
    id_mapping = (
        df.select("person_id", "visit_detail_id")
        .unique()
        .sort(["person_id", "visit_detail_id"])
        .with_columns(visit_detail_id_new=pl.int_range(pl.len()))
    )

    # Remap visit_detail_id
    df = (
        df.join(id_mapping, on=["person_id", "visit_detail_id"])
        .drop("visit_detail_id")
        .rename({"visit_detail_id_new": "visit_detail_id"})
    )

    # Remap parent_visit_detail_id using the same mapping
    df = (
        df.join(
            id_mapping.rename(
                {
                    "visit_detail_id": "parent_visit_detail_id",
                    "visit_detail_id_new": "parent_visit_detail_id_new",
                }
            ),
            on=["person_id", "parent_visit_detail_id"],
            how="left",
        )
        .drop("parent_visit_detail_id")
        .rename({"parent_visit_detail_id_new": "parent_visit_detail_id"})
    )

    return df


def build_visit_occurrence(df, verbose=0, n_iter_max=1000):
    # -- Initialization --
    # Get the core of the visit_detail table
    df = build_visit_detail(df)
    # Extend the table for processing
    df = build_visit_detail_extended(df)
    # Initialize counters for the while loop
    n_unknown = df.filter(pl.col("main_visit") == "Unknown").select(pl.len()).item()
    n_iter = 0

    # Look for next batch of main_visits
    df = identify_next_main_visits(df)

    # -- Loop --
    while n_unknown > 0 and n_iter < n_iter_max:
        if verbose > 0:
            print(f"Iter {n_iter:>2}: {n_unknown} unknown rows left.")

        # Identify and update completely contained visits
        df = identify_contained_rows(df)
        df = update_contained_rows(df)

        # Identify and update partially contained visits
        df = identify_partial_rows(df)
        df = update_partial_rows(df)

        # Identify and update not contained visits
        df = identify_not_contained_rows(df)
        df = update_not_contained_rows(df)

        if verbose > 1:
            print(df.filter(pl.col("main_visit") == "Unknown").head(5))

        # Look for next batch of main_visits
        df = identify_next_main_visits(df)

        # Update conditions
        n_unknown = (
            df.filter((pl.col("main_visit") == "Unknown")).select(pl.len()).item()
        )
        n_iter += 1

    if n_unknown > 0:
        warnings.warn(
            f"{n_unknown} rows still unresolved after {n_iter_max} iterations."
        )

    return df


def finalize_visit_tables(df):
    # Assign an unique visit_occurrence_id only to main_visits
    df = assign_visit_occurrence_id(df)

    # Remap visit_detail_id and parent_visit_detail_id to global values
    df = remap_visit_detail_ids(df)

    # Drop the extra helper columns
    df = df.drop(
        # Drop helpers
        pl.col("visit_detail_id_original"),
        pl.col("is_contained"),
        pl.col("is_partial"),
        pl.col("not_contained"),
    )

    # -- Build the core of the visit_detal table --
    # Drop visit_occurrence columns
    visit_detail = df.drop(
        pl.col("visit_start_datetime"),
        pl.col("visit_end_datetime"),
        pl.col(
            "main_visit"
        ),  # This one is dropped here so it can be used for visit_occurrence
    )

    # -- Build the core of the visit_occurrence table --
    # Get only main visits
    visit_occurrence = df.filter(pl.col("main_visit") == "Yes").drop(
        pl.col("main_visit")
    )

    # Rename columns
    visit_occurrence = visit_occurrence.rename(
        {
            "visit_detail_type_concept_id": "visit_type_concept_id",
        }
    )

    # Drop columns from visit_detail
    visit_occurrence = visit_occurrence.drop(
        pl.col("visit_detail_start_datetime"),
        pl.col("visit_detail_end_datetime"),
        pl.col("visit_detail_id"),
        pl.col("parent_visit_detail_id"),
    )

    return visit_detail, visit_occurrence


def _process_batch(batch_df: pl.DataFrame, n_iter_max: int) -> pl.DataFrame:
    """Process a batch of persons (multiple person_ids) in a single worker."""
    groups = [group for _, group in batch_df.group_by("person_id")]
    return pl.concat(
        [
            build_visit_occurrence(group, verbose=0, n_iter_max=n_iter_max)
            for group in groups
        ]
    )


def process_visit_table(
    data_dir: str | Path,
    params_visit: dict,
    n_jobs: int = -2,
    batch_size: int = 1000,
) -> None:
    """Build and save the VISIT_DETAIL and VISIT_OCCURRENCE parquet tables.

    Loads raw visit files, processes each person's visits in parallel, then
    finalizes and saves the resulting tables.

    Parameters
    ----------
    data_dir : str | Path
        Root directory where input data is located and output will be saved.
    params_visit : dict
        Configuration dictionary. Must contain an 'output_dir' key specifying
        the subdirectory where parquet files will be written.
    n_jobs : int, optional
        Number of parallel jobs for joblib. -1 uses all available cores,
        -2 leaves one core free. Default is -2.
    batch_size : int, optional
        Number of people in each processing batch. Default is 1000.
    """
    # -- Manage folders -----------------------------------------------
    output_dir = params_visit["output_dir"]

    # Convert to Path
    data_dir = Path(data_dir)
    # Create directory
    os.makedirs(data_dir / output_dir, exist_ok=True)

    # -- Print runtime configuration ----------------------------------
    polars_threads = pl.thread_pool_size()
    actual_n_jobs = os.cpu_count() if n_jobs == -1 else n_jobs
    print(
        f"Runtime configuration:\n"
        f"  n_jobs:          {actual_n_jobs} workers\n"
        f"  polars_threads:  {polars_threads} threads/worker\n"
        f"  total threads:   {actual_n_jobs * polars_threads}\n"
        f"  available cores: {os.cpu_count()}",
        flush=True,
    )

    # -- Load each file and prepare it --------------------------------
    table = preprocess_files(params_visit, data_dir, verbose=1)

    # -- Split into batches -------------------------------------------
    person_ids = table["person_id"].unique().to_list()
    batches = [
        table.filter(pl.col("person_id").is_in(person_ids[i : i + batch_size]))
        for i in range(0, len(person_ids), batch_size)
    ]

    print(
        f"Processing {len(person_ids)} persons in {len(batches)} batches of ~{batch_size}..."
    )
    results = Parallel(n_jobs=n_jobs)(
        delayed(_process_batch)(batch, n_iter_max=10000)
        for batch in tqdm(batches, desc="Processing batches", unit="batch")
    )

    # -- Reassemble and finalize --------------------------------------
    df = pl.concat(results)
    visit_detail, visit_occurrence = finalize_visit_tables(df)

    visit_detail = format_to_omop.format_table(table, omop_schemas["VISIT_DETAIL"])
    visit_occurrence = format_to_omop.format_table(
        table, omop_schemas["VISIT_OCCURRENCE"]
    )

    # -- Save to parquet ----------------------------------------------
    print("Saving... ", end="")
    visit_detail.write_parquet(
        data_dir / output_dir / "VISIT_DETAIL.parquet",
    )
    visit_occurrence.write_parquet(
        data_dir / output_dir / "VISIT_OCCURRENCE.parquet",
    )
    print("Done!")
