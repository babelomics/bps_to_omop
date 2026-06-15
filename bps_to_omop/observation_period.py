"""
This module contains neccesary functions to build the OBSERVATION_PERIOD
table of an OMOP-CDM instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#observation_period

http://omop-erd.surge.sh/omop_cdm/tables/OBSERVATION_PERIOD.html
"""

# %%
import os
from pathlib import Path

import polars as pl

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import format_to_omop


# %%
def group_dates(df: pl.DataFrame, n_days: int, verbose: int = 0) -> pl.DataFrame:
    """Groups rows of dates from the same person that are less
    than n_days apart, keeping only the first start_date and
    the last end_date, respectively.

    It will remove rows that are partially contained within
    the previous one.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame with at least four columns:
        ['person_id', 'start_date', 'end_date', 'type_concept'].
        Column names do not need to be the same but the order
        must be the same as here.
        This allows its use for different tables with columns
        that have the same purpose but different names.
    n_days : int
        Minimum number of days between consecutive rows to consider
        them as separate periods. Any interval under n_days will be
        merged. Any interval above n_days will be kept.
    verbose : int, optional
        Information output, default 0
        - 0 No info
        - 1 Show stage of processing

    Returns
    -------
    pl.DataFrame
        Copy of input DataFrame with grouped rows.
    """
    col_person, col_start, col_end, col_type = df.columns[:4]

    # == Preparation ==============================================
    if verbose > 0:
        print("Grouping dates:")
        print("- Sorting and preparing data...")

    df_sorted = df.sort(
        [col_person, col_start, col_end], descending=[False, False, True]
    )

    # == Detect period breaks =====================================
    if verbose > 0:
        print("- Looking up indexes...")

    threshold = pl.duration(days=n_days)

    df_flagged = (
        df_sorted.with_columns(
            # Gap between this row's start_date and the previous row's end_date,
            # within the same person.
            (pl.col(col_start) - pl.col(col_end).shift(1).over(col_person)).alias(
                "_gap"
            ),
        )
        .with_columns(
            # A new group starts when:
            #   - it is the first row for this person, OR
            #   - the gap to the previous row's end_date is >= n_days
            (
                pl.col("_gap").is_null()  # first row of each person
                | (pl.col("_gap") >= threshold)  # gap large enough → new period
            ).alias("_new_group")
        )
        .with_columns(
            # Assign a cumulative group ID so every contiguous block of rows
            # belonging to the same period shares the same integer label.
            pl.col("_new_group")
            .cum_sum()
            .alias("_group_id")
        )
    )

    # == Aggregate within groups ==================================
    if verbose > 0:
        print("- Retrieving rows and computing type_concept...")

    df_done = (
        df_flagged.group_by([col_person, "_group_id"], maintain_order=True)
        .agg(
            pl.col(col_start).first(),
            pl.col(col_end).last(),
            # mode: most frequent value; take the first if there's a tie
            pl.col(col_type).mode().first(),
        )
        .drop("_group_id")
        .sort([col_person, col_start])
    )

    if verbose > 0:
        print("- Done!")

    return df_done


def process_observation_period_table(
    data_dir: str | Path, params_obs: dict, verbose: int = 0
) -> None:
    """Build and save the OBSERVATION_PERIOD parquet tables.

    Starts with the VISIT_OCCURRENCE table, that must have been generated
    beforehand, and computes the start and end of the observation periods
    for each patient. An Observation Period is a lapse of time where records
    appear with less than n_days between them.

    Parameters
    ----------
    data_dir : str | Path
        Root directory where input data is located and output will be saved.
    params_obs : dict
        Configuration dictionary. Must contain:
        - 'output_dir' key specifying the subdirectory where parquet
        files will be written.
        - 'n_days' key with the number of days that must separate records
        to be considered as independent observation periods.
    verbose : int, optional
        Information output, by default 0
    """
    # TODO:
    # - Remove visits mentions
    # - Configure retrieving the visit table, error if it does not exist.
    # - configure applying the schema from pyarrow but in polars

    # -- Manage params ------------------------------------------------
    output_dir = params_obs["output_dir"]
    visit_occurrence_dir = params_obs["visit_occurrence_dir"]
    n_days = params_obs["n_days"]

    # Convert to Path
    data_dir = Path(data_dir)
    # Create directory
    os.makedirs(data_dir / output_dir, exist_ok=True)

    # -- Load VISIT_OCCURRENCE ----------------------------------------
    visit = pl.read_parquet(
        data_dir / visit_occurrence_dir / "VISIT_OCCURRENCE.parquet"
    )

    # -- Build OBSERVATION_PERIOD -------------------------------------
    table = visit.select(
        "person_id", "visit_start_date", "visit_end_date", "visit_type_concept_id"
    )

    # we group periods that are not more than n_days days apart
    table = group_dates(table, n_days, verbose=verbose)

    # Create unique id and rename columns
    table = (
        table.drop("observation_period_id")
        .with_row_index("observation_period_id")
        .rename(
            {
                "visit_start_date": "observation_period_start_date",
                "visit_end_date": "observation_period_end_date",
                "visit_type_concept_id": "period_type_concept_id",
            }
        )
    )

    observation_period = format_to_omop.format_table(
        table, omop_schemas["OBSERVATION_PERIOD"]
    )

    # -- Save to parquet ----------------------------------------------
    print("Saving... ", end="")
    observation_period.write_parquet(
        data_dir / output_dir / "OBSERVATION_PERIOD.parquet",
    )
    print("Done!")


# %%
