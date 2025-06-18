import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.measurement as mea
from bps_to_omop.omop_schemas import omop_schemas


def retrieve_visit_occurrence_id(df: pd.DataFrame, table_dir: Path) -> pd.DataFrame:
    """Retrieve the visit_occurrence_id foreign key fro the VISIT_OCCURRENCE table.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    visit_dir : Path
        Location of the VISIT_OCCURRENCE.parquet file.

    Returns
    -------
    pd.DataFrame
        Input dataframe with additional columns for visit_occurrence_id,
        visit_start_date and visit_end_date, if found.
    """
    print("Looking for visit_occurrence_id...")
    # Create the primary key
    df["measurement_id"] = pa.array(range(len(df)))

    # Look for visit_occurrence_id
    df_visit_occurrence = pd.read_parquet(table_dir / "VISIT_OCCURRENCE.parquet")
    df = gen.find_visit_occurence_id(
        df,
        ["person_id", "start_date", "measurement_id"],
        df_visit_occurrence,
        verbose=2,
    )
    return df


# %%
# == Final touches ====================================================
def create_measurement_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Creates the MEASUREMENT table following the OMOP-CDM schema.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed dataframe with measurement data
    schema : pa.Schema
        Schema information

    Returns
    -------
    pa.Table
        Table containing the MEASUREMENT table
    """
    print("Formatting to OMOP...")
    # Convert to pyarrow table, value_source_value is mixed dtype so we force str
    df["value_source_value"] = df["value_source_value"].astype(str)
    table = pa.Table.from_pandas(df, preserve_index=False)
    # Rename existing columns
    table = gen.rename_table_columns(
        table,
        {
            "start_date": "measurement_date",
            "type_concept": "measurement_type_concept_id",
        },
    )

    # Fill, reorder and cast to schema
    table = gen.format_table(table, schema)

    return table


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./src/genomop_measurement_params.yaml",
    )
    args = parser.parse_args()
    process_measurement_table(args.parameters_file)
