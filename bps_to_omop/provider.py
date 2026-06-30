"""
This file contains usual transformations to generate the PROVIDER
table of an OMOP-CDM database instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#provider

http://omop-erd.surge.sh/omop_cdm/tables/PROVIDER.html
"""

from os import makedirs
from pathlib import Path

import polars as pl
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import common, format_to_omop, map_to_omop


def preprocess_files(data_dir: Path, params_provider: dict) -> pd.DataFrame:
    """Preprocess all files to create an unique dataframe

    Parameters
    ----------
    data_dir : Path
        Path to the upstream location of the data files
    params_provider : dict
        dictionary with the parameters for the preprocessing

    Returns
    -------
    pd.DataFrame
        Dataframe with all information joined together
    """

    input_dir = params_provider["input_dir"]
    input_files = params_provider["input_files"]
    column_name_map = params_provider.get("column_name_map", {}) or {}
    column_values_map = params_provider.get("column_values_map", {}) or {}
    constant_values = params_provider.get("constant_values", {}) or {}

    # == Load file and prepare it =====================================================================
    print("Preprocessing files...")
    provider = []
    for f in input_files:
        print(f" Processing {f}: ")
        tmp_df = pl.read_parquet(data_dir / input_dir / f)

        # -- Rename columns -------------------------------------------
        # First ensure we have a dict with the relevant info
        tmp_colmap = column_name_map.get(f, {})

        # Ensure there is at least a column that was mapped to specialty_source_value
        assert (
            "specialty_source_value" in tmp_colmap.values()
        ), f"File {f} has no map to location_id"

        # Apply changes
        tmp_df = tmp_df.rename(tmp_colmap)

        # Keep only columns that belong in a PROVIDER table
        provider_cols = [
            col for col in omop_schemas["PROVIDER"].names if col in tmp_df.columns
        ]

        # Reduce size to essentials
        tmp_df = tmp_df.select(provider_cols).unique()

        # -- Apply values mapping -------------------------------------
        tmp_valmap = column_values_map.get(f, {})
        if tmp_valmap:
            # Loop over column_values to create all changes at once later
            expressions = []
            for source_column, mapping in tmp_valmap.items():
                # Create the new concept_id name
                concept_column = source_column.replace("_source_value", "_concept_id")

                # Create the expression to use later
                expressions.append(
                    pl.col(source_column)
                    .replace(mapping, default=None)
                    .alias(concept_column)
                )

        # -- Add Constant values --------------------------------------
        tmp_cteval = constant_values.get(f, {})
        if tmp_cteval:
            # Loop over constant_values to create all changes at once later
            expressions = []
            for col_name, col_value in tmp_cteval.items():
                # Create the expression to use later
                expressions.append(pl.lit(col_value).alias(col_name))
            # Create the new columns
            tmp_df = tmp_df.with_columns(expressions)

        # -- Format the table -----------------------------------------
        tmp_df = format_to_omop.format_table(tmp_df, omop_schemas["PROVIDER"])

        # Append to table
        provider.append(tmp_df)

    # Create the table
    provider = pl.concat(provider)

    # Remove duplicates between tables
    provider = provider.unique(subset="specialty_source_value")

    # Generate the provider_id
    provider = provider.drop("provider_id").with_row_index("provider_id")

    return provider


def check_unmapped_values(
    df: pl.DataFrame, params_data: dict, test_list: list
) -> pl.DataFrame:
    """Check and handle unmapped values in the groups of columns specified by
    test_list.

    Unmapped values will be remapped using the "unmapped_{col}" parameter in the
    params_data dict.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    params_data : dict
        dictionary with the parameters for the preprocessing
    test_list : list
        Header of the columns to be checked.

    Returns
    -------
    pd.DataFrame
        Input dataframe with unmapped values
    """

    for col in test_list:
        # Check for unmapped values
        unmapped_values = (
            df.filter(pl.col(f"{col}_source_concept_id").is_null())
            .get_column(f"{col}_source_value")
            .to_list()
        )
        # Apply mapping if needed
        if len(unmapped_values) > 0:
            preview = unmapped_values[:10]
            suffix = (
                f" ... and {len(unmapped_values) - 10} more"
                if len(unmapped_values) > 10
                else ""
            )
            print(f" No concept ID found for {col} source values: {preview}{suffix}")
            print("  Applying custom concepts...")

            map_dict = {str(k): v for k, v in params_data[f"unmapped_{col}"].items()}
            df = df.with_columns(
                pl.col(f"{col}_source_value")
                .replace(map_dict, default=None)
                .alias(f"{col}_source_concept_id")
            )

            # Check for unmapped values again
            unmapped_values = (
                df.filter(pl.col(f"{col}_source_concept_id").is_null())
                .get_column(f"{col}_source_value")
                .to_list()
            )
            preview = unmapped_values[:10]
            suffix = (
                f" ... and {len(unmapped_values) - 10} more"
                if len(unmapped_values) > 10
                else ""
            )
            print(f"   No concept ID found for {col} source values: {preview}{suffix}")
            print(f"   Consider adding custom concepts to unmapped_{col}. Moving on...")

    return df


# %%
def process_provider_table(data_dir: str | Path, provider_params: dict) -> None:
    """Process provider data files and generate OMOP PROVIDER table.

    Parameters
    ----------
    data_dir : str
        Directory containing input provider data files.
    provider_params : dict
        Configuration parameters containing 'output_dir' and processing settings.

    Returns
    -------
    None
        Saves PROVIDER.parquet file to the specified output directory.
    """

    # Ensure data_dir is a Path object
    data_dir = Path(data_dir)

    # Create output directory
    output_dir = data_dir / provider_params["output_dir"]
    makedirs(output_dir, exist_ok=True)

    # Load and preprocess input files
    provider = preprocess_files(data_dir, provider_params)

    # Check for unmapped specialty codes
    cols_prefix = ["specialty"]
    for col_prefix in cols_prefix:
        map_to_omop.report_unmapped(
            data_dir / output_dir, provider, col_prefix, extra_cols=["type_concept"]
        )

    # Create standardized OMOP provider table
    provider = format_to_omop.format_table(provider, omop_schemas["PROVIDER"])

    # Save to parquet file
    output_file = output_dir / "PROVIDER.parquet"
    print(f"Saving to {output_file}...")
    provider.write_parquet(output_file)
    print("Done.")
