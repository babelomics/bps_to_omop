"""
This file contains usual transformations to generate the PROVIDER
table of an OMOP-CDM database instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement

http://omop-erd.surge.sh/omop_cdm/tables/MEASUREMENT.html
"""

from os import makedirs
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import common, format_to_omop, map_to_omop

def preprocess_files(data_dir: Path, params_data: dict) -> pd.DataFrame:
    """Preprocess all files to create an unique dataframe

    Parameters
    ----------
    data_dir : Path
        Path to the upstream location of the data files
    params_data : dict
        dictionary with the parameters for the preprocessing

    Returns
    -------
    pd.DataFrame
        Dataframe with all information joined together
    """

    print("Preprocessing files...")
    input_dir = params_data["input_dir"]
    input_files = params_data["input_files"]
    column_name_map = params_data["column_name_map"]
    column_values_map = params_data["column_values_map"]

    # == Load file and prepare it =====================================================================
    print("Preprocessing files...")
    provider = []
    for f in input_files:
        print(f" Processing {f}: ")
        tmp = pd.read_parquet(data_dir / input_dir / f)

        # Rename columns
        column_name_map = {**column_name_map[f]}
        tmp = tmp.rename(column_name_map, axis=1)

        # Keep only columns that belong in a PROVIDER table
        existing_cols = [col for col in omop_schemas["PROVIDER"].names if col in tmp.columns]
        tmp = tmp.loc[:,existing_cols]

        # Remove duplicates
        tmp = tmp.drop_duplicates()

        # Retrieve the concept_id using the supplied parameters
        map_dict = column_values_map[f]["specialty_source_value"]
        tmp["specialty_concept_id"] = tmp["specialty_source_value"].map(map_dict)

        # Append to table
        provider.append(tmp)

    # Create the table
    provider = pd.concat(provider)

    # Generate the provider_id
    provider["provider_id"] = pa.array(range(len(provider)))

    return provider


def check_unmapped_values(
    df: pd.DataFrame, params_data: dict, test_list: list
) -> pd.DataFrame:
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
        unmapped_values = map_to_omop.find_unmapped_values(
            df, f"{col}_source_value", f"{col}_concept_id"
        )
        # Apply mapping if needed
        if len(unmapped_values) > 0:
            print(f" No concept ID found for {col} source values: {[*unmapped_values]}")
            print("  Applying custom concepts...")
            df = map_to_omop.update_concept_mappings(
                df,
                f"{col}_source_value",
                f"{col}_concept_id",
                params_data[f"unmapped_{col}"],
            )

            unmapped_values = map_to_omop.find_unmapped_values(
                df, f"{col}_source_value", f"{col}_concept_id"
            )

    return df


def create_provider_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Creates the PROVIDER table following the OMOP-CDM schema.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed dataframe with provider data
    schema : pa.Schema
        Schema information

    Returns
    -------
    pa.Table
        Table containing the PROVIDER table
    """
    print("Formatting to OMOP...")
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Fill, reorder and cast to schema
    table = format_to_omop.format_table(table, schema)

    return table


# %%
def process_provider_table(data_dir: str, provider_params: dict) -> None:
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
    output_path = data_dir / provider_params["output_dir"]
    makedirs(output_path, exist_ok=True)

    # Load and preprocess input files
    raw_provider_data = preprocess_files(data_dir, provider_params)

    # Check for unmapped specialty codes
    unmapped_fields = ["specialty"]
    validated_provider_data = check_unmapped_values(
        raw_provider_data, provider_params, unmapped_fields
    )

    # Create standardized OMOP provider table
    provider_table = create_provider_table(
        validated_provider_data, omop_schemas["PROVIDER"]
    )

    # Save to parquet file
    output_file = output_path / "PROVIDER.parquet"
    print(f"Saving to {output_file}...")
    parquet.write_table(provider_table, output_file)
    print("Done.")
