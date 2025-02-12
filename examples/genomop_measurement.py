import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

try:
    from package.datasets import data_dir
except:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.measurement as mea
from bps_to_omop.omop_schemas import omop_schemas


def preprocess_files(
    params_data: dict, concept_df: pd.DataFrame, data_dir_: Path
) -> pd.DataFrame:
    """Preprocess all files to create an unique dataframe

    Parameters
    ----------
    params_data : dict
        dictionary with the parameters for the preprocessing
    concept_df : pd.DataFrame
        CONCEPT table
    data_dir_ : Path
        Path to the upstream location of the data files

    Returns
    -------
    pd.DataFrame
        Dataframe with all information joined together
    """

    print("Preprocessing files...")
    input_dir = params_data["input_dir"]
    input_files = params_data["input_files"]
    column_map = params_data["column_map"]
    vocabulary_config = params_data["vocabulary_config"]
    value_map = params_data["value_map"]

    df_complete = []
    for f in input_files:
        print(f" Processing {f}: ")
        tmp_df = pd.read_parquet(data_dir_ / input_dir / f)
        # assign new vocabulary column if needed
        if params_data.get("append_vocabulary", False):
            if params_data["append_vocabulary"].get(f, False):
                tmp_df["vocabulary_id"] = params_data["append_vocabulary"][f]
        # Apply renaming
        if column_map.get(f, False):
            tmp_df = tmp_df.rename(column_map[f], axis=1)
        # Perform the mapping
        tmp_df = gen.map_source_value(
            tmp_df,
            vocabulary_config[f],
            concept_df,
            "measurement_source_value",
            "vocabulary_id",
            "measurement_source_concept_id",
        )
        if value_map[f] == "numeric":
            tmp_df["value_as_number"] = pd.to_numeric(tmp_df["value_source_value"])
        elif value_map[f] == "concept":
            tmp_df = gen.map_source_value(
                tmp_df,
                vocabulary_config[f],
                concept_df,
                "value_source_value",
                "value_vocabulary_id",
                "value_source_concept_id",
            )
        # Add to final dataframe
        df_complete.append(tmp_df)

    # -- Finish off joint dataframe -----------------------------------
    return pd.concat(df_complete, axis=0)


def map_units(
    df: pd.DataFrame, clc_df: pd.DataFrame, concept_df: pd.DataFrame
) -> pd.DataFrame:
    """Create automatic mappings for measurement units based on CLC vocabulary.

    This function maps measurement units from source values to standardized units
    using CLC vocabulary mappings and concept relationships. It first maps units
    using a CLC vocabulary lookup, then applies UCUM and SNOMED standardization.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing measurement data with a 'measurement_source_value'
        column that needs unit mapping.
    clc_df : pd.DataFrame
        CLC vocabulary dataframe containing the mapping between source measurement
        values and their standardized CLC unit representations. Must contain
        'NombreConvCLC' and 'UnidadConv' columns.
    concept_df : pd.DataFrame
        Concept relationship dataframe used for mapping measurement units to
        standardized UCUM and SNOMED concepts.

    Returns
    -------
    pd.DataFrame
        The input dataframe with additional mapped unit columns:
        - 'unit_source_value': Mapped CLC standardized units
        - Additional unit mapping columns based on UCUM and SNOMED
        (specific columns depend on measurement_units mapping function)

    Notes
    -----
    The function performs unit mapping in two steps:
    1. Maps source measurement values to CLC standardized units using vocabulary lookup
    2. Further maps the units to UCUM and SNOMED using concept relationships

    See Also
    --------
    gen.create_vocabulary_mapping : Creates the mapping dictionary from CLC vocabulary
    mea.map_measurement_units : Maps units to UCUM and SNOMED
    """
    print("Mapping units...")
    # Retrieve unit_source_value from clc_df vocabulary
    map_dict = gen.create_vocabulary_mapping(
        df, clc_df, "measurement_source_value", "NombreConvCLC", "UnidadConv"
    )
    df["unit_source_value"] = df["measurement_source_value"].map(map_dict)
    # Map source_concept_id using UCUM and SNOMED
    return mea.map_measurement_units(df, concept_df)


def map_standard_concepts(
    df: pd.DataFrame, concept_rel_df: pd.DataFrame
) -> pd.DataFrame:
    """Maps source concepts to standard concepts for measurements, units and values.

    This function maps source concept IDs to their corresponding standard concept IDs
    for measurements, units, and values using concept relationships. It also handles
    special cases where certain concept IDs should be null based on value types.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing measurement data with the following columns:
        - measurement_source_concept_id: Source concept ID for measurements
        - unit_source_concept_id: Source concept ID for units
        - value_source_concept_id: Source concept ID for values
        - value_source_value: Original value string/number
        - unit_source_value: Original unit string
    concept_rel_df : pd.DataFrame
        Concept relationship dataframe containing mappings between source and
        standard concepts. Used to standardize concept IDs across the different
        measurement fields.

    Returns
    -------
    pd.DataFrame
        The input dataframe with additional mapped standard concept columns:
        - measurement_concept_id: Standardized measurement concept
        - unit_concept_id: Standardized unit concept (null for rows without units)
        - value_as_concept_id: Standardized value concept (null for numeric values)

    Notes
    -----
    The function applies special rules for certain fields:
    - value_as_concept_id is set to null for rows where value_source_value
        can be converted to a number
    - unit_concept_id is set to null for rows where unit_source_value is null
    See: https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement

    See Also
    --------
    gen.map_source_concept_id : Maps individual source concepts to standard concepts
    """
    print("Mapping to standard concepts...")
    df = gen.map_source_concept_id(
        df, concept_rel_df, "measurement_source_concept_id", "measurement_concept_id"
    )
    df = gen.map_source_concept_id(
        df, concept_rel_df, "unit_source_concept_id", "unit_concept_id"
    )
    df = gen.map_source_concept_id(
        df, concept_rel_df, "value_source_concept_id", "value_as_concept_id"
    )
    # -- check value_as_concept_id and unit_concept_id
    # These fields must be null if value is not a concept / is a number
    numeric_rows = ~pd.to_numeric(df["value_source_value"], errors="coerce").isna()
    df.loc[numeric_rows, "value_as_concept_id"] = np.nan
    non_unit_rows = df["unit_source_value"].isna()
    df.loc[non_unit_rows, "unit_concept_id"] = np.nan

    return df


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
        unmapped_values = gen.find_unmapped_values(
            df, f"{col}_source_value", f"{col}_concept_id"
        )
        # Apply mapping if needed
        if len(unmapped_values) > 0:
            print(f" No concept ID found for {col} source values: {[*unmapped_values]}")
            print("  Applying custom concepts...")
            df = gen.update_concept_mappings(
                df,
                f"{col}_source_value",
                f"{col}_source_concept_id",
                f"{col}_concept_id",
                params_data[f"unmapped_{col}"],
            )

            unmapped_values = gen.find_unmapped_values(
                df, f"{col}_source_value", f"{col}_concept_id"
            )

    return df


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
def create_measurement_table(df: pd.DataFrame, schema: dict) -> pa.Table:
    """Creates the MEASUREMENT table following the OMOP-CDM schema.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed dataframe with measurement data
    schemas : dict
        dict containing the schema information

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
    table = gen.fill_omop_table(table, schema)
    table = gen.reorder_omop_table(table, schema)
    table = table.cast(schema)

    return table


def process_measurement_table(params_file: str, data_dir_: Path = None) -> pa.Table:

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_data = ext.read_yaml_params(params_file)
    output_dir = params_data["output_dir"]
    vocab_dir = params_data["vocab_dir"]
    visit_dir = params_data["visit_dir"]

    os.makedirs(data_dir_ / output_dir, exist_ok=True)

    # -- Load vocabularies --------------------------------------------
    print("Loading vocabularies...")
    concept_df = pd.read_parquet(
        data_dir_ / vocab_dir / "CONCEPT.parquet"
    ).infer_objects()
    concept_rel_df = pd.read_parquet(
        data_dir_ / vocab_dir / "CONCEPT_RELATIONSHIP.parquet"
    ).infer_objects()
    # Load CLC database
    clc_df = pd.read_parquet(data_dir_ / vocab_dir / "CLC.parquet")

    # -- Load each file and prepare it --------------------------------
    df = preprocess_files(params_data, concept_df, data_dir_)

    # -- Map units ----------------------------------------------------
    df = map_units(df, clc_df, concept_df)

    # -- Map to standard concepts -------------------------------------
    df = map_standard_concepts(df, concept_rel_df)

    # -- Check for codes that were not mapped -------------------------
    test_list = ["measurement", "unit"]
    df = check_unmapped_values(df, params_data, test_list)

    # -- Retrieve visit_occurrence_id ---------------------------------
    df = retrieve_visit_occurrence_id(df, data_dir_ / visit_dir)

    # -- Standardize contents -----------------------------------------
    table = create_measurement_table(df, omop_schemas["MEASUREMENT"])

    # -- Save ---------------------------------------------------------
    print("Saving to parquet...")
    parquet.write_table(table, data_dir_ / output_dir / "MEASUREMENT.parquet")
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./hepapred/preomop/genomop_measurement_params.yaml",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Common path to all data files",
        default=data_dir,
    )
    args = parser.parse_args()
    process_measurement_table(args.parameters_file, args.data_dir)
