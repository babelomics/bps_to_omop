"""
This module contains typical transformations to generate the DRUG_EXPOSURE
table of an OMOP-CDM database instance.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#drug_exposure

http://omop-erd.surge.sh/omop_cdm/tables/DRUG_EXPOSURE.html
"""

from os import makedirs
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import common, format_to_omop, map_to_omop


# %%
def preprocess_files(
    params_data: dict, concept_df: pd.DataFrame, data_dir: Path
) -> pd.DataFrame:
    """Preprocess all files to create an unique dataframe

    Parameters
    ----------
    params_data : dict
        dictionary with the parameters for the preprocessing
    concept_df : pd.DataFrame
        OMOP CONCEPT table
    data_dir : Path
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

    df_complete = []
    for f in input_files:
        print(f" Processing {f}: ")
        tmp_df = pd.read_parquet(data_dir / input_dir / f)
        # assign new vocabulary column if needed
        if params_data.get("append_vocabulary", False):
            if params_data["append_vocabulary"].get(f, False):
                tmp_df["vocabulary_id"] = params_data["append_vocabulary"][f]
        # Apply renaming
        if column_map.get(f, False):
            tmp_df = tmp_df.rename(column_map[f], axis=1)
        # Perform the mapping
        tmp_df = map_to_omop.map_source_value(
            tmp_df,
            vocabulary_config[f],
            concept_df,
            "drug_source_value",
            "vocabulary_id",
            "drug_source_concept_id",
        )
        # Add to final dataframe
        df_complete.append(tmp_df)

    # -- Finish off joint dataframe -----------------------------------
    df_complete = pd.concat(df_complete, axis=0)

    # -- Make sure dates are correct ----------------------------------
    df_complete["start_date"] = pd.to_datetime(df_complete["start_date"])
    df_complete["end_date"] = pd.to_datetime(df_complete["end_date"])

    return df_complete


# %%
def map_standard_concepts(
    df: pd.DataFrame, concept_rel_df: pd.DataFrame, verbose: int = 1
) -> pd.DataFrame:
    """Maps source concepts to standard concepts for drugs.

    This function maps source concept IDs to their corresponding standard concept IDs
    for drugs using concept relationships.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing drug_exposure data with the following column:
        - drug_source_concept_id: Source concept ID for drugs
    concept_rel_df : pd.DataFrame
        Concept relationship dataframe containing mappings between source and
        standard concepts.
    verbose : int, default 0
        Verbosity level.

    Returns
    -------
    pd.DataFrame
        The input dataframe with additional mapped standard concept columns:
        - drug_concept_id: Standardized drug concept

    See Also
    --------
    map_to_omop.map_source_concept_id : Maps individual source concepts to standard concepts
    """
    if verbose > 0:
        print("Mapping to standard concepts...")
    df = map_to_omop.map_source_concept_id(
        df, concept_rel_df, "drug_source_concept_id", "drug_concept_id"
    )

    return df


# %%
def check_unmapped_values(
    df: pd.DataFrame,
    params_data: dict,
    test_list: list,
    concept_df: pd.DataFrame,
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
    concept_df : pd.DataFrame
        OMOP CONCEPT table

    Returns
    -------
    pd.DataFrame
        Input dataframe with unmapped values
    """

    # Get a list of the standard codes
    std_codes = concept_df.loc[
        concept_df["standard_concept"] == "S", "concept_id"
    ].to_list()

    for col in test_list:
        # Check for unmapped values
        unmapped_values = map_to_omop.find_unmapped_values(
            df, f"{col}_source_value", f"{col}_concept_id"
        )
        # Apply mapping if needed
        if len(unmapped_values) > 0:
            print(f" No concept ID found for {col}_source_value: {[*unmapped_values]}")
            if params_data.get(f"unmapped_{col}", False):
                # Print that we are applying custom concepts and which ones
                print("  Applying custom concepts...")
                for k, v in params_data[f"unmapped_{col}"].items():
                    print(f"  - {k}: {v}")
                    # Check that the mapping are standard concept_id
                    assert v in std_codes, f"concept_id {v} is not standard"

                # Apply the update mappings to get the source_concept_id
                df = map_to_omop.update_concept_mappings(
                    df,
                    f"{col}_source_value",
                    f"{col}_concept_id",
                    params_data[f"unmapped_{col}"],
                )

            else:
                print("  No custom concepts provided. Moving on.")

    return df


# %%
def retrieve_visit_occurrence_id(
    df: pd.DataFrame, visit_dir: Path, batch_size: int = 10000
) -> pd.DataFrame:
    """Retrieve the visit_occurrence_id foreign key from the VISIT_OCCURRENCE table.

    Before generating visit_occurrence_id, it creates the drug_exposure_id field.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    visit_dir : Path
        Location of the VISIT_OCCURRENCE.parquet file.
    batch_size : int, default 10000
        Number of ppl to process

    Returns
    -------
    pd.DataFrame
        Input dataframe with additional columns for visit_occurrence_id,
        visit_start_date and visit_end_date, if found.
    """
    print("Looking for visit_occurrence_id...")
    # -- Create the primary key
    df["drug_exposure_id"] = pa.array(range(len(df)))

    # -- Define the required columns
    required_df_columns = ["person_id", "start_date", "drug_exposure_id"]

    # -- Get for visit_occurrence table
    df_visit_occurrence = pd.read_parquet(visit_dir / "VISIT_OCCURRENCE.parquet")

    # Retrieve the visits_occurence_id matches in batches
    return common.retrieve_visit_in_batches(
        df, required_df_columns, df_visit_occurrence, batch_size
    )


def create_drug_exposure_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Creates the DRUG_EXPOSURE table following the OMOP-CDM schema.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed dataframe with drug_exposure data
    schema : pa.Schema
        Schema information

    Returns
    -------
    pa.Table
        Table containing the DRUG_EXPOSURE table
    """
    print("Formatting to OMOP...")
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Rename columns
    table = format_to_omop.rename_table_columns(
        table,
        {
            "start_date": "drug_start_datetime",
            "end_date": "drug_end_datetime",
            "type_concept": "drug_type_concept_id",
        },
    )

    # Format dates to remove times
    start_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["drug_start_datetime"], unit="day"
        ),
        pa.date32(),
    )
    end_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["drug_end_datetime"], unit="day"
        ),
        pa.date32(),
    )
    table = table.add_column(1, "drug_start_date", start_date)
    table = table.add_column(2, "drug_end_date", end_date)

    # Fill, reorder and cast to schema
    table = format_to_omop.format_table(table, schema)

    return table


# %%
def process_drug_exposure_table(data_dir: Path, params_drug_exposure: dict):

    # -- Unwrap some params for clarity ------------------------------
    output_dir = params_drug_exposure["output_dir"]
    vocab_dir = params_drug_exposure["vocab_dir"]
    visit_dir = params_drug_exposure["visit_dir"]

    # Convert to Path
    data_dir = Path(data_dir)
    # Create directory
    makedirs(data_dir / output_dir, exist_ok=True)

    # -- Load vocabularies --------------------------------------------
    print("Loading vocabularies...")
    concept_df = pd.read_parquet(
        data_dir / vocab_dir / "CONCEPT.parquet"
    ).infer_objects()
    concept_rel_df = pd.read_parquet(
        data_dir / vocab_dir / "CONCEPT_RELATIONSHIP.parquet"
    ).infer_objects()

    # -- Load each file and prepare it --------------------------------
    df = preprocess_files(params_drug_exposure, concept_df, data_dir)

    # -- Map to standard concepts -------------------------------------
    df = map_standard_concepts(df, concept_rel_df)

    # -- Check for codes that were not mapped -------------------------
    test_list = ["drug"]
    df = check_unmapped_values(df, params_drug_exposure, test_list, concept_df)

    # -- Retrieve visit_occurrence_id ---------------------------------
    df = retrieve_visit_occurrence_id(df, data_dir / visit_dir)

    # -- Standardize contents -----------------------------------------
    table = create_drug_exposure_table(df, omop_schemas["DRUG_EXPOSURE"])

    # -- Save ---------------------------------------------------------
    print("Saving to parquet...")
    parquet.write_table(table, data_dir / output_dir / "DRUG_EXPOSURE.parquet")
    print("Done.")
