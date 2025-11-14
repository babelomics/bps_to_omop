"""
This module contains necessary transformations to generate the
table PROCEDURE_OCCURRENCE from an OMOP-CDM.

See:

https://ohdsi.github.io/CommonDataModel/cdm54.html#procedure_occurrence

http://omop-erd.surge.sh/omop_cdm/tables/PROCEDURE_OCCURRENCE.html
"""

from os import makedirs
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import common, format_to_omop, map_to_omop


def preprocess_files(
    params_data: dict, concept_df: pd.DataFrame, data_dir: Path
) -> pd.DataFrame:
    """Preprocess all files to create an unique dataframe

    Parameters
    ----------
    params_data : dict
        dictionary with the parameters for the preprocessing
    concept_df : pd.DataFrame
        CONCEPT table
    data_dir : Path
        Path to the upstream location of the data files

    Returns
    -------
    pd.DataFrame
        Dataframe with all information joined together
    """
    # == Load each file and prepare it =====================================
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
        # Perform the mapping from source to omop codes
        tmp_df = map_to_omop.map_source_value(
            tmp_df,
            vocabulary_config[f],
            concept_df,
            "procedure_source_value",
            "vocabulary_id",
            "procedure_source_concept_id",
        )
        # Add to final dataframe
        df_complete.append(tmp_df)

    # -- Finish off joint dataframe ---------------------------------------
    df_complete = pd.concat(df_complete, axis=0)

    # -- Make sure dates are correct ----------------------------------
    df_complete["start_date"] = pd.to_datetime(df_complete["start_date"])
    df_complete["end_date"] = pd.to_datetime(df_complete["end_date"])

    return df_complete


def map_standard_concepts(
    df: pd.DataFrame, concept_rel_df: pd.DataFrame
) -> pd.DataFrame:
    """Maps source concepts to standard concepts for procedures.

    This function is a wrapper for map_to_omop.map_source_concept_id applied to the
    PROCEDURE_OCCURRENCE table.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing procedures data with the following columns:
        - procedure_source_concept_id: Source concept ID for procedure
    concept_rel_df : pd.DataFrame
        Concept relationship dataframe containing mappings between source and
        standard concepts.

    Returns
    -------
    pd.DataFrame
        The input dataframe with additional mapped standard concept columns:
        - procedure_concept_id: Standard procedure concept

    See Also
    --------
    map_to_omop.map_source_concept_id : Maps individual source concepts to standard concepts
    """
    print("Mapping to standard concepts...")
    df = map_to_omop.map_source_concept_id(
        df, concept_rel_df, "procedure_source_concept_id", "procedure_concept_id"
    )

    return df


def retrieve_visit_occurrence_id(
    df: pd.DataFrame, visit_dir: Path, batch_size: int = 10000
) -> pd.DataFrame:
    """Retrieve the visit_occurrence_id foreign key from the VISIT_OCCURRENCE table.

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

    # -- Create the primary key
    df["procedure_occurrence_id"] = pa.array(range(len(df)))

    # -- Define the required columns
    required_df_columns = ["person_id", "start_date", "procedure_occurrence_id"]

    # -- Find visit_occurence_id
    print("Finding visit_occurrence_id...")
    df_visit_occurrence = pd.read_parquet(visit_dir / "VISIT_OCCURRENCE.parquet")

    return common.retrieve_visit_in_batches(
        df, required_df_columns, df_visit_occurrence, batch_size
    )


def create_procedure_occcurence_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Creates the PROCEDURE_OCCURRENCE table following the OMOP-CDM schema.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed dataframe with procedure data
    schema : pa.Schema
        Schema information

    Returns
    -------
    pa.Table
        Table containing the MEASUREMENT table
    """
    print("Formatting to OMOP...")

    # Remove duplicates
    df = df.drop_duplicates()

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Rename columns
    table = format_to_omop.rename_table_columns(
        table,
        {
            "start_date": "procedure_datetime",
            "end_date": "procedure_end_datetime",
            "type_concept": "procedure_type_concept_id",
        },
    )

    # Format dates to remove times
    start_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["procedure_datetime"], unit="day"
        ),
        pa.date32(),
    )
    end_date = pc.cast(
        pc.floor_temporal(  # pylint: disable=E1101
            table["procedure_end_datetime"], unit="day"
        ),
        pa.date32(),
    )
    table = table.add_column(1, "procedure_datetime", start_date)
    table = table.add_column(2, "procedure_end_datetime", end_date)

    # Fill, reorder and cast to schema
    table = format_to_omop.format_table(table, schema)

    return table


# %%
def process_procedure_occurrence_table(data_dir: Path, params_proc: dict):

    # -- Unwrap some params for clarity ------------------------------
    output_dir = params_proc["output_dir"]
    vocab_dir = params_proc["vocab_dir"]
    visit_dir = params_proc["visit_dir"]

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
    df = preprocess_files(params_proc, concept_df, data_dir)

    # -- Map to standard concepts -------------------------------------
    df = map_standard_concepts(df, concept_rel_df)

    # -- fallback mapping
    # If we find unmapped values, it is possible it's not ICD10, but ICD9, or otherwise.
    # We will retrieve unmapped values, and try to map to both

    # Define the fallback_vocabs
    fallback_vocabs = params_proc.get(
        "fallback_vocabs",
        False,  # Use this by default
    )

    if fallback_vocabs:
        df, unmapped_mask = map_to_omop.fallback_mapping(
            df,
            concept_df,
            concept_rel_df,
            fallback_vocabs,
            "procedure_source_value",
            "procedure_source_concept_id",
            "procedure_concept_id",
        )
    else:
        unmapped_mask = map_to_omop.get_unmapped_mask(df, "procedure_concept_id")

    # If we still have unmapped, report them
    if unmapped_mask.any():
        # Retrieve the unmapped values
        report_unmapped = map_to_omop.report_unmapped(
            df,
            df.loc[unmapped_mask, "procedure_source_value"].to_list(),
            "procedure_source_value",
            "procedure_source_concept_id",
            "procedure_concept_id",
        )
        # Save them for later reference
        report_unmapped.to_csv(
            data_dir / output_dir / "unmapped_procedures.csv",
            index=False,
        )

    # -- Retrieve visit_occurrence_id ---------------------------------
    df = retrieve_visit_occurrence_id(df, data_dir / visit_dir)

    # -- Standardize contents -----------------------------------------
    table = create_procedure_occcurence_table(df, omop_schemas["PROCEDURE_OCCURRENCE"])

    # -- Save to parquet ----------------------------------------------
    print("Saving to parquet...")
    parquet.write_table(table, data_dir / output_dir / "PROCEDURE_OCCURRENCE.parquet")
    print("Done.")
