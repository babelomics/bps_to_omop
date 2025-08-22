"""
This module contains usual transformations to generate the MEASUREMENT
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

    print("Preprocessing files...")
    input_dir = params_data["input_dir"]
    input_files = params_data["input_files"]
    column_map = params_data["column_map"]
    vocabulary_config = params_data["vocabulary_config"]
    value_map = params_data["value_map"]

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
            "measurement_source_value",
            "vocabulary_id",
            "measurement_source_concept_id",
        )
        if value_map[f] == "numeric":
            try:
                tmp_df["value_as_number"] = pd.to_numeric(tmp_df["value_source_value"])
                # Assign concept columns as nan
                tmp_df["value_source_concept_id"] = np.nan
            except ValueError as e:
                raise ValueError(
                    f"Some values in {f} could not be converted to numeric. Check columns assigned to 'value_source_value' and preprocess if necessary."
                ) from e
        elif value_map[f] == "concept":
            tmp_df = map_to_omop.map_source_value(
                tmp_df,
                vocabulary_config[f],
                concept_df,
                "value_source_value",
                "value_vocabulary_id",
                "value_source_concept_id",
            )
            # Assign numeric columns as nan
            tmp_df["value_as_number"] = np.nan
        # Add to final dataframe
        df_complete.append(tmp_df)

    # -- Finish off joint dataframe -----------------------------------
    df_complete = pd.concat(df_complete, axis=0)

    # -- Make sure dates are correct ----------------------------------
    df_complete["start_date"] = pd.to_datetime(df_complete["start_date"])
    df_complete["end_date"] = pd.to_datetime(df_complete["end_date"])

    return df_complete


def map_units(
    df: pd.DataFrame, clc_df: pd.DataFrame, concept_df: pd.DataFrame
) -> pd.DataFrame:
    """Create automatic mappings for measurement units from CLC vocabulary using
    UCUM and SNOMED standardization.

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
    map_to_omop.create_vocabulary_mapping : Creates the mapping dictionary from CLC vocabulary
    mea.map_measurement_units : Maps units to UCUM and SNOMED
    """
    print("Mapping units...")
    # Retrieve unit_source_value from clc_df vocabulary
    map_dict = map_to_omop.create_vocabulary_mapping(
        df, clc_df, "measurement_source_value", "NombreConvCLC", "UnidadConv"
    )
    df["unit_source_value"] = df["measurement_source_value"].map(map_dict)
    # Map source_concept_id using UCUM and SNOMED
    return map_measurement_units(df, concept_df)


def map_measurement_units(df: pd.DataFrame, concept_df: pd.DataFrame) -> pd.DataFrame:
    """Maps source unit values to standardized vocabulary concepts using UCUM and SNOMED.

    This function attempts to map source unit values first using UCUM vocabulary.
    For any unmapped units, it then tries mapping using SNOMED vocabulary as a fallback.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing measurement data with column 'unit_source_value'
    unit_concepts_df : pd.DataFrame
        Reference DataFrame containing standardized unit concepts with columns
        'concept_code' and 'concept_name'

    Returns
    -------
    pd.DataFrame
        DataFrame with additional mapped columns:
        - unit_vocabulary_id: Vocabulary source ('UCUM' or 'SNOMED')
        - unit_source_concept_id: Mapped concept identifier

    Notes
    -----
    The mapping process uses the following strategy:
    1. Try mapping all units to UCUM using concept_code
    2. For any unmapped units, attempt SNOMED mapping using concept_name

    See Also
    --------
    map_to_omop.map_source_value : Helper function that performs the actual mapping
    """
    result_df = df.copy()

    # Reduce size of concept_df
    concept_units = concept_df[
        (concept_df["domain_id"] == "Unit")
        & (concept_df["vocabulary_id"].isin(["UCUM", "SNOMED"]))
    ]

    # First attempt: Map to UCUM vocabulary
    result_df.loc[:, "unit_vocabulary_id"] = "UCUM"
    result_df = map_to_omop.map_source_value(
        result_df,
        {"UCUM": "concept_code"},
        concept_units,
        source_column="unit_source_value",
        vocabulary_column="unit_vocabulary_id",
        concept_id_column="unit_source_concept_id",
    )

    # Second attempt: Map remaining unmapped units to SNOMED
    if result_df["unit_source_concept_id"].isna().any():
        result_df.loc[
            result_df["unit_source_concept_id"].isna(), "unit_vocabulary_id"
        ] = "SNOMED"
        result_df = map_to_omop.map_source_value(
            result_df,
            {"SNOMED": "concept_name", "UCUM": "concept_code"},
            concept_units,
            source_column="unit_source_value",
            vocabulary_column="unit_vocabulary_id",
            concept_id_column="unit_source_concept_id",
        )

    return result_df


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
    map_to_omop.map_source_concept_id : Maps individual source concepts to standard concepts
    """
    print("Mapping to standard concepts...")
    df = map_to_omop.map_source_concept_id(
        df, concept_rel_df, "measurement_source_concept_id", "measurement_concept_id"
    )
    df = map_to_omop.map_source_concept_id(
        df, concept_rel_df, "unit_source_concept_id", "unit_concept_id"
    )
    df = map_to_omop.map_source_concept_id(
        df, concept_rel_df, "value_source_concept_id", "value_as_concept_id"
    )
    # -- check value_as_concept_id and unit_concept_id
    # These fields must be null if value is not a concept / is a number
    numeric_rows = ~pd.to_numeric(df["value_source_value"], errors="coerce").isna()
    df.loc[numeric_rows, "value_as_concept_id"] = np.nan
    df.loc[~numeric_rows, "unit_concept_id"] = np.nan

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


def retrieve_visit_occurrence_id(
    df: pd.DataFrame, visit_dir: Path, batch_size: int = 10000
) -> pd.DataFrame:
    """Retrieve the visit_occurrence_id foreign key from the VISIT_OCCURRENCE table.

    Before generating visit_occurrence_id, it creates the measurement_id field.

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
    df["measurement_id"] = pa.array(range(len(df)))

    # -- Define the required columns
    required_df_columns = ["person_id", "start_date", "measurement_id"]

    # -- Get for visit_occurrence table
    df_visit_occurrence = pd.read_parquet(visit_dir / "VISIT_OCCURRENCE.parquet")

    # Retrieve the visits_occurence_id matches in batches
    return common.retrieve_visit_in_batches(
        df, required_df_columns, df_visit_occurrence, batch_size
    )


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
    table = format_to_omop.rename_table_columns(
        table,
        {
            "start_date": "measurement_date",
            "type_concept": "measurement_type_concept_id",
        },
    )

    # Fill, reorder and cast to schema
    table = format_to_omop.format_table(table, schema)

    return table


def process_measurement_table(data_dir: Path, params_measurement: dict):

    # -- Unwrap some params for clarity ------------------------------
    output_dir = params_measurement["output_dir"]
    vocab_dir = params_measurement["vocab_dir"]
    visit_dir = params_measurement["visit_dir"]

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
    # Load CLC database
    clc_df = pd.read_parquet(data_dir / vocab_dir / "CLC.parquet")

    # -- Load each file and prepare it --------------------------------
    df = preprocess_files(params_measurement, concept_df, data_dir)

    # -- Map units ----------------------------------------------------
    df = map_units(df, clc_df, concept_df)

    # -- Map to standard concepts -------------------------------------
    df = map_standard_concepts(df, concept_rel_df)

    # -- Check for codes that were not mapped -------------------------
    test_list = ["measurement", "unit"]
    df = check_unmapped_values(df, params_measurement, test_list)

    # -- Retrieve visit_occurrence_id ---------------------------------
    df = retrieve_visit_occurrence_id(df, data_dir / visit_dir)

    # -- Standardize contents -----------------------------------------
    table = create_measurement_table(df, omop_schemas["MEASUREMENT"])

    # -- Save ---------------------------------------------------------
    print("Saving to parquet...")
    parquet.write_table(table, data_dir / output_dir / "MEASUREMENT.parquet")
    print("Done.")
