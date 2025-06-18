# 29/01/2025
#
# This file contains usual transformation to generate the MEASUREMENT
# table of an OMOP-CDM database instance.
#
# https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement
#
# http://omop-erd.surge.sh/omop_cdm/tables/MEASUREMENT.html

import numpy as np
import pandas as pd

from bps_to_omop import general as gen


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
    gen.map_source_value : Helper function that performs the actual mapping
    """
    result_df = df.copy()

    # Reduce size of concept_df
    concept_units = concept_df[
        (concept_df["domain_id"] == "Unit")
        & (concept_df["vocabulary_id"].isin(["UCUM", "SNOMED"]))
    ]

    # First attempt: Map to UCUM vocabulary
    result_df.loc[:, "unit_vocabulary_id"] = "UCUM"
    result_df = gen.map_source_value(
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
        result_df = gen.map_source_value(
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
