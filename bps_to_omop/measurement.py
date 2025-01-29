# 29/01/2025
#
# This file contains usual transformation to generate the MEASUREMENT
# table of an OMOP-CDM database instance.
#
# https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement
#
# http://omop-erd.surge.sh/omop_cdm/tables/MEASUREMENT.html

import pandas as pd

from bps_to_omop import general as gen


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
