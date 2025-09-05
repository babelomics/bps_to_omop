"""
Functions to help mapping concepts to and from an OMOP-CDM instance
"""

import numpy as np
import pandas as pd
import pyarrow as pa


def map_source_value(
    df: pd.DataFrame,
    target_vocab: dict,
    concept_df: pd.DataFrame,
    source_column: str = "source_value",
    vocabulary_column: str = "vocabulary_id",
    concept_id_column: str = "source_concept_id",
) -> pd.DataFrame:
    """Map source_value concepts to concept IDs across multiple vocabularies.

    This function takes two dataframes - one containing source concepts and another
    containing the OMOP CONCEPT table (concept_df) - and maps the source concepts to their
    corresponding concept IDs. Supports multiple vocabularies in the input data.

    It links either the 'concept_name' or 'concept_code' to the corresponding 'concept_id'.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing source concepts to be mapped.
        Must contain columns:
            - vocabulary_id : str
            - source_value : str
    target_vocab : dict
        Dictionary with target vocabularies as keys and their corresponding
        target column in CONCEPT table as values. Values can be:
            - 'concept_name' to map to concept names in concept_df table.
            - 'concept_code' to map to concept codes in concept_df table.
        Example:
            {'CLC':'concept_name', 'ICD10':'concept_code}
            Will map CLC vocabulary entries to the 'concept_name' column
            in the CONCEPT table and ICD10 vocabulary entries to the
            'concept_code' column in the CONCEPT table.
    concept_df : pd.DataFrame
        Reference DataFrame containing the concept mappings.
        Must contain columns:
            - vocabulary_id : str
            - concept_code : str
            - concept_name : str
            - concept_id : int
    source_column : str, optional, default "source_value"
        Name of the column that has the source values.
    vocabulary_column : str, optional, default "vocabulary_id"
        Name of the column that has the vocabulary_id values.
    concept_id_column : str, optional, default "source_concept_id"
        Name of the column that will be returned with the corresponding concept_id.


    Returns
    -------
    pd.DataFrame
        Input DataFrame with an additional column:
            - condition_source_concept_id : int
                Mapped concept IDs corresponding to source values

    Raises
    ------
    ValueError
        If no vocabulary is found
    KeyError
        If any source concepts are not found in the mapping

    Notes
    -----
    - The original DataFrame is not modified; a copy is returned
    - Update only the rows for each target_vocab at a time. This will
        rewrite existing mappings for the same vocabulary.
    """

    # Create a copy of the input DataFrame to store results
    result_df = df.copy()
    result_df[concept_id_column] = np.nan

    # Process each vocabulary
    for vocab, target in target_vocab.items():
        # Create masks for current vocabulary
        df_mask = df[vocabulary_column] == vocab
        concept_mask = concept_df["vocabulary_id"] == vocab

        # Get subset of data for current vocabulary
        df_subset = df[df_mask]
        concept_subset = concept_df[concept_mask]

        # Get unique concepts for current vocabulary
        unique_concepts = df_subset[source_column].unique()
        mapping_df = concept_subset[concept_subset[target].isin(unique_concepts)]

        # Create mapping for current vocabulary
        concept_map = dict(zip(mapping_df[target], mapping_df["concept_id"]))

        # Update only the rows for current vocabulary
        result_df.loc[df_mask, concept_id_column] = df_subset[source_column].map(
            concept_map
        )

    # Force correct datatypes
    result_df[concept_id_column] = result_df[concept_id_column].astype(pd.Int64Dtype())
    return result_df


def map_source_concept_id(
    df: pd.DataFrame,
    concept_rel_df: pd.DataFrame,
    source_column: str = "source_concept_id",
    concept_id_column: str = "concept_id",
) -> pd.DataFrame:
    """
    Maps source concepts to standard concepts using an OMOP CONCEPT_RELATIONSHIP DataFrame.

    This function identifies source concept IDs in the input DataFrame and maps them
    to their corresponding standard concepts using a 'Maps to' relationship from the
    provided CONCEPT_RELATIONSHIP table.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source concepts.
    concept_rel_df : pandas.DataFrame
        CONCEPT_RELATIONSHIP table DataFrame containing the mapping information.
        Must have columns: 'relationship_id', 'concept_id_1', 'concept_id_2'.
    source_column : str, optional, default "source_concept_id"
        Name of the column in df containing the source concept IDs to be mapped.
    concept_id_column : str, optional, default is "concept_id"
        Name of the output column that will contain the mapped standard concept IDs.

    Returns
    -------
    pandas.DataFrame
        A copy of the input DataFrame with an additional column (specified by target_column)
        containing the mapped standard concepts. Unmapped concepts will be set to 0.

    Notes
    -----
    - The function only considers 'Maps to' relationships from the concept_rel_df
    - Unmapped concepts will be set to 0 in the output
    - The original DataFrame is not modified; a copy is returned
    - All concept IDs are converted to Int64 type

    Examples
    --------
    >>> df = pd.DataFrame({'source_concept_id': [1, 2, 3]})
    >>> concept_rel_df = pd.DataFrame({
    ...     'relationship_id': ['Maps to', 'Maps to'],
    ...     'concept_id_1': [1, 2],
    ...     'concept_id_2': [100, 200]
    ... })
    >>> result = map_source_concept_id(df, concept_rel_df)
    >>> result['source_concept_id']
    0    100
    1    200
    2      0
    Name: source_concept_id, dtype: Int64
    """

    # Filter for 'Maps to' relationships
    mapping_relationships = concept_rel_df[
        concept_rel_df["relationship_id"] == "Maps to"
    ]

    # Create a copy of the input DataFrame to store results
    result_df = df.copy()

    # Get unique concepts
    unique_concepts = df[source_column].unique()
    mapping_df = mapping_relationships[
        mapping_relationships["concept_id_1"].isin(unique_concepts)
    ]

    # Create and apply mapping for current vocabulary
    concept_map = dict(zip(mapping_df["concept_id_1"], mapping_df["concept_id_2"]))

    # Update only the rows for current vocabulary
    result_df[concept_id_column] = result_df[source_column].map(concept_map)

    # Fill unmapped values (NaN) with 0
    result_df[concept_id_column] = result_df[concept_id_column].fillna(0)

    # Force correct datatypes
    result_df[source_column] = result_df[source_column].astype(pd.Int64Dtype())
    result_df[concept_id_column] = result_df[concept_id_column].astype(pd.Int64Dtype())

    return result_df


def create_vocabulary_mapping(
    df: pd.DataFrame,
    vocabulary_df: pd.DataFrame,
    source_column: str,
    vocab_code_column: str,
    vocab_value_column: str,
) -> dict:
    """
    Create a mapping dictionary between source codes and their corresponding vocabulary values.

    Parameters
    ----------
    df : pandas.DataFrame
        The source dataframe containing the codes to be mapped.
    vocabulary_df : pandas.DataFrame
        The vocabulary dataframe containing the mapping information.
    source_column : str
        The column name in source_df containing the codes to be mapped.
    vocab_code_column : str
        The column name in vocabulary_df containing the codes that match source_column.
    vocab_value_column : str
        The column name in vocabulary_df containing the values to map to.

    Returns
    -------
    dict
        A dictionary mapping codes from source_column to their corresponding values.
    """
    unique_codes = df[source_column].unique()

    filtered_vocab = vocabulary_df.loc[
        vocabulary_df[vocab_code_column].isin(unique_codes),
        [vocab_code_column, vocab_value_column],
    ].drop_duplicates()

    mapping_dict = filtered_vocab.set_index(vocab_code_column)[
        vocab_value_column
    ].to_dict()

    return mapping_dict


def apply_source_mapping(
    table: pa.Table, value_mappings: dict, output_columns: dict = None
) -> pa.Table:
    """
    Apply value mappings to specified columns in a PyArrow table and create new mapped columns.

    This function takes source value columns and creates corresponding concept_id columns
    based on provided mappings. For each source column, it creates a new column with '_concept_id'
    suffix containing the mapped values.

    Parameters
    ----------
    table : pa.Table
        Input PyArrow table containing the source value columns to be mapped
    value_mappings : dict
        Dictionary where keys are column names and values are mapping dictionaries.
        Each mapping dictionary maps source values to their corresponding concept IDs.
    output_columns : Optional, dict, default None
        Optional dictionary mapping source column names to desired output column names.
        If not provided or if a column is not in the dictionary, defaults to replacing
        '_source_value' with '_concept_id'.

    Returns
    -------
    pa.Table
        PyArrow table with additional columns containing the mapped values.
        New columns are named by replacing '_source_value' with '_concept_id' in
        the original column names.

    Examples
    --------
    >>> mappings = {
    ...     "gender_source_value": {"M": 8507, "F": 8532}
    ... }
    >>> table = pa.table({
    ...     "gender_source_value": ["M", "F", "M"]
    ... })
    >>> result = apply_source_mapping(table, mappings)
    >>> print(result.column_names)
    ['gender_source_value', 'gender_concept_id']
    """
    result_table = table

    for source_column, mapping in value_mappings.items():
        # Vectorize the mapping function with a default value of None
        vectorized_map = np.vectorize(lambda x: mapping.get(x, None))

        # Apply mapping and convert to PyArrow array
        mapped_values = pa.array(vectorized_map(table[source_column].to_numpy()))

        # Get output column name from custom mapping or use default
        if output_columns:
            concept_column = output_columns.get(
                source_column, source_column.replace("_source_value", "_concept_id")
            )
        else:
            concept_column = source_column.replace("_source_value", "_concept_id")

        # Append new column with mapped values
        result_table = result_table.append_column(concept_column, mapped_values)

    return result_table


def find_unmapped_values(
    df: pd.DataFrame, source_value_column: str, source_concept_id_column: str
) -> list:
    """
    Identify source values that have no concept ID mapping (NaN values).

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source values and concept IDs
    source_value_column : str
        Name of column containing original source values/codes
    source_concept_id_column : str
        Name of column containing existing concept ID mappings

    Returns
    -------
    list
        List of source values that have no concept ID mapping

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'source_code': ['A1', 'B2', 'C3'],
    ...     'source_concept_id': [123, np.nan, 789]
    ... })
    >>> unmapped = find_unmapped_values(df, 'source_code', 'source_concept_id')
    >>> print(unmapped)  # ['B2']
    """

    return (
        df[(df[source_concept_id_column] == 0) | (df[source_concept_id_column].isna())][
            source_value_column
        ]
        .drop_duplicates()
        .to_list()
    )


def get_unmapped_mask(df: pd.DataFrame, col: str) -> pd.Series:
    """Get boolean mask for unmapped values (null, NaN, 0, or empty)."""
    return df[col].isna() | (df[col] == 0) | (df[col] == "")


def fallback_mapping(
    df: pd.DataFrame,
    concept_df: pd.DataFrame,
    concept_rel_df: pd.DataFrame,
    fallback_vocabs: dict,
    source_value_column: str,
    source_concept_id_column: str,
    concept_id_column: str,
    vocabulary_id_column: str = "vocabulary_id",
) -> tuple:
    """
    Apply fallback vocabulary mappings to unmapped concept values.

    Iterates through fallback vocabularies to map unmapped rows by updating
    vocabulary_id and attempting source value and concept ID mappings.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing data to be mapped
    concept_df : pd.DataFrame
        Reference DataFrame with CONCEPT table
    concept_rel_df : pd.DataFrame
        DataFrame with CONCEPT_RELATIONSHIP for mapping
    fallback_vocabs : dict
        Dictionary mapping vocabulary names to target values.
        Example {"ICD10CM":"concept_code"}, maps ICD10CM via their concept_codes
    source_value_column : str
        Column name containing source values to map
        E.g.: "condition_source_value"
    source_concept_id_column : str
        Column name for source values concept IDs
        E.g.: "condition_source_concept_id"
    concept_id_column : str
        Column name for standard concept IDs
        E.g.: "condition_concept_id"
    vocabulary_id_column : str
        Column that holds the vocabulary_id of the source values.
        By default, "vocabulary_id

    Returns
    -------
    tuple[pd.DataFrame, pd.Series]
        Modified DataFrame and boolean mask of remaining unmapped rows
    """
    # Iterate over fallback_vocabs
    for vocab, target in fallback_vocabs.items():

        # Identify rows that need updating (null, NaN, 0 or empty values)
        unmapped_mask = (
            df[concept_id_column].isna()
            | (df[concept_id_column] == 0)
            | df[concept_id_column].isnull()
            | (df[concept_id_column] == "")
        )

        if unmapped_mask.any():

            print(
                f" {unmapped_mask.sum()} unmapped values found. Falling back to {vocab}:{target}",
                flush=True,
            )

            # Assign them to unmapped rows
            df.loc[unmapped_mask, vocabulary_id_column] = vocab

            # Try to map again to source_concept_id
            df = map_source_value(
                df,
                fallback_vocabs,
                concept_df,
                source_value_column,
                vocabulary_id_column,
                source_concept_id_column,
            )
            # Try to map to standard concept ids
            df = map_source_concept_id(
                df,
                concept_rel_df,
                source_concept_id_column,
                concept_id_column,
            )
        else:
            break
    else:
        # When loop finishes, reidentify rows that need updating
        unmapped_mask = (
            df[concept_id_column].isna()
            | (df[concept_id_column] == 0)
            | df[concept_id_column].isnull()
            | (df[concept_id_column] == "")
        )
        print(
            f" {unmapped_mask.sum()} values are still unmapped after fallback.",
            flush=True,
        )

    return df, unmapped_mask


def report_unmapped(
    df: pd.DataFrame,
    unmapped: list,
    source_value_column: str,
    source_concept_id_column: str,
    concept_id_column: str,
    extra_cols: tuple | list = ("vocabulary_id", "type_concept"),
) -> pd.DataFrame:
    """
    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source values and concept IDs
    unmapped: list
        List of unmapped source values. See find_unmapped_values().
    source_value_column : str
        Name of column containing original source values/codes
    source_concept_id_column : str
        Name of column containing existing concept ID mappings
    concept_id_column : str
        Name of column containing standard concept_ids.
    extra_cols : tuple | list
        Name of other columns to show. By default:
        ["vocabulary_id", "type_concept"]

    Returns
    -------
    pd.DataFrame
        Dataframe with the unmapped source_values

    Notes
    -------
    - For the mapping to work needs the correct code and the correct
    vocabulary.
    - Since we are mixing files, some of them might have the correct
    combination while others do not.
    - We retrieve the problematic unmapped codes to see if there are
    instances where the mapping was succesful.
    - This way we can investigate why.
    """
    cols = [
        source_value_column,
        source_concept_id_column,
        concept_id_column,
    ] + list(extra_cols)
    report_df = (
        df.loc[df[source_value_column].isin(unmapped), cols]
        .drop_duplicates()
        .sort_values(source_value_column)
    )
    print(
        f" {len(unmapped)} unmapped values found. Examples:\n",
        report_df.head(6),
        flush=True,
    )

    return report_df


def update_concept_mappings(
    df: pd.DataFrame,
    source_column: str,
    target_column: str,
    new_concept_mappings: dict,
) -> pd.DataFrame:
    """
    Update concept mappings in a DataFrame using provided new mappings.

    Only updates rows where the target column is null/NaN/0. Existing non-zero
    values in the target column are preserved.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing source values and concept IDs
    source_column : str
        Name of column containing original source values/codes
    target_column : str
        Name of column where updated concept IDs will be stored
    new_concept_mappings : dict
        Dictionary of {source_value: concept_id} pairs to update existing mappings

    Returns
    -------
    pd.DataFrame
        Copy of input DataFrame with updated concept ID mappings for unmapped values.
        Existing non-zero mappings are preserved.

    Raises
    ------
    KeyError
        If source_column or target_column don't exist in the DataFrame
    ValueError
        If DataFrame is empty or columns contain incompatible data types

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'source_code': ['A1', 'B2', 'C3'],
    ...     'concept_id': [123, 0, 789]
    ... })
    >>> new_mappings = {'B2': 456}
    >>> result = update_concept_mappings(df, 'source_code', 'concept_id', new_mappings)
    >>> result['concept_id'].tolist()
    [123, 456, 789]
    """
    # Input validation
    if df.empty:
        raise ValueError("DataFrame cannot be empty")

    if source_column not in df.columns:
        raise KeyError(f"Source column '{source_column}' not found in DataFrame")

    if target_column not in df.columns:
        raise KeyError(f"Target column '{target_column}' not found in DataFrame")

    if not new_concept_mappings:
        return df.copy()

    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Identify rows that need updating (null, NaN, or 0 values)
    unmapped_mask = (
        result_df[target_column].isna()
        | (result_df[target_column] == 0)
        | result_df[target_column].isnull()
        | (result_df[target_column] == "")
    )

    # Update only the unmapped rows
    for source_value, concept_id in new_concept_mappings.items():
        mask = (result_df[source_column] == source_value) & unmapped_mask
        result_df.loc[mask, target_column] = concept_id

    return result_df


def create_wide_relationship_table(
    concept_df: pd.DataFrame,
    concept_relationship_df: pd.DataFrame,
    concept_df_right: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Create a wide relationship table by joining CONCEPT and CONCEPT_RELATIONSHIP tables
    from the OMOP common data model.

    This function combines information from the CONCEPT and CONCEPT_RELATIONSHIP tables to
    create a single wide table that allows for easy consultation of relationships between
    medical concepts. It preserves all columns from the CONCEPT table, placing the unique
    columns from CONCEPT_RELATIONSHIP in the center to make consultations easier.
    Uses *_1 and *_2 suffixes to differentiate between left and right CONCEPT tables.

    Parameters
    ----------
    concept_df : pd.DataFrame
        The CONCEPT table from the OMOP CDM.
    concept_relationship_df : pd.DataFrame
        The CONCEPT_RELATIONSHIP table from the OMOP CDM.

    Returns
    -------
    pd.DataFrame
        A wide table containing concept relationships with all attributes from both
        related concepts.

    Notes
    -----
    This function focuses on concepts in the 'Condition' domain.

    The resulting DataFrame will have the following structure:
    - All columns from concept_df suffixed with '_1' for the left concept
    - 'concept_id_1', 'relationship_id', and 'concept_id_2' as 'ci_1','relationshio' and 'ci_2'
        from concept_relationship_df
    - All columns from concept_df suffixed with '_2' for the right concept
    """
    # Convert specified columns to category dtype for memory efficiency
    category_columns = [
        "domain_id",
        "vocabulary_id",
        "concept_class_id",
        "standard_concept",
    ]
    concept_df.loc[:, category_columns] = concept_df[category_columns].astype(
        "category"
    )
    concept_relationship_df["relationship_id"] = concept_relationship_df[
        "relationship_id"
    ].astype("category")

    # Prepare left table (concept 1)
    left_df = concept_df.add_suffix("_1")

    # Prepare central table (relationships)
    central_df = concept_relationship_df[
        ["concept_id_1", "relationship_id", "concept_id_2"]
    ].rename(
        columns={
            "concept_id_1": "ci_1",
            "relationship_id": "relationship",
            "concept_id_2": "ci_2",
        }
    )

    # Prepare right table (concept 2)
    try:
        right_df = concept_df_right.add_suffix("_2")
    except:
        right_df = concept_df.add_suffix("_2")

    # Join left and central tables
    merged_df = pd.merge(
        left_df, central_df, left_on="concept_id_1", right_on="ci_1", how="left"
    )

    # Join merged result with right table
    final_df = pd.merge(
        merged_df, right_df, left_on="ci_2", right_on="concept_id_2", how="left"
    )

    return final_df


def get_icd_codes(code_bps: str, bps_df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Retrieve ICD OMOP-compatible diagnosis codes for a given BPS (Base Poblacional de Salud) code.

    This function takes a BPS code and a DataFrame containing BPS to CIE (ICD) mappings,
    and returns a list of tuples with the corresponding CIE codes.

    Parameters
    ----------
    code_bps : str
        The BPS code to look up.
    bps_df : pd.DataFrame
        A DataFrame containing the mapping between BPS codes and CIE (ICD) codes.
        Expected columns: 'CODIGO_PATOLOGIA', 'COD_CIE_NORMALIZADO', 'TIPO_CIE'

    Returns
    -------
    list[tuple[str, str]]
        A list of tuples, each containing:
        - The normalized CIE (ICD) code
        - The corresponding OMOP vocabulary ID ('ICD10CM' or 'ICD9CM')

    Notes
    -----
    The function performs the following steps:
    1. Filters the input DataFrame for rows matching the given BPS code.
    2. Extracts the normalized CIE codes.
    3. Maps the Spanish CIE types to OMOP vocabulary IDs.
    4. Pairs each CIE code with its corresponding vocabulary ID.

    The mapping from Spanish to English vocabularies is as follows:
    - 'CIE10ES' -> 'ICD10CM'
    - 'CIE9MC' -> 'ICD9CM'
    """
    # Filter DataFrame for the given BPS code
    matched_rows = bps_df[bps_df["CODIGO_PATOLOGIA"] == code_bps]

    # Extract normalized CIE codes
    cie_codes = matched_rows["COD_CIE_NORMALIZADO"].tolist()

    # Map Spanish CIE types to OMOP vocabulary IDs
    vocabulary_mapping = {"CIE10ES": "ICD10CM", "CIE9MC": "ICD9CM"}
    omop_vocab_ids = matched_rows["TIPO_CIE"].map(vocabulary_mapping).tolist()

    # Pair CIE codes with their corresponding OMOP vocabulary IDs
    return list(zip(cie_codes, omop_vocab_ids))
