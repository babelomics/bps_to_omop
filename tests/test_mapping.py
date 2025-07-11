import numpy as np
import pandas as pd
import pytest
from utils.map_to_omop import (
    map_source_concept_id,
    map_source_value,
    update_concept_mappings,
)


def test_map_source_value_by_concept_code():
    """
    Test if mapping by concept_code works.
    Firt entry should map correctly, second is not valid, should map to none.
    """

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "CLC"],
            "source_value": ["CLC00229", "CLC999"],
        }
    )

    # Define the columns to which each vocabulary should be mapped to
    target_vocab = {
        "CLC": "concept_code",
    }

    # Define the concept table
    concept_df = pd.DataFrame(
        {
            "concept_id": [2000001178],
            "concept_name": ["Virus de la hepatitis B (Ag c), Ac"],
            "domain_id": ["Measurement"],
            "vocabulary_id": ["CLC"],
            "standard_code": [None],
            "concept_code": ["CLC00229"],
        }
    )

    # Define what should be the output
    df_output = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "CLC"],
            "source_value": ["CLC00229", "CLC999"],
            "source_concept_id": [2000001178, None],
        }
    ).astype({"source_concept_id": pd.Int64Dtype()})

    df_out = map_source_value(df_input, target_vocab, concept_df)

    pd.testing.assert_frame_equal(df_output, df_out)


def test_map_source_value_by_concept_name():
    """
    Test if mapping by concept_name works.
    Firt entry should map correctly, second is not valid, should map to none.
    """

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "CLC"],
            "source_value": [
                "Virus de la hepatitis B (Ag c), Ac",
                "Virus de la hepatitis B",
            ],
        }
    )

    # Define the columns to which each vocabulary should be mapped to
    target_vocab = {
        "CLC": "concept_name",
    }

    # Define the concept table
    concept_df = pd.DataFrame(
        {
            "concept_id": [2000001178],
            "concept_name": ["Virus de la hepatitis B (Ag c), Ac"],
            "domain_id": ["Measurement"],
            "vocabulary_id": ["CLC"],
            "standard_code": [None],
            "concept_code": ["CLC00229"],
        }
    )

    # Define what should be the output
    df_output = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "CLC"],
            "source_value": [
                "Virus de la hepatitis B (Ag c), Ac",
                "Virus de la hepatitis B",
            ],
            "source_concept_id": [2000001178, None],
        }
    ).astype({"source_concept_id": pd.Int64Dtype()})

    df_out = map_source_value(df_input, target_vocab, concept_df)

    pd.testing.assert_frame_equal(df_output, df_out)


def test_map_source_value_multiple_target_vocab():
    """
    Test using different target vocabs works.
    The first one should map by concept_name, and the
    second one by concept_code
    """

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "SNOMED"],
            "source_value": ["Hemoglobina", "187033005"],
        }
    )

    # Define the columns to which each vocabulary should be mapped to
    target_vocab = {
        "CLC": "concept_name",
        "SNOMED": "concept_code",
    }

    # Define the concept table
    concept_df = pd.DataFrame(
        {
            "concept_id": [2000001144, 4092846],
            "concept_name": ["Hemoglobina", "Hepatitis C virus measurement"],
            "domain_id": ["Measurement", "Measurement"],
            "vocabulary_id": ["CLC", "SNOMED"],
            "standard_code": [None, "S"],
            "concept_code": ["CLC00229", "187033005"],
        }
    )

    # Define what should be the output
    df_output = pd.DataFrame(
        {
            "vocabulary_id": ["CLC", "SNOMED"],
            "source_value": ["Hemoglobina", "187033005"],
            "source_concept_id": [2000001144, 4092846],
        }
    ).astype({"source_concept_id": pd.Int64Dtype()})

    df_out = map_source_value(df_input, target_vocab, concept_df)

    pd.testing.assert_frame_equal(df_output, df_out)


def test_map_source_concept_id():
    """
    Test map_source_concept_id.
    The first one should map because it has a 'Maps to' relationship.
    The second one should map to 0 because it is a 'Is a' relationship, not a 'Maps to'.
    The third one should map to 0 because it has no relationship.
    """

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["SNOMED", "test_is_a", "test_no_rel"],
            "source_value": ["187033005", "AA00", "AA01"],
            "source_concept_id": [4092846, 2000000000, 2000000010],
        }
    ).astype({"source_concept_id": pd.Int64Dtype()})

    # Define the concept table
    concept_rel_df = pd.DataFrame(
        {
            "concept_id_1": [4092846, 2000000000],
            "relationship_id": ["Maps to", "Is a"],
            "concept_id_2": [4092846, 2000000001],
        }
    )

    # Define what should be the output
    df_output = pd.DataFrame(
        {
            "vocabulary_id": ["SNOMED", "test_is_a", "test_no_rel"],
            "source_value": ["187033005", "AA00", "AA01"],
            "source_concept_id": [4092846, 2000000000, 2000000010],
            "concept_id": [4092846, 0, 0],
        }
    ).astype({"source_concept_id": pd.Int64Dtype(), "concept_id": pd.Int64Dtype()})

    df_out = map_source_concept_id(df_input, concept_rel_df)

    pd.testing.assert_frame_equal(df_output, df_out)


def test_update_concept_mappings_no_update():
    """Test function returns unchanged copy when no mappings provided."""

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["SNOMED", "test_is_a", "test_no_rel"],
            "source_value": ["187033005", "AA00", "AA01"],
            "source_concept_id": [4092846, 2000000000, 2000000010],
            "concept_id": [4092846, 0, 0],
        }
    ).astype({"source_concept_id": pd.Int64Dtype(), "concept_id": pd.Int64Dtype()})

    df_out = update_concept_mappings(
        df_input,
        "source_value",
        "concept_id",
        {},
    ).astype({"concept_id": pd.Int64Dtype()})

    pd.testing.assert_frame_equal(df_input, df_out)
    assert df_input is not df_out  # Ensure it's a copy


def test_update_concept_mappings_basic_update():
    """Test basic functionality with simple mappings."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 0, 0],
        }
    )

    new_mappings = {"B2": 456, "C3": 789}

    df_out = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 456, 789],
        }
    )

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_with_nan():
    """Test function handles NaN values correctly."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, np.nan, 0],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    new_mappings = {"B2": 456, "C3": 789}

    df_out = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 456, 789],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_with_none():
    """Test function handles None values correctly."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, None, 0],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    new_mappings = {"B2": 456, "C3": 789}

    df_out = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 456, 789],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_empty_strings():
    """Test function treats empty strings as unmapped."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, "", 0],
        }
    )

    new_mappings = {"B2": 456, "C3": 789}

    df_out = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 456, 789],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    ).astype({"concept_id": pd.Int64Dtype()})
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_duplicate_mappings():
    """
    Test function works fine if there are duplicated source values
    that map to different concept_id.
    """

    # Define the table that hold the values to be mapped
    df_input = pd.DataFrame(
        {
            "vocabulary_id": ["SNOMED", "test_is_a", "test_is_a", "test_no_rel"],
            "source_value": ["187033005", "AA00", "AA00", "AA01"],
            "concept_id": [4092846, 0, 0, 0],
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    new_mappings = {"AA00": 999, "AA01": 111}

    df_output = pd.DataFrame(
        {
            "vocabulary_id": ["SNOMED", "test_is_a", "test_is_a", "test_no_rel"],
            "source_value": ["187033005", "AA00", "AA00", "AA01"],
            "concept_id": [4092846, 999, 999, 111],  # Both AA00 rows updated
        }
    ).astype({"concept_id": pd.Int64Dtype()})

    df_out = update_concept_mappings(
        df_input,
        "source_value",
        "concept_id",
        new_mappings,
    ).astype({"concept_id": pd.Int64Dtype()})

    pd.testing.assert_frame_equal(df_output, df_out)


def test_update_concept_mappings_partial_mapping():
    """Test function with mappings that don't cover all unmapped values."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3", "D4"],
            "concept_id": [123, 0, 0, 0],
        }
    )

    new_mappings = {"B2": 456}  # Only map one value

    df_out = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3", "D4"],
            "concept_id": [123, 456, 0, 0],  # C3 and D4 remain unmapped
        }
    )

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_no_matching_values():
    """Test function when new mappings don't match any source values."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2", "C3"],
            "concept_id": [123, 0, 0],
        }
    )

    new_mappings = {"X1": 999, "Y2": 888}  # No matching source values

    df_out = df_input.copy()  # Should remain unchanged

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


def test_update_concept_mappings_different_data_types():
    """Test function with different data types."""
    df_input = pd.DataFrame(
        {
            "source_value": [1, 2, 3],
            "concept_id": ["A", None, 0],
        }
    )

    new_mappings = {2: "B", 3: "C"}

    df_out = pd.DataFrame(
        {
            "source_value": [1, 2, 3],
            "concept_id": ["A", "B", "C"],
        }
    )

    result = update_concept_mappings(
        df_input, "source_value", "concept_id", new_mappings
    )
    pd.testing.assert_frame_equal(result, df_out)


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================


def test_update_concept_mappings_missing_source_column():
    """Test function raises KeyError for missing source column."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2"],
            "concept_id": [123, 0],
        }
    )

    with pytest.raises(KeyError, match="Source column 'missing_col' not found"):
        update_concept_mappings(df_input, "missing_col", "concept_id", {"A1": 999})


def test_update_concept_mappings_missing_target_column():
    """Test function raises KeyError for missing target column."""
    df_input = pd.DataFrame(
        {
            "source_value": ["A1", "B2"],
            "concept_id": [123, 0],
        }
    )

    with pytest.raises(KeyError, match="Target column 'missing_col' not found"):
        update_concept_mappings(df_input, "source_value", "missing_col", {"A1": 999})


def test_update_concept_mappings_empty_dataframe():
    """Test function raises ValueError for empty DataFrame."""
    df_input = pd.DataFrame()

    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        update_concept_mappings(df_input, "source_value", "concept_id", {"A1": 999})
