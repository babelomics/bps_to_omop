import numpy as np
import pandas as pd
import pytest

from bps_to_omop.utils import map_to_omop


@pytest.fixture
def sample_dataframes():
    """Create sample DataFrames for testing."""

    # CONCEPT
    columns = ["concept_code", "concept_name", "vocabulary_id", "concept_id"]
    rows = [
        ("I10", "Essential hypertension", "ICD10CM", 44821949),
        ("401.9", "Essential hypertension", "ICD9CM", 35207668),
        ("59621000", "Essential hypertension", "SNOMED", 320128),
    ]
    concept_df = pd.DataFrame.from_records(rows, columns=columns)

    # CONCEPT_RELATIONSHIP
    columns = ["concept_id_1", "relationship_id", "concept_id_2"]
    rows = [
        (35207668, "Maps to", 320128),
        (44821949, "Maps to", 320128),
        (320128, "Maps to", 320128),
        (320128, "Mapped from", 320128),
    ]
    concept_rel_df = pd.DataFrame.from_records(rows, columns=columns)

    return concept_df, concept_rel_df


@pytest.fixture
def sample_params():
    """Create a sample parameters file."""
    sample_params = {
        "input_dir": "input",
        "output_dir": "output",
        "fallback_vocabs": {"ICD10CM": "concept_code", "ICD9CM": "concept_code"},
    }

    return sample_params


def test_maps_unmapped_values_with_fallback(sample_dataframes, sample_params):
    """Test that unmapped values get processed with fallback vocabularies."""
    concept_df, concept_rel_df = sample_dataframes

    # Define input
    columns = [
        "test_source_value",
        "vocabulary_id",
        "test_source_concept_id",
        "test_concept_id",
    ]
    rows = [
        ("I10", "ICD10CM", 44821949, 320128),  # This is fine, does it change?
        ("I10", "ICD10CM", 44821949, 0),  # Missing concept_if, does it update?
        ("401.9", "ICD10CM", np.nan, 0),  # Wrong vocab, does it update?
    ]
    df_input = pd.DataFrame.from_records(rows, columns=columns)

    # Define expected output
    rows = [
        ("I10", "ICD10CM", 44821949, 320128),
        ("I10", "ICD10CM", 44821949, 320128),
        ("401.9", "ICD9CM", 35207668, 320128),
    ]
    expected_output = pd.DataFrame.from_records(rows, columns=columns)
    expected_output
    expected_output["test_source_concept_id"] = expected_output[
        "test_source_concept_id"
    ].astype(pd.Int64Dtype())
    expected_output["test_concept_id"] = expected_output["test_concept_id"].astype(
        pd.Int64Dtype()
    )

    # Apply the function
    df_output = map_to_omop.fallback_mapping(
        df_input,
        concept_df,
        concept_rel_df,
        sample_params,
        col_prefix="test",
    )

    # Check
    pd.testing.assert_frame_equal(
        df_output,
        expected_output,
    )


def test_no_mapping_without_vocab(sample_dataframes, sample_params):
    """
    Test that if a source code has no mapping it will change the vocabulary_id column.

    Last vocab in fallback_vocabs will remain in vocabulary_id, erasing the original value, if any.
    This is ok as vocabulary_id column is not carried to the final OMOP tables.
    """
    concept_df, concept_rel_df = sample_dataframes

    # Define input
    columns = [
        "test_source_value",
        "vocabulary_id",
        "test_source_concept_id",
        "test_concept_id",
    ]
    rows = [
        ("155296003", "Nebraska Lexicon", np.nan, 0),  # Wrong vocab, does it update?
    ]
    df_input = pd.DataFrame.from_records(rows, columns=columns)

    # Define expected output
    rows = [
        (
            "155296003",
            "ICD9CM",
            np.nan,
            0,
        ),  # Tried to map, leaving ICD9CM, without luck
    ]
    expected_output = pd.DataFrame.from_records(rows, columns=columns)
    expected_output
    expected_output["test_source_concept_id"] = expected_output[
        "test_source_concept_id"
    ].astype(pd.Int64Dtype())
    expected_output["test_concept_id"] = expected_output["test_concept_id"].astype(
        pd.Int64Dtype()
    )

    # Apply the function
    df_output = map_to_omop.fallback_mapping(
        df_input,
        concept_df,
        concept_rel_df,
        sample_params,
        col_prefix="test",
    )

    # Check
    pd.testing.assert_frame_equal(
        df_output,
        expected_output,
    )


def test_handles_different_unmapped_value_types(sample_dataframes, sample_params):
    """Test that function correctly identifies different types of unmapped values."""
    concept_df, concept_rel_df = sample_dataframes

    # Define input
    columns = [
        "test_source_value",
        "vocabulary_id",
        "test_source_concept_id",
        "test_concept_id",
    ]
    rows = [
        ("I10", "ICD10CM", 0, 0),
        ("I10", "ICD10CM", "", ""),
        ("I10", "ICD10CM", np.nan, np.nan),
        ("I10", "ICD10CM", None, None),
    ]
    df_input = pd.DataFrame.from_records(rows, columns=columns)

    # Define expected output
    rows = [
        ("I10", "ICD10CM", 44821949, 320128),
        ("I10", "ICD10CM", 44821949, 320128),
        ("I10", "ICD10CM", 44821949, 320128),
        ("I10", "ICD10CM", 44821949, 320128),
    ]

    # Apply the function
    df_output = map_to_omop.fallback_mapping(
        df_input,
        concept_df,
        concept_rel_df,
        sample_params,
        col_prefix="test",
    )

    expected_output = pd.DataFrame.from_records(rows, columns=columns)
    expected_output

    # Check
    pd.testing.assert_frame_equal(df_output, expected_output, check_dtype=False)
