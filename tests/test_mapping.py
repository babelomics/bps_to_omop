import os
import pathlib

import numpy as np
import pandas as pd
import pytest
import yaml

import bps_to_omop.extract as ext
from bps_to_omop.mapping import (
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
            "source_concept_id": [4092846, 2000000000, 20000000010, 2000000010],
            "concept_id": [4092846, 0, 0, 0],
        }
    ).astype({"source_concept_id": pd.Int64Dtype(), "concept_id": pd.Int64Dtype()})

    # # Define what should be the output
    # df_output = pd.DataFrame(
    #     {
    #         "vocabulary_id": ["SNOMED", "test_is_a", "test_no_rel"],
    #         "source_value": ["187033005", "AA00", "AA01"],
    #         "source_concept_id": [4092846, 2000000000, 2000000010],
    #         "concept_id": [4092846, 0, 0],
    #     }
    # ).astype({"source_concept_id": pd.Int64Dtype(), "concept_id": pd.Int64Dtype()})

    df_out = update_concept_mappings(
        df_input,
        "source_value",
        "concept_id",
        {},
    ).astype({"concept_id": pd.Int64Dtype()})

    pd.testing.assert_frame_equal(df_input, df_out)
