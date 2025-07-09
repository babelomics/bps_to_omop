import os
import pathlib

import numpy as np
import pandas as pd
import pytest
import yaml

import bps_to_omop.extract as ext
from bps_to_omop.mapping import map_source_value


def test_map_source_value_by_concept_code():
    """Test if mapping by concept_code works."""

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
    """Test if mapping by concept_code works."""

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
