import os
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
import pytest
import yaml

from bps_to_omop.omop_schemas import omop_schemas


@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary directory structure for testing."""
    for folder in ["input", "output", "vocab"]:
        foder_dir = tmp_path / folder
        foder_dir.mkdir()
    return tmp_path


@pytest.fixture
def sample_params(test_data_dir):
    """Create a sample parameters file."""
    params = {
        "input_dir": "input",
        "output_dir": "output",
        "input_files": ["measurement_values.parquet", "measurement_concept.parquet"],
        "vocab_dir": "vocab",
        "visit_dir": "visit",
        "append_vocabulary": {"measurement_values.parquet": "CLC"},
        "column_map": {
            "measurement_values.parquet": {
                "desc_clc": "measurement_source_value",
                "valor": "value_source_value",
            }
        },
        "vocabulary_config": {
            "measurement_values.parquet": {"CLC": "concept_name"},
            "measurement_concept.parquet": {"SNOMED": "concept_name"},
        },  # TODO Add test to map by concept_code
        "value_map": {
            "measurement_values.parquet": "numeric",
            "measurement_concept.parquet": "concept",
        },  # TODO Maybe rename concept to categorical for consistency
        "unmapped_measurement": {},
        "unampped_unit": {"x 10^3/µL": 8848},
    }

    params_file = test_data_dir / "test_params.yaml"
    with open(params_file, "w") as f:
        yaml.dump(params, f)

    return params_file


@pytest.fixture
def sample_measurement_values(test_data_dir):
    """Create sample input parquet file with numeric values."""
    df = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2020-01-01", "2020-01-01", "2020-01-01"],
            "end_date": ["2020-01-01", "2020-01-01", "2020-01-01"],
            "type_concept": ["1", "1", "1"],
            "desc_clc": ["Hemoglobina", "Plaquetas (recuento)", "Albúmina"],
            "valor": ["11.0", "22.0", "33.0"],
        }
    )

    file_path = test_data_dir / "input" / "test_provider.parquet"
    df.to_parquet(file_path)
    return file_path


@pytest.fixture
def sample_measurement_categorical(test_data_dir):
    """Create sample input parquet file with categorical data."""
    df = pd.DataFrame(
        {
            "person_id": [1, 2],
            "start_date": ["2020-01-01", "2020-01-01"],
            "end_date": ["2020-01-01", "2020-01-01"],
            "type_concept": ["1", "1"],
            "desc_clc": [
                "Hepatitis C virus measurement",
                "Hepatitis C virus measurement",
            ],
            "vocabulary_id": ["SNOMED", "SNOMED"],
            "valor": ["Negative", "Positive"],
        }
    )

    file_path = test_data_dir / "input" / "test_provider.parquet"
    df.to_parquet(file_path)
    return file_path


@pytest.fixture
def sample_visit_table(test_data_dir):
    """Create sample input parquet file with categorical data."""
    df = pd.DataFrame(
        {
            "visit_occurrence_id": [1, 2],
            "person_id": [1, 2],
            "visit_start_date": ["2020-01-01", "2020-01-01"],
            "visit_end_date": ["2020-01-01", "2020-01-01"],
            "visit_type_concept_id": ["1", "1"],
        }
    )

    file_path = test_data_dir / "input" / "test_provider.parquet"
    df.to_parquet(file_path)
    return file_path
