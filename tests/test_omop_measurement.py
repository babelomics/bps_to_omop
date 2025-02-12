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
        "column_name_map": {},
        "column_values_map": {
            "test_measurement.parquet": {"CARDIOLOGIA": 38004451, "PEDIATRIA": 38004477}
        },
        "unmapped_specialty": {"NUEVA_ESPECIALIDAD": 0},
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
            "desc_clc": ["Hemoglobina", "Albúmina", "Albúmina"],
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
            "desc_clc": ["Measurement", "Measurement"],
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
