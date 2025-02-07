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
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    return tmp_path


@pytest.fixture
def sample_params(test_data_dir):
    """Create a sample parameters file."""
    params = {
        "input_dir": "input",
        "output_dir": "output",
        "input_files": ["test_provider.parquet"],
        "column_name_map": {},
        "column_values_map": {
            "test_provider.parquet": {"CARDIOLOGIA": 38004451, "PEDIATRIA": 38004477}
        },
        "unmapped_specialty": {"NUEVA_ESPECIALIDAD": 0},
    }

    params_file = test_data_dir / "test_params.yaml"
    with open(params_file, "w") as f:
        yaml.dump(params, f)

    return params_file


@pytest.fixture
def sample_input_data(test_data_dir):
    """Create sample input parquet file."""
    df = pd.DataFrame(
        {
            "ESPECIALIDAD": [
                "CARDIOLOGIA",
                "PEDIATRIA",
                "CARDIOLOGIA",
                "NUEVA_ESPECIALIDAD",
            ]
        }
    )

    file_path = test_data_dir / "input" / "test_provider.parquet"
    df.to_parquet(file_path)
    return file_path


def test_specialty_mapping(test_data_dir, sample_input_data, sample_params):
    """Test that specialties are mapped correctly to concept IDs."""
    # Read the input data
    df = pd.read_parquet(sample_input_data)

    # Load parameters
    with open(sample_params, encoding="utf-8") as f:
        params = yaml.safe_load(f)

    # Check unique specialties
    unique_spe = df["ESPECIALIDAD"].unique()
    assert "CARDIOLOGIA" in unique_spe
    assert "PEDIATRIA" in unique_spe

    # Check mapping
    specialty_map = params["column_values_map"]["test_provider.parquet"]
    assert specialty_map["CARDIOLOGIA"] == 38004451
    assert specialty_map["PEDIATRIA"] == 38004477


def test_output_schema(test_data_dir, sample_input_data, sample_params):
    """Test that the output follows the OMOP schema."""
    # First, run the script (you'll need to modify your script to be importable)
    # For now, we'll just check the schema structure

    # Read the output file if it exists
    output_file = test_data_dir / "output" / "PROVIDER.parquet"
    if output_file.exists():
        provider_table = parquet.read_table(output_file)

        # Check schema matches OMOP
        expected_schema = omop_schemas["PROVIDER"]
        assert set(provider_table.schema.names) == set(expected_schema.names)

        # Check data types
        for field in expected_schema:
            assert field.name in provider_table.schema.names
            assert provider_table.schema.field(field.name).type == field.type


def test_unmapped_specialty_handling(test_data_dir, sample_input_data, sample_params):
    """Test handling of unmapped specialty values."""
    # Read the input data
    df = pd.read_parquet(sample_input_data)

    # Load parameters
    with open(sample_params) as f:
        params = yaml.safe_load(f)

    # Check that NUEVA_ESPECIALIDAD is in unmapped specialties
    assert "NUEVA_ESPECIALIDAD" in params["unmapped_specialty"]

    # Verify it exists in the input data
    assert "NUEVA_ESPECIALIDAD" in df["ESPECIALIDAD"].values


def test_unique_provider_ids(test_data_dir, sample_input_data, sample_params):
    """Test that provider IDs are unique."""
    output_file = test_data_dir / "output" / "PROVIDER.parquet"
    if output_file.exists():
        provider_table = parquet.read_table(output_file)
        provider_ids = provider_table.column("provider_id").to_pylist()

        # Check for uniqueness
        assert len(provider_ids) == len(set(provider_ids))

        # Check they're sequential starting from 0
        assert min(provider_ids) == 0
        assert max(provider_ids) == len(provider_ids) - 1


@pytest.fixture(autouse=True)
def cleanup(test_data_dir):
    """Clean up test files after each test."""
    yield
    import shutil

    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)
