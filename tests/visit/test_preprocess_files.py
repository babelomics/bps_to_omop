import os
import pathlib
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bps_to_omop.visit import preprocess_files


@pytest.fixture
def test_data_dir():
    """Create a temporary directory structure for testing."""
    # Create temporary directory
    temp_dir = tempfile.TemporaryDirectory()
    test_dir = Path(temp_dir.name)

    # Create test directory structure
    for folder in ["input", "output", "visit"]:
        folder_dir = test_dir / folder
        folder_dir.mkdir()

    # Return directory and cleanup handle
    yield test_dir
    temp_dir.cleanup()


@pytest.fixture
def sample_input_files(test_data_dir):
    """Create sample input parquet files."""
    # Create sample data for visit table
    visit_data = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "end_date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "type_concept": ["1", "1", "1"],
            "some_col": ["A", "B", "C"],
        }
    )

    # Convert dates
    visit_data["start_date"] = pd.to_datetime(visit_data["start_date"])
    visit_data["end_date"] = pd.to_datetime(visit_data["end_date"])

    # Save as parquet
    file_path = test_data_dir / "input" / "test_visit.parquet"
    visit_data.to_parquet(file_path)

    return file_path


@pytest.fixture
def sample_input_file_with_unused_cols(test_data_dir):
    """Create sample input parquet files."""
    # Create sample data for visit table
    visit_data = pd.DataFrame(
        {
            "person_id": [1, 1, 2],
            "start_date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "end_date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "type_concept": ["1", "1", "1"],
            "some_col": ["A", "B", "C"],
            "unused_col": [1, 2, 3],
        }
    )

    # Convert dates
    visit_data["start_date"] = pd.to_datetime(visit_data["start_date"])
    visit_data["end_date"] = pd.to_datetime(visit_data["end_date"])

    # Save as parquet
    file_path = test_data_dir / "input" / "test_visit_with_extra_col.parquet"
    visit_data.to_parquet(file_path)

    return file_path


@pytest.fixture
def sample_params():
    """Create sample parameters for testing."""
    return {
        "input_dir": "input",
        "input_files": ["test_visit.parquet"],
        "visit_concept_dict": {
            "test_visit.parquet": [["single_code", 123456, {}]],
            "test_visit_with_extra_col.parquet": [["single_code", 123456, {}]],
        },
        "provider_params": {},
        "col_to_provider_id": {},
    }


def test_preprocess_files_basic(test_data_dir, sample_input_files, sample_params):
    """Test basic preprocessing functionality."""
    # Call the function
    result = preprocess_files(sample_params, test_data_dir, verbose=0)

    # Validate result
    assert result is not None
    assert len(result) == 3


def test_preprocess_files_multiple_files(
    test_data_dir, sample_input_files, sample_params
):
    """Test preprocessing with multiple files."""
    # Update params for multiple files
    sample_params["input_files"] = ["test_visit.parquet", "test_visit.parquet"]

    # Call the function
    result = preprocess_files(sample_params, test_data_dir, verbose=0)

    # Validate result
    assert result is not None
    assert len(result) == 6


def test_preprocess_file_with_extra_unused_cols(
    test_data_dir, sample_input_file_with_unused_cols, sample_params
):
    """Test preprocessing with multiple files."""
    # Update params for multiple files
    sample_params["input_files"] = ["test_visit_with_extra_col.parquet"]

    # Call the function
    result = preprocess_files(sample_params, test_data_dir, verbose=0)

    # Validate result
    assert result is not None
    assert len(result) == 3
    assert "unused_col" not in result.columns


def test_preprocess_files_concept_assignment(
    test_data_dir, sample_input_files, sample_params
):
    """Test visit concept ID assignment."""
    # Modify parameters for different concept assignment
    sample_params["visit_concept_dict"] = {
        "test_visit.parquet": [["single_code", 987654, {}]]
    }

    # Call the function
    result = preprocess_files(sample_params, test_data_dir, verbose=0)

    # Validate result
    assert result is not None
