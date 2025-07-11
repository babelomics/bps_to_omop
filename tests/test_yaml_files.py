import os
import sys

import pytest
import yaml

sys.path.append("../bps_to_omop/")
from utils.extract import read_yaml_params, update_yaml_params


# == Fixtures =========================================================
@pytest.fixture
def temp_yaml_file(tmp_path):
    """Create a temporary YAML file with initial content."""
    file_path = tmp_path / "params.yaml"
    initial_content = {
        "str": "/path1",
        "list": ["file1", "file2"],
        "dict": {"file1": "/path1", "file2": "/path2"},
    }

    with open(file_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(initial_content, f)

    return file_path


# == Tests ============================================================
def test_read_existing_file(temp_yaml_file):
    """Test reading from an existing YAML file."""

    result = read_yaml_params(temp_yaml_file)

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file1": "/path1", "file2": "/path2"}


def test_create_new_file_with_dict(tmp_path):
    """Test creating a new YAML file with a new dict if it doesn't exist."""
    new_file = tmp_path / "new_params.yaml"
    new_setting = {"new_setting": "new_value"}

    update_yaml_params(str(new_file), "new_setting", new_setting)
    result = read_yaml_params(str(new_file))

    assert result["new_setting"] == "new_value"
    assert os.path.exists(new_file)


def test_create_new_file_with_value(tmp_path):
    """Test creating a new YAML file with a new value if it doesn't exist."""
    new_file = tmp_path / "new_params.yaml"
    new_setting = "new_value"

    update_yaml_params(str(new_file), "new_setting", new_setting)
    result = read_yaml_params(str(new_file))

    assert result["new_setting"] == "new_value"
    assert os.path.exists(new_file)


def test_empty_updates(temp_yaml_file):
    """Test applying empty updates."""
    updates = {}
    update_yaml_params(temp_yaml_file, "updates", updates)
    result = read_yaml_params(temp_yaml_file)

    assert len(result["updates"]) == 0

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file1": "/path1", "file2": "/path2"}


def test_new_string(temp_yaml_file):
    """Test writing new string."""
    new_string = "new_string"
    update_yaml_params(temp_yaml_file, "new_string", new_string)
    result = read_yaml_params(temp_yaml_file)

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file1": "/path1", "file2": "/path2"}
    assert result["new_string"] == "new_string"


def test_new_list(temp_yaml_file):
    """Test writing new list."""
    new_list = ["file3", "file4"]
    update_yaml_params(temp_yaml_file, "new_list", new_list)
    result = read_yaml_params(temp_yaml_file)

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file1": "/path1", "file2": "/path2"}
    assert result["new_list"] == ["file3", "file4"]


def test_new_dict(temp_yaml_file):
    """Test writing new dict"""
    new_dict = {"file3": "path3", "file4": "path4"}
    update_yaml_params(temp_yaml_file, "new_dict", new_dict)
    result = read_yaml_params(temp_yaml_file)

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file1": "/path1", "file2": "/path2"}
    assert result["new_dict"] == {"file3": "path3", "file4": "path4"}


def test_overwrite_existing_value(temp_yaml_file):
    """Test overwriting an existing value."""
    new_dict = {"file3": "path3", "file4": "path4"}
    update_yaml_params(temp_yaml_file, "dict", new_dict)
    result = read_yaml_params(temp_yaml_file)

    assert result["str"] == "/path1"
    assert result["list"] == ["file1", "file2"]
    assert result["dict"] == {"file3": "path3", "file4": "path4"}
