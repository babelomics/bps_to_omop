import os
import sys

import pytest
import yaml

sys.path.append("../bps_to_omop/")
from bps_to_omop.extract import read_yaml_config, update_yaml_config

# # == Fixtures =========================================================
# @pytest.fixture
# def temp_yaml_file(tmp_path):
#     """Create a temporary YAML file with initial content."""
#     file_path = tmp_path / "config.yaml"
#     initial_content = {
#         "database": {
#             "host": "localhost",
#             "port": 5432
#         },
#         "api_key": "initial_key"
#     }

#     with open(file_path, 'w') as f:
#         yaml.safe_dump(initial_content, f)

#     return file_path

# == Tests ============================================================


def test_create_new_file_with_dict(tmp_path):
    """Test creating a new YAML file if it doesn't exist."""
    new_file = tmp_path / "new_config.yaml"
    updates = {"new_setting": "value"}

    update_yaml_config(str(new_file), "updates", updates)
    result = read_yaml_config(str(new_file))

    assert result["new_setting"] == "value"
    assert os.path.exists(new_file)


def test_create_new_file_with_dict(tmp_path):
    """Test creating a new YAML file if it doesn't exist."""
    new_file = tmp_path / "new_config.yaml"
    updates = {"new_setting": "value"}

    update_yaml_config(str(new_file), "updates", updates)
    result = read_yaml_config(str(new_file))

    assert result["new_setting"] == "value"
    assert os.path.exists(new_file)


# def test_empty_updates(temp_yaml_file):
#     """Test applying empty updates."""
#     updates = {}
#     result = update_yaml_config(temp_yaml_file, updates)

#     assert result["api_key"] == "initial_key"
#     assert result["database"]["port"] == 5432

# # Tests
# def test_read_existing_file(temp_yaml_file):
#     """Test reading from an existing YAML file."""
#     updates = {"new_key": "new_value"}
#     result = update_yaml_file(temp_yaml_file, updates)

#     assert "database" in result
#     assert result["database"]["host"] == "localhost"
#     assert result["new_key"] == "new_value"

# def test_update_nested_value(temp_yaml_file):
#     """Test updating a nested value in the YAML file."""
#     updates = {"database": {"host": "new_host", "port": 5432}}
#     result = update_yaml_file(temp_yaml_file, updates)

#     assert result["database"]["host"] == "new_host"

#     # Verify file was actually updated
#     with open(temp_yaml_file, 'r') as file:
#         loaded_content = yaml.safe_load(file)
#         assert loaded_content["database"]["host"] == "new_host"


# def test_overwrite_existing_value(temp_yaml_file):
#     """Test overwriting an existing value."""
#     updates = {"api_key": "new_key"}
#     result = update_yaml_file(temp_yaml_file, updates)

#     assert result["api_key"] == "new_key"

#     # Verify old value is gone
#     with open(temp_yaml_file, 'r') as file:
#         loaded_content = yaml.safe_load(file)
#         assert loaded_content["api_key"] == "new_key"
