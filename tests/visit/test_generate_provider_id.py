from pathlib import Path

import polars as pl
import pytest

from bps_to_omop.visit import generate_provider_id


@pytest.fixture
def provider_parquet(tmp_path):
    """Write a small provider reference table to a temp parquet file."""
    provider_table = pl.DataFrame(
        {
            "provider_col": ["A", "B", "C"],
            "provider_id": [1, 2, 3],
        }
    )
    path = tmp_path / "provider.parquet"
    provider_table.write_parquet(path)
    return tmp_path


@pytest.fixture
def base_params():
    return {
        "provider_params": {"file.parquet": True},
        "provider_table_path": "provider.parquet",
        "source_to_provider_id": {"file.parquet": {"source_col": "provider_col"}},
    }


def test_maps_known_values(provider_parquet, base_params):
    """All source values present in the provider table are mapped correctly."""
    table = pl.DataFrame({"source_col": ["A", "B", "C"]})
    result = generate_provider_id(table, "file.parquet", base_params, provider_parquet)
    assert result.to_list() == [1, 2, 3]


def test_unmatched_values_are_null(provider_parquet, base_params):
    """Source values absent from the provider table produce null."""
    table = pl.DataFrame({"source_col": ["A", "UNKNOWN"]})
    result = generate_provider_id(table, "file.parquet", base_params, provider_parquet)
    assert result.to_list() == [1, None]


def test_no_provider_config_returns_all_nulls(provider_parquet, base_params):
    """When the file has no provider mapping, a fully-null Series is returned."""
    table = pl.DataFrame({"source_col": ["A", "B"]})
    result = generate_provider_id(
        table, "other_file.parquet", base_params, provider_parquet
    )
    assert result.is_null().all()
    assert len(result) == len(table)


def test_no_provider_config_returns_int64(provider_parquet, base_params):
    """Null fallback Series has Int64 dtype."""
    table = pl.DataFrame({"source_col": ["A"]})
    result = generate_provider_id(
        table, "other_file.parquet", base_params, provider_parquet
    )
    assert result.dtype == pl.Int64


def test_empty_table_with_mapping(provider_parquet, base_params):
    """An empty input table returns an empty Series without errors."""
    table = pl.DataFrame({"source_col": pl.Series([], dtype=pl.Utf8)})
    result = generate_provider_id(table, "file.parquet", base_params, provider_parquet)
    assert len(result) == 0


def test_empty_table_without_mapping(provider_parquet, base_params):
    """An empty input table with no mapping config returns an empty null Series."""
    table = pl.DataFrame({"source_col": pl.Series([], dtype=pl.Utf8)})
    result = generate_provider_id(
        table, "other_file.parquet", base_params, provider_parquet
    )
    assert len(result) == 0
