# %%
import os
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet

from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import format_to_omop, map_to_omop, pyarrow_utils


# %%
def process_location_table(data_dir: Path, params_location: dict):
    """
    Process location data files and create an OMOP-formatted LOCATION table.

    Parameters
    ----------
    data_dir : Path
        Base directory containing input and output subdirectories.
    params_location : dict
        Configuration dictionary with keys: 'input_dir', 'output_dir',
        'input_files', and optional 'column_name_map', 'column_values_map',
        'constant_values'.

    Returns
    -------
    None
        Writes LOCATION.parquet file to the specified output directory.
    """

    # == Load parameters ==============================================
    input_dir = params_location["input_dir"]
    output_dir = params_location["output_dir"]
    input_files = params_location["input_files"]
    column_name_map = params_location.get("column_name_map", {})
    column_values_map = params_location.get("column_values_map", {})
    constant_values = params_location.get("constant_values", {})

    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)
    os.makedirs(data_dir / output_dir, exist_ok=True)

    # == Get the list of all relevant files ====================================
    # Get files
    table_location = []

    for f in input_files:
        tmp_table = parquet.read_table(data_dir / input_dir / f)

        # -- Rename columns -------------------------------------------
        # First ensure we have a dict with the relevant info
        tmp_colmap = column_name_map.get(f, {})

        # Ensure there is at least a column that was mapped to location_id
        assert (
            "location_id" in tmp_colmap.values()
        ), f"File {f} has no map to location_id"

        tmp_table = format_to_omop.rename_table_columns(tmp_table, tmp_colmap)

        # -- Apply values mapping -------------------------------------
        tmp_valmap = column_values_map.get(f, {})
        if tmp_valmap:
            tmp_table = map_to_omop.apply_source_mapping(tmp_table, tmp_valmap)

        # -- Add Constant values --------------------------------------
        tmp_cteval = constant_values.get(f, {})
        if tmp_cteval:
            for col_name, col_value in tmp_cteval.items():
                if isinstance(col_value, (int, float)):
                    col_values = pyarrow_utils.create_uniform_double_array(
                        tmp_table.shape[0], col_value
                    )
                    tmp_table = tmp_table.append_column(col_name, col_values)
                else:
                    col_values = pyarrow_utils.create_uniform_str_array(
                        tmp_table.shape[0], col_value
                    )
                    tmp_table = tmp_table.append_column(col_name, col_values)

        # -- Format the table -----------------------------------------
        tmp_table = format_to_omop.format_table(tmp_table, omop_schemas["LOCATION"])

        # Append to list
        table_location.append(tmp_table)

    # Concat and save
    table_location = pa.concat_tables(table_location)
    # Save
    parquet.write_table(table_location, data_dir / output_dir / "LOCATION.parquet")
