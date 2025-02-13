import argparse
import os
import sys
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
from bps_to_omop.omop_schemas import omop_schemas


def process_death_table(params_file: Path, data_dir_: Path = None):

    # -- Load parameters --------------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_data = ext.read_yaml_params(params_file)
    input_dir = params_data["input_dir"]
    output_dir = params_data["output_dir"]
    input_files = params_data["input_files"]

    os.makedirs(data_dir_ / output_dir, exist_ok=True)

    # == Get the list of all relevant files ====================================
    # Get files
    table_death = []

    for f in input_files:
        tmp_table = parquet.read_table(data_dir_ / input_dir / f)

        # -- Rename columns
        # First ensure we have a dict even when no options where provided
        column_name_map = (params_data.get("column_name_map", {}) or {}).get(f, {})
        # ensure we always change the end_date
        column_name_map = {
            **column_name_map,
            "end_date": "death_date",
            "type_concept": "death_type_concept_id",
        }
        tmp_table = gen.rename_table_columns(tmp_table, column_name_map)

        # -- Apply values mapping
        column_values_map = (params_data.get("column_values_map", {}) or {}).get(f, {})
        tmp_table = gen.apply_source_mapping(tmp_table, column_values_map)

        # Death table should not have nulls, remove them
        mask = pc.is_valid(pc.cast(tmp_table["death_date"], pa.string()))
        tmp_table = tmp_table.filter(mask)
        tmp_table = gen.format_table(tmp_table, omop_schemas["DEATH"])

        # Append to list
        table_death.append(tmp_table)

    # Concat and save
    table_death = pa.concat_tables(table_death)
    # Save
    parquet.write_table(table_death, data_dir_ / output_dir / "DEATH.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./hepapred/preomop/genomop_death_params.yaml",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Common path to all data files",
        default=data_dir,
    )
    args = parser.parse_args()
    process_death_table(args.parameters_file, args.data_dir)
