import argparse
import os
import sys
import warnings
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
from bps_to_omop import person
from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import extract, format_to_omop, map_to_omop


def process_person_table(params_file: Path, data_dir_: Path = None):

    # -- Load parameters --------------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_data = extract.read_yaml_params(params_file)
    input_dir = params_data["input_dir"]
    output_dir = params_data["output_dir"]
    input_files = params_data["input_files"]
    column_name_map = params_data["column_name_map"]
    column_values_map = params_data["column_values_map"]

    os.makedirs(data_dir_ / output_dir, exist_ok=True)

    # == Get the list of all relevant files ====================================
    # Get files
    table_person = []

    for f in input_files:
        tmp_table = parquet.read_table(data_dir_ / input_dir / f)

        # Build date columns
        tmp_table = person.build_date_columns(tmp_table)

        # -- Rename columns
        # First ensure we have a dict even when no options where provided
        column_name_map = (params_data.get("column_name_map", {}) or {}).get(f, {})
        column_name_map = {**column_name_map, "start_date": "birth_datetime"}
        tmp_table = format_to_omop.rename_table_columns(tmp_table, column_name_map)

        # -- Apply values mapping
        column_values_map = (params_data.get("column_values_map", {}) or {}).get(f, {})
        tmp_table = map_to_omop.apply_source_mapping(tmp_table, column_values_map)

        # Format the table
        tmp_table = format_to_omop.format_table(tmp_table, omop_schemas["PERSON"])

        # Append to list
        table_person.append(tmp_table)

    # Concat and save
    table_person = pa.concat_tables(table_person)
    # Save
    parquet.write_table(table_person, data_dir_ / output_dir / "PERSON.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./hepapred/preomop/genomop_person_params.yaml",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Common path to all data files",
        default=data_dir,
    )
    args = parser.parse_args()
    process_person_table(args.parameters_file, args.data_dir)
