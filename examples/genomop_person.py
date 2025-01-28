# %%
import os
import sys

import numpy as np
import pyarrow as pa
from package.datasets import data_dir
from pyarrow import parquet

sys.path.append("./external/bps_to_omop/")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.person as per
from bps_to_omop.omop_schemas import omop_schemas

# %%
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_person_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
input_dir = data_dir / params_data["input_dir"]
output_dir = data_dir / params_data["output_dir"]
input_files = params_data["input_files"]
column_name_map = params_data["column_name_map"]
column_values_map = params_data["column_values_map"]

os.makedirs(output_dir, exist_ok=True)

# %%
# == Get the list of all relevant files ====================================
# Get files
table_person = []

for f in input_files:
    tmp_table = parquet.read_table(input_dir / f)

    # Build date columns
    tmp_table = per.build_date_columns(tmp_table)

    # Rename columns
    col_map = {**column_name_map[f], "start_date": "birth_datetime"}
    tmp_table = gen.rename_table_columns(tmp_table, column_name_map[f])

    # Apply the mapping
    if len(column_values_map[f]) > 0:
        column_map = column_values_map[f]
        tmp_table = gen.apply_source_mapping(tmp_table, column_map)

    # -- Finishing up
    # Fill other fields
    tmp_table = gen.fill_omop_table(tmp_table, omop_schemas["PERSON"])
    tmp_table = gen.reorder_omop_table(tmp_table, omop_schemas["PERSON"])
    # Cast to schema
    tmp_table = tmp_table.cast(omop_schemas["PERSON"])

    # Append to list
    table_person.append(tmp_table)

# Concat and save
table_person = pa.concat_tables(table_person)
# Save
parquet.write_table(table_person, output_dir / "PERSON.parquet")
