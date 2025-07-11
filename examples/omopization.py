# %%
import os
import sys
import warnings

import pyarrow.parquet as parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
from bps_to_omop import person
from bps_to_omop.utils import extract, pyarrow_utils

# %%
# -- Define parameters ------------------------------------------------
params_file = "./package/preomop/omopization_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = extract.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]
person_columns = params_data["person_columns"]
date_columns = params_data["date_columns"]
type_concept_mapping = params_data["type_concept_mapping"]
os.makedirs(data_dir / output_dir, exist_ok=True)

# %%
# -- transform the tables ---------------------------------------------
print("Transforming tables...")
for f in input_files:
    print(f"- {f}")
    # Read data
    table = parquet.read_table(data_dir / input_dir / f)
    cols_to_remove = []
    # Remove the __index_level_0__ if exists
    cols_to_remove += ["__index_level_0__"]

    # -- person_id --------------------------------------------------------------------------------
    # Get the person_id
    person_id, person_source_value = person.transform_person_id(
        table, person_columns[f]
    )
    # Remove column from list to keep
    cols_to_remove += [person_columns[f]]

    # -- start_date and end_date ------------------------------------------------------------------
    # Ensure they are ordered, i.e. end_date is after start_date
    try:
        start_date, end_date = extract.find_start_end_dates(
            table, date_columns[f], verbose=0
        )
    except (ValueError, TypeError) as inst:
        print(f"Error found! {inst}")
        raise inst
    # Remove columns from list to keep
    cols_to_remove += date_columns[f]

    # -- type_concept -----------------------------------------------------------------------------
    # Create a columns with the code
    type_concept_code = type_concept_mapping[f]
    type_concept = pyarrow_utils.create_uniform_int_array(len(table), type_concept_code)

    # -- Final steps ------------------------------------------------------------------------------
    # Append to old table
    print(f"{f} input and output columns:")
    print(" >", table.column_names)
    table = table.add_column(0, "person_id", person_id)
    table = table.add_column(1, "person_source_value", person_source_value)
    table = table.add_column(1, "start_date", start_date)
    table = table.add_column(2, "end_date", end_date)
    table = table.add_column(3, "type_concept", type_concept)
    # Remove unnecesary columns
    cols_to_keep = [col for col in table.column_names if col not in cols_to_remove]
    table = table.select(cols_to_keep)
    print(" <", table.column_names)

    # Save to the same file
    parquet.write_table(table, data_dir / output_dir / f)

print("Done!\n")
