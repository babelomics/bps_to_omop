# %%
import os
import sys

import pyarrow as pa
from package.datasets import data_dir
from pyarrow import parquet

sys.path.append("./external/bps_to_omop")
import utils.common as gen
import utils.extract as ext
import utils.transform_table as ftr

from bps_to_omop.omop_schemas import omop_schemas

from . import format_to_omop

# %%
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_observation_period_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters... ", end="")

# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]
n_days = params_data["n_days"]

os.makedirs(data_dir / output_dir, exist_ok=True)
print("Done!")

# %%
# -- Firstly, concatenate all files -----------------------------------------
print("Preprocessing files... ")
table = []
for f in input_files:
    print(" ", f)
    table_raw = parquet.read_table(
        data_dir / input_dir / f,
        columns=["person_id", "start_date", "end_date", "type_concept"],
    )

    # If no transformations, finish iteration
    if not params_data.get("transformations", False):
        table.append(table_raw)
        continue
    elif not params_data.get("transformations", False).get(f, False):
        table.append(table_raw)
        continue
    # If transformations have to be done, do them
    else:
        for func in params_data["transformations"][f]:
            func = ftr.transformations[params_data["transformations"][f]]
            table_raw = func(table_raw)
        table.append(table_raw)
# Join them all
table = pa.concat_tables(table)

# %%
# -- Secondly, pretreatment --------------------------------------------
print("Cleaning files... ", end="")
# Change to pandas to clean easily.
df_rare = table.to_pandas()
# Remove duplicates and nans
df_rare = df_rare.drop_duplicates()
df_rare = df_rare.dropna(subset=["start_date", "end_date"])
print("Done!")

# %%
# -- Thirdly, treat dates -----------------------------------------------
# Group the dates. First we remove dates contained one inside
# other and then we group periods that are not more than n_days apart
# days between them
df_rare = gen.remove_overlap(
    df_rare,
    sorting_columns=["person_id", "start_date", "end_date", "type_concept"],
    ascending_order=[True, True, False, True],
    verbose=2,
)
df_rare = gen.group_dates(df_rare, n_days, verbose=2)

# %%
# -- Final treatment -----------------------------------------------------
print("Casting to schema... ", end="")
# We assign the local id, rename columns to their final name and caste
# to the final format of each column. Show some info and save.

# Convert back to pyarrow table
table = pa.Table.from_pandas(df_rare, preserve_index=False)

# Add observation_period_id
observation_period_id = pa.array(range(len(table)))
table = table.add_column(0, "observation_period_id", observation_period_id)

# Rename to omop columns
table = format_to_omop.rename_table_columns(
    table,
    {
        "start_date": "observation_period_start_date",
        "end_date": "observation_period_end_date",
        "type_concept": "period_type_concept_id",
    },
)

# Fill, reorder and cast to schema
table = format_to_omop.format_table(table, omop_schemas["OBSERVATION_PERIOD"])
print("Done!")

# %%
print("Saving... ", end="")
parquet.write_table(table, data_dir / output_dir / "OBSERVATION_PERIOD.parquet")
print("Done!")
