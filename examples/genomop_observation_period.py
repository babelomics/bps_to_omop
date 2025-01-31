# %%
import os
import sys

import pyarrow as pa
from package.datasets import data_dir
from pyarrow import parquet

sys.path.append("./external/bps_to_omop")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.observation_period as obs
from bps_to_omop.omop_schemas import omop_schemas

# %%
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_observation_period_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]
n_days = params_data["n_days"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# %%
# -- Firstly, concatenate all files -----------------------------------------
table_raw = [obs.ad_hoc_read(data_dir / input_dir / f) for f in input_files]
table_raw = pa.concat_tables(table_raw)

# %%
# -- Secondly, pretreatment --------------------------------------------
# Change to pandas to clean easily.
df_rare = table_raw.to_pandas()
# Remove duplicates and nans
df_rare = df_rare.drop_duplicates()
df_rare = df_rare.dropna(subset=["start_date", "end_date"])

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
# We assign the local id, rename columns to their final name and caste
# to the final format of each column. Show some info and save.

# Convert back to pyarrow table
table_OBSERVATION_PERIOD = pa.Table.from_pandas(df_rare, preserve_index=False)

# Add observation_period_id
observation_period_id = pa.array(range(len(table_OBSERVATION_PERIOD)))
table_OBSERVATION_PERIOD = table_OBSERVATION_PERIOD.add_column(
    0, "observation_period_id", observation_period_id
)

# Rename columns
new_names = [
    "observation_period_id",
    "person_id",
    "observation_period_start_date",
    "observation_period_end_date",
    "period_type_concept_id",
]
table_OBSERVATION_PERIOD = table_OBSERVATION_PERIOD.rename_columns(new_names)

# Cast to final scheme
table_OBSERVATION_PERIOD = table_OBSERVATION_PERIOD.cast(
    omop_schemas["OBSERVATION_PERIOD"]
)

# %%
parquet.write_table(
    table_OBSERVATION_PERIOD, data_dir / output_dir / "OBSERVATION_PERIOD.parquet"
)
