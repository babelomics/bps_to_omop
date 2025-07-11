import os
import sys
import warnings

import pandas as pd
import pyarrow as pa
from pyarrow import parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import extract, format_to_omop, map_to_omop

# == Parameters =======================================================
params_file = "./package/preomop/genomop_provider_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")
# -- Load yaml file and related info
params_data = extract.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]
column_name_map = params_data["column_name_map"]
column_values_map = params_data["column_values_map"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# == Load file and prepare it =====================================================================
print("Preprocessing files...")
provider_id = 0
for f in input_files:
    print(f" Processing {f}: ")
    df = pd.read_parquet(data_dir / input_dir / f)

    # Rename columns
    col_map = {**column_name_map[f]}
    df = df.rename(column_name_map[f])

    # Retrieve unique specialties
    unique_spe = df["specialty_source_value"].unique()
    # Fill a row for each value
    provider = []
    for i, spe in enumerate(unique_spe):
        provider_row = {
            "provider_id": provider_id,
            "specialty_source_value": spe,
            "specialty_concept_id": column_values_map[f][spe],
        }
        provider.append(provider_row)
        provider_id += 1

# %%
# == Check for codes that were not mapped =============================
test_list = ["specialty"]

for col in test_list:
    # Check for unmapped values
    unmapped_values = map_to_omop.find_unmapped_values(
        df, f"{col}_source_value", f"{col}_concept_id"
    )
    # Apply mapping if needed
    if len(unmapped_values) > 0:
        print(f" No concept ID found for source values: {[*unmapped_values]}")

# == Final steps ==================================================================================
print("Formatting to OMOP...")
# Convert to table
provider_table = pa.Table.from_pylist(provider, schema=omop_schemas["PROVIDER"])

# Fill, reorder and cast to schema
provider_table = format_to_omop.format_table(provider_table, omop_schemas["PROVIDER"])

# == Save to parquet ==============================================================================
print("Saving to parquet...")
parquet.write_table(provider_table, data_dir / output_dir / "PROVIDER.parquet")
print("Done.")
