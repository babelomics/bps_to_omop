# %%
import os
import sys
import warnings

import pyarrow as pa
from pyarrow import parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import common, extract, format_to_omop

# %%
# == Parameters =======================================================
params_file = "./package/preomop/genomop_cdm_source_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")
# -- Load yaml file and related info
params_data = extract.read_yaml_params(params_file)
output_dir = params_data["output_dir"]
cdm_source_fields = params_data["cdm_source_fields"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# Table data
cdm_source_row = [cdm_source_fields]

# %%
# -- Table generation -------------------------------------------------
table_cdm_source = pa.Table.from_pylist(
    cdm_source_row, schema=omop_schemas["CDM_SOURCE"]
)

# Format to schema
table_cdm_source = format_to_omop.format_table(
    table_cdm_source, omop_schemas["CDM_SOURCE"]
)

# Save
parquet.write_table(table_cdm_source, output_dir / "CDM_SOURCE.parquet")
