import os
import sys

import pyarrow.parquet as parquet
from package.datasets import data_dir

sys.path.append("./external/bps_to_omop")
import utils.extract as ext

import bps_to_omop.visit_occurrence as vso

# %%
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_visit_occurrence_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
output_dir = params_data["output_dir"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# == Apply functions ==================================================
table_VISIT_OCCURRENCE_0 = vso.gather_tables(data_dir, params_data, verbose=1)
table_VISIT_OCCURRENCE_1 = vso.clean_tables(
    table_VISIT_OCCURRENCE_0, params_data, verbose=2
)
table_VISIT_OCCURRENCE_2 = vso.to_omop(table_VISIT_OCCURRENCE_1, verbose=1)

# == Save to parquet ==================================================
parquet.write_table(
    table_VISIT_OCCURRENCE_2, data_dir / output_dir / "VISIT_OCCURRENCE.parquet"
)
