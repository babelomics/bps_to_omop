import os
import sys
import warnings

import pyarrow.parquet as parquet

try:
    from package.datasets import data_dir
except ModuleNotFoundError:
    warnings.warn("No 'data_dir' variable provided.")

sys.path.append("./external/bps_to_omop/")
from bps_to_omop import visit_occurrence
from bps_to_omop.omop_schemas import omop_schemas
from bps_to_omop.utils import extract, format_to_omop, map_to_omop

# %%
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_visit_occurrence_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = extract.read_yaml_params(params_file)
output_dir = params_data["output_dir"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# == Apply functions ==================================================
table_VISIT_OCCURRENCE_0 = visit_occurrence.gather_tables(
    data_dir, params_data, verbose=1
)
table_VISIT_OCCURRENCE_1 = visit_occurrence.clean_tables(
    table_VISIT_OCCURRENCE_0, params_data, verbose=2
)
table_VISIT_OCCURRENCE_2 = visit_occurrence.to_omop(table_VISIT_OCCURRENCE_1, verbose=1)

# == Save to parquet ==================================================
parquet.write_table(
    table_VISIT_OCCURRENCE_2, data_dir / output_dir / "VISIT_OCCURRENCE.parquet"
)
