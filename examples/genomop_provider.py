import os
import sys

import pandas as pd
import pyarrow as pa
from package.datasets import data_dir
from pyarrow import parquet

sys.path.append("external/bps_to_omop/")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
from bps_to_omop.omop_schemas import omop_schemas

# == Parameters ===================================================================================
print("Generating PROVIDER table:")
print("Loading parameters...")
load_dotenv()
main_dir = os.environ.get("LOCAL_DATA_DIR")
input_dir = f"{main_dir}/omop_initial/clinical/"
output_dir = f"{main_dir}/omop_intermediate/PROVIDER/"
os.makedirs(output_dir, exist_ok=True)

# == Parameters =======================================================
params_file = "./package/preomop/genomop_provider_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")
# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]

os.makedirs(data_dir / output_dir, exist_ok=True)


# -- Mapping dict ---------------------------------------------------------------------------------
# Ideally this should be put as a single concept in the OMOP vocabularies and mapped.
# As there are only two here, we are going to do it manually.
espe_dict = {"ginecologia": 38003902, "aparato digestivo": 38004455}

# == Load file and prepare it =====================================================================
print("Preprocessing files...")
# -- 02_Patologias_BPS.parquet --------------------------------------------------------------------
f = "05_Consultas_externas.parquet"
print(f" Processing {f}: ")
df = pd.read_parquet(f"{input_dir}{f}")
# Retrieve unique specialties
unique_spe = df["ESPECIALIDAD"].unique()
# Fill a row for each value
provider = []
for i, spe in enumerate(unique_spe):
    spe_norm = gen.normalize_text(spe)
    provider_row = {
        "provider_id": i,
        "provider_name": gen.normalize_text(spe),
        "provider_source_value": spe,
        "specialty_source_value": spe,
        "specialty_concept_id": espe_dict[spe_norm],
    }
    provider.append(provider_row)

# == Final steps ==================================================================================
print("Formatting to OMOP...")
# Convert to table
provider_table = pa.Table.from_pylist(provider, schema=omop_schemas["PROVIDER"])

# Fill all other columns required by the OMOP schema
provider_table = gen.fill_omop_table(
    provider_table, omop_schemas["PROVIDER"], verbose=1
)

# Reorder columns and cast to OMOP schema
provider_table = gen.reorder_omop_table(provider_table, omop_schemas["PROVIDER"])
provider_table = provider_table.cast(omop_schemas["PROVIDER"])

# == Save to parquet ==============================================================================
print("Saving to parquet...")
parquet.write_table(provider_table, f"{output_dir}PROVIDER.parquet")
print("Done.")
