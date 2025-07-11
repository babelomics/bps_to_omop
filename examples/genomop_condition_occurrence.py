import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as parquet
from package.datasets import data_dir

sys.path.append("./external/bps_to_omop")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
from bps_to_omop.omop_schemas import omop_schemas

from . import format_to_omop
from . import map_to_omop as mpp

# %%
# == Parameters =======================================================
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_condition_occurrence_params.yaml"

# -- Load parameters --------------------------------------------------
print("Reading parameters...")

# -- Load yaml file and related info
params_data = ext.read_yaml_params(params_file)
input_dir = params_data["input_dir"]
output_dir = params_data["output_dir"]
input_files = params_data["input_files"]
vocab_dir = params_data["vocab_dir"]
visit_dir = params_data["visit_dir"]
vocabulary_config = params_data["vocabulary_config"]
column_map = params_data["column_map"]

os.makedirs(data_dir / output_dir, exist_ok=True)

# == Load vocabularies =================================================
print("Loading vocabularies...")
concept_df = pd.read_parquet(data_dir / vocab_dir / "CONCEPT.parquet").infer_objects()
concept_rel_df = pd.read_parquet(
    data_dir / vocab_dir / "CONCEPT_RELATIONSHIP.parquet"
).infer_objects()

# == Load each file and prepare it =====================================
print("Preprocessing files...")
df_complete = []

for f in input_files:
    print(f" Processing {f}: ")
    df = pd.read_parquet(data_dir / input_dir / f)
    # assign new vocabulary column if needed
    if params_data.get("append_vocabulary", False):
        if params_data["append_vocabulary"].get(f, False):
            df["vocabulary_id"] = params_data["append_vocabulary"][f]
    # Apply renaming
    df = df.rename(column_map[f], axis=1)
    # Perform the mapping
    df = mpp.map_source_value(df, vocabulary_config[f], concept_df)
# Add to final dataframe
df_complete.append(df)

# -- Finish off joint dataframe ---------------------------------------
df = pd.concat(df_complete, axis=0)

# == Mapping to standard concept_id ===================================
print("Mapping to standard concepts...")
df = mpp.map_source_concept_id(df, concept_rel_df)
df = df.rename(
    {
        "concept_id": "condition_concept_id",
        "source_concept_id": "condition_source_concept_id",
    },
    axis=1,
)

# == Create the primary key ===========================================
df["condition_occurrence_id"] = pa.array(range(len(df)))

# == Find visit_occurence_id ==========================================
print("Finding visit_occurrence_id...")
df_visit_occurrence = pd.read_parquet(data_dir / visit_dir / "VISIT_OCCURRENCE.parquet")
# Primero asignamos el conditi
df = gen.find_visit_occurrence_id(
    df,
    ["person_id", "start_date", "condition_occurrence_id"],
    df_visit_occurrence,
    verbose=2,
)

# == Final steps ======================================================
print("Formatting to OMOP...")
# Remove duplicates
df = df.drop_duplicates()
# Pasamos a pyarrow table
table = pa.Table.from_pandas(df, preserve_index=False)

# Rename columns
table = format_to_omop.rename_table_columns(
    table,
    {
        "start_date": "condition_start_datetime",
        "end_date": "condition_end_datetime",
        "type_concept": "condition_type_concept_id",
        "source_value": "condition_source_value",
    },
)

# Format dates to remove times
visit_start_date = pc.cast(
    pc.floor_temporal(  # pylint: disable=E1101
        table["condition_start_datetime"], unit="day"
    ),
    pa.date32(),
)
visit_end_date = pc.cast(
    pc.floor_temporal(  # pylint: disable=E1101
        table["condition_end_datetime"], unit="day"
    ),
    pa.date32(),
)
table = table.add_column(1, "condition_start_date", visit_start_date)
table = table.add_column(2, "condition_end_date", visit_end_date)

# Format to schema
table_cdm_source = format_to_omop.format_table(
    table, omop_schemas["CONDITION_OCCURRENCE"]
)

# == Save to parquet ==================================================
print("Saving to parquet...")
parquet.write_table(table, data_dir / output_dir / "CONDITION_OCCURRENCE.parquet")
print("Done.")
