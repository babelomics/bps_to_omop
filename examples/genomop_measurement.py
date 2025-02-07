import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from pacakge.datasets import data_dir

sys.path.append("./external/bps_to_omop")
import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.measurement as mea
from bps_to_omop.omop_schemas import omop_schemas

# %%
# == Parameters =======================================================
# -- PARAMETERS -------------------------------------------------------
params_file = "./package/preomop/genomop_measurement_params.yaml"

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

# %%
# == Load vocabularies =================================================
print("Loading vocabularies...")
concept_df = pd.read_parquet(data_dir / vocab_dir / "CONCEPT.parquet").infer_objects()
concept_rel_df = pd.read_parquet(
    data_dir / vocab_dir / "CONCEPT_RELATIONSHIP.parquet"
).infer_objects()
# Load CLC database
clc_df = pd.read_parquet(data_dir / vocab_dir / "CLC.parquet")

# %%
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
    df = gen.map_source_value(
        df,
        vocabulary_config[f],
        concept_df,
        "measurement_source_value",
        "vocabulary_id",
        "measurement_source_concept_id",
    )
    # Add to final dataframe
    df_complete.append(df)

# -- Finish off joint dataframe ---------------------------------------
df = pd.concat(df_complete, axis=0)

# %%
# == Mapping Numeric values ===========================================
print("Converting values ...")
# Create the omop field for the numeric values
df["value_as_number"] = pd.to_numeric(df["value_source_value"])

# %%
# == Mapping units ====================================================
print("Mapping units...")
# Retrieve unit_source_value from clc_df vocabulary
map_dict = gen.create_vocabulary_mapping(
    df, clc_df, "measurement_source_value", "NombreConvCLC", "UnidadConv"
)
df["unit_source_value"] = df["measurement_source_value"].map(map_dict)
# Map source_concept_id using UCUM and SNOMED
df = mea.map_measurement_units(df, concept_df)

# %%
# == Mapping to standard concept_id ===================================
print("Mapping to standard concepts...")
df = gen.map_source_concept_id(
    df, concept_rel_df, "measurement_source_concept_id", "measurement_concept_id"
)
df = gen.map_source_concept_id(
    df, concept_rel_df, "unit_source_concept_id", "unit_concept_id"
)

# %%
# == Check for codes that were not mapped =============================
test_list = ["measurement", "unit"]

for col in test_list:
    # Check for unmapped values
    unmapped_values = gen.find_unmapped_values(
        df, f"{col}_source_value", f"{col}_concept_id"
    )
    # Apply mapping if needed
    if len(unmapped_values) > 0:
        print(f" No concept ID found for source values: {[*unmapped_values]}")
        print("  Applying custom concepts...")
        df = gen.update_concept_mappings(
            df,
            f"{col}_source_value",
            f"{col}_source_concept_id",
            f"{col}_concept_id",
            params_data[f"unmapped_{col}"],
        )

        unmapped_values = gen.find_unmapped_values(
            df, f"{col}_source_value", f"{col}_concept_id"
        )

# %%
# == Retrieve visit_occurrence_id =====================================
print("Looking for visit_occurrence_id...")
# Create the primary key
df["measurement_id"] = pa.array(range(len(df)))

# Look for visit_occurrence_id
df_visit_occurrence = pd.read_parquet(data_dir / visit_dir / "VISIT_OCCURRENCE.parquet")
df = gen.find_visit_occurence_id(
    df, ["person_id", "start_date", "measurement_id"], df_visit_occurrence, verbose=2
)

# %%
# == Final touches ====================================================
print("Formatting to OMOP...")
# Convert to pyarrow table
table = pa.Table.from_pandas(df, preserve_index=False)
# Rename existing columns
table = gen.rename_table_columns(
    table,
    {
        "start_date": "measurement_date",
        "type_concept": "measurement_type_concept_id",
    },
)

# Fill, reorder and cast to schema
table = gen.fill_omop_table(table, omop_schemas["MEASUREMENT"])
table = gen.reorder_omop_table(table, omop_schemas["MEASUREMENT"])
table = table.cast(omop_schemas["MEASUREMENT"])

# %%
# Save!
print("Saving to parquet...")
parquet.write_table(table, data_dir / output_dir / "MEASUREMENT.parquet")
print("Done.")

# %%
