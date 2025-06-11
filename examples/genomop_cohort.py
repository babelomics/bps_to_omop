import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from src.utils import read_params

# -- TO REMOVE --
os.chdir("..")
print(os.getcwd())
params_file = "./src/genomop_cohort_params.yaml"
# ---------------

sys.path.append(
    "./external/bps_to_omop"
)  # This is needed or else some other functions in bps_to_omop wont work
import external.bps_to_omop.bps_to_omop.general as gen
from external.bps_to_omop.bps_to_omop.omop_schemas import omop_schemas


def process_cohort_table(params_file: str):

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = read_params("./params.yaml")
    params_data = read_params(params_file)

    data_dir = Path(params_gen["repo_data_dir"])
    input_dir = params_data["input_dir"]
    output_dir = params_data["output_dir"]
    os.makedirs(data_dir / output_dir, exist_ok=True)

    # -- Preprocessing ------------------------------------------------
    # Iterate over the cohorts
    cohort_tables = {"COHORT": [], "COHORT_DEFINITION": []}
    for cohort_name, cohort_params in params_data["cohorts"].items():

        # Retrieve data
        if not cohort_params.get("sociodemo_file", False):
            print(f"Sociodemo file not provided for {cohort_name}, skipping...")
            continue
        df = pd.read_parquet(data_dir / input_dir / cohort_params["sociodemo_file"])

        # -- Generate the record of the COHORT_DEFINITION table
        schema_fields = [field.name for field in omop_schemas["COHORT_DEFINITION"]]
        cohort_def_row = {}
        for field in cohort_params:
            if field in schema_fields:
                cohort_def_row[field] = cohort_params[field]
        cohort_tables["COHORT_DEFINITION"].append(cohort_def_row)

        # -- Generate the records of the COHORT table
        # preload the static fields of COHORT table
        cohort_definition_id = cohort_params["cohort_definition_id"]
        cohort_start_date = pd.to_datetime(cohort_params["cohort_start_date"])
        cohort_end_date = pd.to_datetime(cohort_params["cohort_end_date"])

        # Create a record for each patient in the sociodemo file
        for person in df["person_id"].unique():
            cohort_row = {
                "cohort_definition_id": cohort_definition_id,
                "subject_id": person,
                "cohort_start_date": cohort_start_date,
                "cohort_end_date": cohort_end_date,
            }
            cohort_tables["COHORT"].append(cohort_row)

    # -- Table generation ---------------------------------------------
    for table in cohort_tables:
        cohort_tables[table] = pa.Table.from_pylist(
            cohort_tables[table], schema=omop_schemas[table]
        )

        # Format to schema
        cohort_tables[table] = gen.format_table(
            cohort_tables[table], omop_schemas[table]
        )

        # Save
        parquet.write_table(
            cohort_tables[table], data_dir / output_dir / f"{table}.parquet"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM COHORT and COHORT_DEFINITION tables from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./src/genomop_cohort_params.yaml",
    )
    args = parser.parse_args()
    process_cohort_table(args.parameters_file)

# %%
