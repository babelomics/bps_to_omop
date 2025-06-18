import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

import bps_to_omop.extract as ext
import bps_to_omop.general as gen
import bps_to_omop.measurement as mea
from bps_to_omop.omop_schemas import omop_schemas

# %%
# == Final touches ====================================================


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--parameters_file",
        type=str,
        help="Parameters file. See guide.",
        default="./src/genomop_measurement_params.yaml",
    )
    args = parser.parse_args()
    process_measurement_table(args.parameters_file)
