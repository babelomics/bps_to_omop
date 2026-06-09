# %%
import argparse
import os
import sys

sys.path.append(
    "./external/bps_to_omop/"
)  # This is needed or else some other functions in bps_to_omop wont work
from external.bps_to_omop.bps_to_omop import visit
from external.bps_to_omop.bps_to_omop.utils import extract

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM VISIT_OCCURRENCE and VISIT_DETAIL tables from BPS data."
    )
    parser.add_argument(
        "--general_parameters_file",
        type=str,
        help="General parameters file. See guide.",
        default="./params.yaml",
    )
    parser.add_argument(
        "--visit_parameters_file",
        type=str,
        help="Parameters file to generate VISIT_OCCURRENCE and VISIT_DETAIL tables. See guide.",
        default="./src/genomop_visit_params.yaml",
    )
    args = parser.parse_args()

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = extract.read_yaml_params(args.general_parameters_file)
    params_visit = extract.read_yaml_params(args.visit_parameters_file)

    data_dir = params_gen["repo_data_dir"]

    # Set paralellization params, be conservative by default
    polars_max_threads = str(params_gen.get("polars_max_threads", "4"))
    n_jobs = params_gen.get("n_jobs", 2)

    # Create output
    os.environ["POLARS_MAX_THREADS"] = polars_max_threads
    visit.process_visit_table(data_dir, params_visit, n_jobs=n_jobs)
