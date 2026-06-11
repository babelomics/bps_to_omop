# %%
import argparse
import sys

sys.path.append(
    "./external/bps_to_omop/"
)  # This is needed or else some other functions in bps_to_omop wont work
from external.bps_to_omop.bps_to_omop import observation_period
from external.bps_to_omop.bps_to_omop.utils import extract

# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM OBSERVATION_PERIOD table from BPS data."
    )
    parser.add_argument(
        "--general_parameters_file",
        type=str,
        help="General parameters file. See guide.",
        default="./params.yaml",
    )
    parser.add_argument(
        "--obs_parameters_file",
        type=str,
        help="Parameters file to generate OBSERVATION_PERIOD table. See guide.",
        default="./src/genomop_observation_period_params.yaml",
    )
    args = parser.parse_args()

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = extract.read_yaml_params(args.general_parameters_file)
    params_obs = extract.read_yaml_params(args.obs_parameters_file)

    data_dir = params_gen["repo_data_dir"]

    # Create output
    observation_period.process_observation_period_table(data_dir, params_obs)
