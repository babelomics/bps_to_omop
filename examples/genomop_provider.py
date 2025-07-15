# %%
import argparse
import sys

sys.path.append("./external/bps_to_omop/")
from external.bps_to_omop.bps_to_omop import provider
from external.bps_to_omop.bps_to_omop.utils import extract

# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM MEASUREMENT table from BPS data."
    )
    parser.add_argument(
        "--general_parameters_file",
        type=str,
        help="General parameters file. See guide.",
        default="./params.yaml",
    )
    parser.add_argument(
        "--provider_parameters_file",
        type=str,
        help="Provider table parameters file. See guide.",
        default="./src/genomop_provider_params.yaml",
    )
    args = parser.parse_args()

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = extract.read_yaml_params(args.general_parameters_file)
    params_provider = extract.read_yaml_params(args.provider_parameters_file)

    data_dir = params_gen["repo_data_dir"]

    # Create output
    provider.process_provider_table(data_dir, params_provider)
