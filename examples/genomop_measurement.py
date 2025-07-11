import argparse
import sys

sys.path.append("./external/bps_to_omop/")
from bps_to_omop import measurement
from bps_to_omop.utils import extract

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
        "--measurement_parameters_file",
        type=str,
        help="Measurement table parameters file. See guide.",
        default="./src/genomop_measurement_params.yaml",
    )
    args = parser.parse_args()

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = extract.read_yaml_params(args.general_parameters_file)
    params_measurement = extract.read_yaml_params(args.measurement_parameters_file)

    data_dir = params_gen["repo_data_dir"]

    # Create output
    measurement.process_measurement_table(data_dir, params_measurement)
