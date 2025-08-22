import argparse
import sys

sys.path.append(
    "./external/bps_to_omop/"
)  # This is needed or else some other functions in bps_to_omop wont work
from external.bps_to_omop.bps_to_omop import condition_occurrence
from external.bps_to_omop.bps_to_omop.utils import extract

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates the OMOP-CDM CONDITION_OCCURRENCE table from BPS data."
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
        help="Condition Occurrence table parameters file. See guide.",
        default="./src/genomop_condition_occurrence_params.yaml",
    )
    args = parser.parse_args()

    # -- Load parameters ----------------------------------------------
    print("Reading parameters...")

    # -- Load yaml file and related info
    params_gen = extract.read_yaml_params(args.general_parameters_file)
    params_cond = extract.read_yaml_params(args.measurement_parameters_file)

    data_dir = params_gen["repo_data_dir"]

    # Create output
    condition_occurrence.process_condition_occurrence_table(data_dir, params_cond)
