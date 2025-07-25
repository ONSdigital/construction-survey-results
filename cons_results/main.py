import os
import warnings

from mbs_results.estimation.estimate import estimate
from mbs_results.outputs.produce_additional_outputs import get_additional_outputs_df
from mbs_results.utilities.inputs import load_config

from cons_results.imputation.impute import impute

# import estimation
from cons_results.outlier_detection.detect_outlier import detect_outlier
from cons_results.outputs.produce_additional_outputs import (
    produce_additional_outputs,
    produce_quarterly_extracts,
)
from cons_results.staging.stage_dataframe import stage_dataframe

# import staging validation checks
# import imputation checks
# import estimation checks


def run_pipeline(config_user_dict=None):
    """This is the main function that runs the pipeline"""

    tag_name = "imputation_outputs"

    config = load_config(config_user_dict)

    snapshot_file_name = os.path.basename(config["snapshot_file_path"]).split(".")[0]

    warnings.warn("This is a placeholder for config validation, not yet implemented")

    df, manual_constructions, filter_df = stage_dataframe(config)
    df.to_csv(f'{config["output_path"]}/{snapshot_file_name}_staging_{tag_name}.csv')

    warnings.warn(
        "This is a placeholder for staging validation checks,  not yet implemented"
    )

    df = impute(df, config, manual_constructions, filter_df)

    df.to_csv(f'{config["output_path"]}/{snapshot_file_name}_impute_{tag_name}.csv')

    warnings.warn("This is a placeholder post-imputation,  not yet implemented")

    warnings.warn(
        "This is a placeholder for imputation validation checks,  not yet implemented"
    )
    estimation_output = estimate(
        df=df, method="separate", convert_NI_GB_cells=False, config=config
    )

    estimation_output.to_csv(
        f'{config["output_path"]}/{snapshot_file_name}_estimate_{tag_name}.csv'
    )

    warnings.warn(
        "This is a placeholder for estimation validation checks,  not yet implemented"
    )

    outlier_detection_output = detect_outlier(estimation_output, config)
    outlier_detection_output.to_csv(
        f'{config["output_path"]}/{snapshot_file_name}_outlier_detection_{tag_name}.csv'
    )

    additional_outputs_df = get_additional_outputs_df(
        estimation_output, outlier_detection_output
    )

    produce_additional_outputs(config, additional_outputs_df)

    produce_quarterly_extracts(config, df)


if __name__ == "__main__":
    run_pipeline()
