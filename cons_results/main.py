import os
import warnings

# from mbs_results.outputs.produce_additional_outputs import get_additional_outputs_df
from mbs_results.utilities.inputs import load_config

from cons_results.imputation.impute import impute

# from cons_results.outputs.produce_additional_outputs import produce_additional_outputs
from cons_results.staging.stage_dataframe import stage_dataframe

# import imputation
# import post-imputation
# import estimation
# import outlier detection

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

    warnings.warn("This is a placeholder for estimation,  not yet implemented")

    warnings.warn(
        "This is a placeholder for estimation validation checks,  not yet implemented"
    )

    warnings.warn("This is a placeholder for outlier detection,  not yet implemented")

    warnings.warn("This is a placeholder for estimation,  not yet implemented")

    # commenting these out for now until estimation_output and outlier_output exist
    # additional_outputs_df = get_additional_outputs_df(
    # estimation_output, outlier_output
    # )
    # produce_additional_outputs(config, additional_outputs_df)


if __name__ == "__main__":
    run_pipeline()
