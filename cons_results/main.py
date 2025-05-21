import warnings

from mbs_results.utilities.inputs import load_config

from cons_results.imputation.impute import impute
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

    warnings.warn("This is a placeholder for config validation, not yet implemented")

    df, manual_constructions, filter_df = stage_dataframe(config)
    df.to_csv(
        f'{config["output_path"]}/{config["snapshot_file_name"]}_staging_{tag_name}.csv'
    )

    warnings.warn(
        "This is a placeholder for staging validation checks,  not yet implemented"
    )

    df = impute(df, config, manual_constructions, filter_df)
    df.to_csv(
        f'{config["output_path"]}/{config["snapshot_file_name"]}_impute_{tag_name}.csv'
    )

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


if __name__ == "__main__":
    run_pipeline()
