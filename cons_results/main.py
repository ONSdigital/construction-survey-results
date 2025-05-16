import warnings

from mbs_results.utilities.inputs import load_config

from cons_results.staging.stage_dataframe import stage_dataframe

from cons_results.imputation.impute import impute

# import imputation
# import post-imputation
# import estimation
# import outlier detection

# import staging validation checks
# import imputation checks
# import estimation checks


def run_pipeline(config_user_dict=None):
    """This is the main function that runs the pipeline"""

    config = load_config(config_user_dict)

    warnings.warn("This is a placeholder for config validation, not yet implemented")

    df, contributors, manual_constructions, filter_df = stage_dataframe(config)
    df.to_csv("tests/data/outputs/staged_data.csv")

    warnings.warn(
        "This is a placeholder for staging validation checksng,  not yet implemented"
    )

    df = impute(df, manual_constructions, config, contributors, filter_df)

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
