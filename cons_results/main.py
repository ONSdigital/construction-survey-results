from mbs_results.estimation.estimate import estimate
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.utils import generate_schemas
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)

from cons_results.imputation.impute import impute
from cons_results.outlier_detection.detect_outlier import detect_outlier
from cons_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from cons_results.staging.stage_dataframe import stage_dataframe
from cons_results.utilities.utils import save_df


def run_pipeline(config_user_dict=None):
    """This is the main function that runs the pipeline"""

    config = load_config("config_user.json", config_user_dict)
    validate_config(config)

    df, unprocessed_data, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(df, config)

    df = impute(df, config, manual_constructions, filter_df)
    validate_imputation(df, config)
    save_df(df, "imputation", config, config["debug_mode"])

    df = estimate(df=df, method="separate", convert_NI_GB_cells=False, config=config)
    validate_estimation(df, config)
    save_df(df, "estimation_output", config, config["debug_mode"])

    df = detect_outlier(df, config)
    validate_outlier_detection(df, config)
    save_df(df, "outlier_output", config, config["debug_mode"])

    df = get_additional_outputs_df(df, unprocessed_data, config)
    save_df(df, "cons_results", config)

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=True, optional_outputs=False, config=config
    )

    generate_schemas(config)


if __name__ == "__main__":
    run_pipeline()
