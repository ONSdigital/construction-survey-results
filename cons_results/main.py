from mbs_results.estimation.estimate import estimate
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.outputs import save_df
from mbs_results.utilities.utils import (
    export_run_id,
    generate_schemas,
    get_datetime_now_as_int,
)
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)
from mbs_results.utilities.setup_logger import setup_logger, upload_logger_file_to_s3

from cons_results.imputation.impute import impute
from cons_results.outlier_detection.detect_outlier import detect_outlier
from cons_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from cons_results.staging.stage_dataframe import stage_dataframe


def run_pipeline(config_user_dict=None):
    """This is the main function that runs the pipeline"""

    # Setup run id
    run_id = (
        config_user_dict.get("run_id")
        if config_user_dict
        else get_datetime_now_as_int()
    )

    # Initialise the logger at the sart of the pipeline
    logger_file_path = f"cons_results_{str(run_id)}.log"
    logger = setup_logger(logger_file_path=logger_file_path)
    logger.info(f"Cons Pipeline Started: Log file: {logger_file_path}")

    config = load_config("config_user.json", config_user_dict)
    config["run_id"] = run_id
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

    export_run_id(config["run_id"])

    upload_logger_file_to_s3(config, logger_file_path)


if __name__ == "__main__":
    run_pipeline()
