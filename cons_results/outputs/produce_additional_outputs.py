import logging

import pandas as pd
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename

from cons_results.outputs.cord_output import get_cord_output
from cons_results.outputs.imputation_contribution_output import (
    get_imputation_contribution_output,
)
from cons_results.outputs.imputes_and_constructed_output import (
    get_imputes_and_constructed_output,
)
from cons_results.outputs.qa_output import produce_qa_output
from cons_results.outputs.quarterly_by_sizeband_output import (
    get_quarterly_by_sizeband_output,
)
from cons_results.outputs.r_m_output import produce_r_m_output

logger = logging.getLogger(__name__)


def produce_additional_outputs(
    additional_outputs_df: pd.DataFrame,
    qa_outputs: bool,
    optional_outputs: bool,
    config: dict,
):

    additional_outputs = get_additional_outputs(
        config,
        {
            "imputes_and_constructed_output": get_imputes_and_constructed_output,
            "quarterly_by_sizeband_output": get_quarterly_by_sizeband_output,
            "produce_qa_output": produce_qa_output,
            "imputation_contribution_output": get_imputation_contribution_output,
            "cord_output": get_cord_output,
            "r_m_output": produce_r_m_output,
        },
        additional_outputs_df,
        qa_outputs,
        optional_outputs,
    )

    if additional_outputs is None:
        return

    for output, (df, name) in additional_outputs.items():
        if name:
            filename = name
        else:
            filename = get_versioned_filename(output, config["run_id"])

        if df is not None:

            header = (
                False
                if output
                in [
                    "quarterly_by_sizeband_output",
                    "quarterly_extracts",
                    "produce_qa_output",
                ]
                else True
            )

            if isinstance(df, dict):
                # if the output is a dictionary (e.g. from produce_qa_output),
                # we need to save each DataFrame in the dictionary

                for name, df in df.items():
                    name = str(name).lower().replace(" ", "_")
                    output_filename = f"{config['output_path']}{name}_{filename}"
                    write_csv_wrapper(
                        df,
                        output_filename,
                        config["platform"],
                        config["bucket"],
                        index=False,
                        header=header,
                    )

                    logger.info(output_filename + " saved")

            elif output == "imputes_and_constructed_output":
                # This needs to output to different location for s3 replication
                write_csv_wrapper(
                    df,
                    config["output_path_replication"] + filename,
                    config["platform"],
                    config["bucket"],
                    index=False,
                )
                logger.info(config["output_path_replication"] + filename + " saved")

            else:
                write_csv_wrapper(
                    df,
                    config["output_path"] + filename,
                    config["platform"],
                    config["bucket"],
                    index=False,
                    header=header,
                )
                logger.info(config["output_path"] + filename + " saved")


def get_additional_outputs_df(
    df: pd.DataFrame, unprocessed_data: pd.DataFrame, config: dict
):
    """
    Creating dataframe that contains all variables needed for producing additional
    outputs.
    Create adjustedresponse_pounds_thousands column based on question numbers in config.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe output from the outliering stage of the pipeline
    unprocessed_data : pd.DataFrame
        Dataframe with all question codes which weren't processed through
        mbs methods like qcode 11, 12, 146.
    config : dict
        main pipeline configuration.

    Returns
    -------
    pd.DataFrame

    """
    question_col = config.get("question_no")
    target = config.get("target")

    # below needed for mandotary and optional outputs
    final_cols = [
        config["reference"],
        config["period"],
        config["sic"],
        "classification",
        config["cell_number"],
        config["auxiliary"],
        "froempment",
        "formtype",
        question_col,
        config["status"],
        "design_weight",
        config["calibration_factor"],
        "outlier_weight",
        f"imputation_flags_{target}",
        "imputation_class",
        f"f_link_{target}",
        f"default_link_f_match_{target}",
        f"b_link_{target}",
        f"default_link_b_match_{target}",
        "construction_link",
        "flag_construction_matches_count",
        "default_link_flag_construction_matches",
        target,
        "adjustedresponse_pounds_thousands",
        "response",
        "status",
        "runame1",
        "entname1",
        "region",
        "winsorised_value",
        "290_flag",
        "derived_zeros",
    ]
    if not config["filter"]:
        count_variables = [f"b_match_{target}_count", f"f_match_{target}_count"]
    else:
        count_variables = [
            f"b_match_filtered_{target}_count",
            f"f_match_filtered_{target}_count",
        ]

    final_cols += count_variables

    # converting cell_number to int
    # needed for outputs that use cell_number for sizebands

    df = df.astype({"classification": str, config["cell_number"]: int})

    unprocessed_data["period"] = (
        unprocessed_data["period"].dt.strftime("%Y%m").astype("int")
    )

    df = pd.concat([df, unprocessed_data])

    df = df[final_cols]

    df.reset_index(drop=True, inplace=True)

    return df
