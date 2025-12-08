import os

import boto3
import pandas as pd
import raz_client
from mbs_results import logger
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.pounds_thousands import create_pounds_thousands_column
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
from cons_results.outputs.standard_errors import create_standard_errors


# flake8: noqa: C901
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
            "standard_errors": create_standard_errors,
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
                if output in ["quarterly_by_sizeband_output", "quarterly_extracts"]
                else True
            )

            if isinstance(df, dict):
                # if the output is a dictionary (e.g. from generate_devolved_outputs),
                # we need to save each DataFrame in the dictionary

                if output == "produce_qa_output":
                    run_id = config["run_id"]

                    filename = f"qa_output_{run_id}.xlsx"

                    # todo: Add read_excel_wrapper to MBS

                    # if platform == "network", save locally using output path
                    if config["platform"] == "network":
                        with pd.ExcelWriter(config["output_path"] + filename) as writer:
                            for period, dataframe in df.items():
                                dataframe.to_excel(
                                    writer, sheet_name=f"{period}", startcol=-1
                                )

                    # if platform == "s3", save to working directory first
                    # then move to s3
                    if config["platform"] == "s3":
                        client = boto3.client("s3")
                        raz_client.configure_ranger_raz(
                            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
                        )

                        with pd.ExcelWriter(filename) as writer:
                            for period, dataframe in df.items():
                                dataframe.to_excel(
                                    writer, sheet_name=f"{period}", startcol=-1
                                )

                        client.upload_file(
                            filename, config["bucket"], config["output_path"] + filename
                        )

                        # deleting from local storage after uploading to S3
                        if os.path.exists(filename):
                            os.remove(filename)

            else:

                write_csv_wrapper(
                    df,
                    config["output_path"] + filename,
                    config["platform"],
                    config["bucket"],
                    index=False,
                    header=header,
                )

            print(config["output_path"] + filename + " saved")


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
    questions_to_apply = config.get("pounds_thousands_questions")
    question_col = config.get("question_no")
    dest_col = config.get("pound_thousand_col")
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
        "response",
        "status",
        "runame1",
        "entname1",
        "region",
        "adjustedresponse_pounds_thousands",
        "winsorised_value",
    ]
    if not config["filter"]:
        count_variables = [f"b_match_{target}_count", f"f_match_{target}_count"]
    else:
        count_variables = [
            f"b_match_filtered_{target}_count",
            f"f_match_filtered_{target}_count",
        ]

    final_cols += count_variables

    df = create_pounds_thousands_column(
        df,
        question_col=question_col,
        source_col=target,
        dest_col=dest_col,
        questions_to_apply=questions_to_apply,
        ensure_at_end=True,
    )

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
