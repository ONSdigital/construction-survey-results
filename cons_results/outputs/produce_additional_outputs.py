import pandas as pd
from mbs_results import logger
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.outputs.scottish_welsh_gov_outputs import generate_devolved_outputs
from mbs_results.utilities.inputs import read_csv_wrapper
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.pounds_thousands import create_pounds_thousands_column
from mbs_results.utilities.utils import convert_column_to_datetime

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
from cons_results.outputs.standard_errors import create_standard_errors
from cons_results.utilities.utils import get_versioned_filename


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
            "quarterly_extracts": produce_quarterly_extracts,
            "devolved_outputs": generate_devolved_outputs,
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
            filename = get_versioned_filename(output, config)

        if df is not None:

            header = (
                False
                if output in ["quarterly_by_sizeband_output", "quarterly_extracts"]
                else True
            )

            if isinstance(df, dict):
                # if the output is a dictionary (e.g. from generate_devolved_outputs),
                # we need to save each DataFrame in the dictionary
                for nation, df in df.items():
                    nation_name = nation.lower().replace(" ", "_")
                    nation_filename = f"{config['output_path']}{nation_name}_{filename}"
                    write_csv_wrapper(
                        df,
                        nation_filename,
                        config["platform"],
                        config["bucket"],
                        index=False,
                    )

                    logger.info(nation_filename + " saved")

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


def produce_quarterly_extracts(
    additional_outputs_df: pd.DataFrame,
    **config,
):
    """
    Function to produce the aggregated adjusted responses for questions
    202, 212, 222, 232 and 243 (repair and maintenance) grouped by quarter
    and region

    Parameters
    ----------
    config : dict
        Dictionary containing configuration parameters
    additional_outputs_df : pd.DataFrame
        DataFrame containing additional outputs
    """

    # todo: the additional outputs df has 2 region columns
    # todo: so we need to deal with this merge issue at some point.
    # todo: For the time being we are using region_y renamed to region

    if set(["region_x", "region_y"]).issubset(additional_outputs_df.columns):
        additional_outputs_df = additional_outputs_df.rename(
            columns={"region_y": "region"}
        ).drop(columns=["region_x"])

    # Create weighted adjusted response
    additional_outputs_df["weighted adjusted value"] = (
        additional_outputs_df[config["target"]]
        * additional_outputs_df["design_weight"]
        * additional_outputs_df["outlier_weight"]
        * additional_outputs_df["calibration_factor"]
    )

    # Select columns from additional outputs DataFrame
    q_extracts_df = additional_outputs_df[
        [
            config["period"],
            config["region"],
            config["question_no"],
            "weighted adjusted value",
        ]
    ].copy()

    # Create quarter column
    q_extracts_df[config["period"]] = convert_column_to_datetime(
        q_extracts_df[config["period"]]
    )

    q_extracts_df["quarter"] = pd.PeriodIndex(q_extracts_df[config["period"]], freq="Q")

    if config["r_and_m_quarter"] is None:
        chosen_quarter = q_extracts_df["quarter"].max()
    else:
        chosen_quarter = pd.Period(config["r_and_m_quarter"])

    # Filter DataFrame
    q_extracts_df = q_extracts_df[
        q_extracts_df[config["question_no"]].isin([202, 212, 222, 232, 243])
    ]
    q_extracts_df = q_extracts_df[q_extracts_df["quarter"] == chosen_quarter]

    # Map region names onto DataFrame
    region_mapping_df = read_csv_wrapper(
        config["region_mapping_path"], config["platform"], config["bucket"]
    )

    q_extracts_df = q_extracts_df.merge(
        region_mapping_df, left_on=config["region"], right_on="region_code"
    )

    # Produce output table
    extracts_table = (
        q_extracts_df.groupby(["quarter", "region_name", config["question_no"]])
        .sum("weighted adjusted value")
        .reset_index()
    )

    # Sort by custom ordering of regions
    custom_region_order = {
        "North East": 1,
        "Yorkshire and The Humber": 2,
        "East Midlands": 3,
        "East of England": 4,
        "London": 5,
        "South East": 6,
        "South West": 7,
        "Wales": 8,
        "West Midlands": 9,
        "North West": 10,
        "Scotland": 11,
    }

    extracts_table = (
        extracts_table.pivot(
            index=["quarter", "region_name"],
            columns=config["question_no"],
            values="weighted adjusted value",
        )
        .sort_values(
            by=["region_name"],
            key=lambda x: x.map(custom_region_order),
        )
        .reset_index()
    )

    file_suffix = str(chosen_quarter)

    filename = f"r_and_m_regional_extracts_{file_suffix}.csv"

    print(config["output_path"] + filename + " saved")

    return extracts_table, filename


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
