import pandas as pd
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.utilities.outputs import write_csv_wrapper
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


def produce_additional_outputs(config: dict, additional_outputs_df: pd.DataFrame):
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
        },
        additional_outputs_df,
    )

    if additional_outputs is None:
        return

    for output, (df, name) in additional_outputs.items():
        if name:
            filename = name
        else:
            filename = get_versioned_filename(output, config)

        if df is not None:
            df.to_csv(config["output_path"] + filename, index=False)
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
    ]

    # Create quarter column
    q_extracts_df[config["period"]] = convert_column_to_datetime(
        q_extracts_df[config["period"]]
    )
    q_extracts_df["quarter"] = pd.PeriodIndex(q_extracts_df[config["period"]], freq="Q")

    if config["quarterly_extract"] is None:
        chosen_quarter = q_extracts_df["quarter"].max()
    else:
        chosen_quarter = pd.Period(config["quarterly_extract"])

    # Filter DataFrame
    q_extracts_df = q_extracts_df[
        q_extracts_df[config["question_no"]].isin([202, 212, 222, 232, 243])
    ]
    q_extracts_df = q_extracts_df[q_extracts_df["quarter"] == chosen_quarter]

    # Map region names onto DataFrame
    region_mapping_df = pd.read_csv(config["region_mapping_path"])

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

    extracts_table = extracts_table.pivot(
        index=["quarter", "region_name"],
        columns=config["question_no"],
        values="weighted adjusted value",
    ).sort_values(
        by=["region_name"],
        key=lambda x: x.map(custom_region_order),
    )

    filename = f"r_and_m_regional_extracts_{chosen_quarter}.csv"

    write_csv_wrapper(
        extracts_table,
        config["output_path"] + filename,
        config["platform"],
        config["bucket"],
        header=False,
    )

    print(config["output_path"] + filename + " saved")

    return extracts_table, filename
