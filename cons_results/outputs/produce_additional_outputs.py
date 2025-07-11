import pandas as pd
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.utilities.utils import convert_column_to_datetime

from cons_results.outputs.imputes_and_constructed_output import (
    get_imputes_and_constructed_output,
)
from cons_results.outputs.qa_output import produce_qa_output
from cons_results.utilities.utils import get_versioned_filename


def produce_additional_outputs(config: dict, additional_outputs_df: pd.DataFrame):
    additional_outputs = get_additional_outputs(
        config,
        {
            "imputes_and_constructed_output": get_imputes_and_constructed_output,
            "produce_qa_output": produce_qa_output,
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
        df.to_csv(config["output_path"] + filename, index=False)
        print(config["output_path"] + filename + " saved")


def produce_quarterly_extracts(config: dict, additional_outputs_df: pd.DataFrame):
    """
    """

    # Todo: if config["gen_quarterly_extracts"] is True:

    q_extracts_df = additional_outputs_df[
        ["period", "region_x", "questioncode", "adjustedresponse"]
    ]

    q_extracts_df["period"] = convert_column_to_datetime(q_extracts_df["period"])
    q_extracts_df["quarter"] = pd.PeriodIndex(q_extracts_df["period"], freq="Q")

    # Todo: Filter on questioncode (no test data for this!)

    extracts_table = q_extracts_df.groupby(["quarter", "region_x", "questioncode"]).sum(
        "adjustedresponse"
    )

    # Todo: rotate table

    print(extracts_table)

    return extracts_table
