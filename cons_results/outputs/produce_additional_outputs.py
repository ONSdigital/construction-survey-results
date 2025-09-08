import pandas as pd
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
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


def produce_quarterly_extracts(config: dict, df: pd.DataFrame):
    """
    Function to produce the aggregated adjusted responses for questions
    202, 212, 222, 232 and 243 (repair and maintenance) grouped by quarter
    and region

    Parameters
    ----------
    config : dict
        Dictionary containing configuration parameters
    df : pd.DataFrame
        Post-imputed DataFrame
    """

    if config["produce_quarterly_extracts"] is True:

        # Select columns from post-imputed DataFrame
        q_extracts_df = df[
            [
                config["period"],
                config["region"],
                config["question_no"],
                config["target"],
            ]
        ]

        # Create quarter column
        q_extracts_df[config["period"]] = convert_column_to_datetime(
            q_extracts_df[config["period"]]
        )
        q_extracts_df["quarter"] = pd.PeriodIndex(
            q_extracts_df[config["period"]], freq="Q"
        )

        latest_quarter = q_extracts_df["quarter"].max()

        # Filter DataFrame
        q_extracts_df = q_extracts_df[
            q_extracts_df[config["question_no"]].isin([202, 212, 222, 232, 243])
        ]
        q_extracts_df = q_extracts_df[q_extracts_df["quarter"] == latest_quarter]

        # Map region names onto DataFrame
        region_mapping_df = pd.read_csv(config["region_mapping_path"])

        q_extracts_df = q_extracts_df.merge(
            region_mapping_df, left_on=config["region"], right_on="region_code"
        )

        # Produce output table
        extracts_table = (
            q_extracts_df.groupby(["quarter", "region_name", config["question_no"]])
            .sum(config["target"])
            .reset_index()
        )

        extracts_table = extracts_table.pivot(
            index=["quarter", "region_name"],
            columns=config["question_no"],
            values=config["target"],
        )

        filename = f"r_and_m_regional_extracts_{latest_quarter}.csv"
        extracts_table.to_csv(config["output_path"] + filename)
        print(config["output_path"] + filename + " saved")
