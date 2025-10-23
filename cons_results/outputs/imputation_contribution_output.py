import numpy as np
import pandas as pd

from cons_results.utilities.utils import get_versioned_filename


def get_imputation_contribution_output(additional_outputs_df: pd.DataFrame, **config):
    """
    Creates imputation contribution output

    Parameters
    ----------
    df : pd.DataFrame
        Outliering output dataframe with relevant variables for output
    config : dict
        A dictionary containing configuration parameters.
    Returns
    -------
    pd.DataFrame
        Output dataframe with returned and imputed combinations for each
        SIC-questioncode grouping.

    """

    df = additional_outputs_df.copy()

    if config["imputation_contribution_periods"]:
        df = df[df[config["period"]].isin(config["imputation_contribution_periods"])]

    question_no = config["question_no"]

    df = df[df[question_no].isin(config["components_questions"])]

    df["returned_or_imputed"] = np.where(
        df["imputation_flags_adjustedresponse"] == "r",
        "returned",
        np.where(
            df["imputation_flags_adjustedresponse"].isin(
                ["fir", "bir", "mc", "fimc", "fic", "c"]
            ),
            "imputed",
            None,  # Use None for missing values instead of np.nan
        ),
    )
    # Convert adjustedresponse from thousands to millions
    df["adjustedresponse"] = df["adjustedresponse"] / 1000

    df["curr_grossed_value"] = (
        df["adjustedresponse"]
        * df["design_weight"]
        * df["outlier_weight"]
        * df["calibration_factor"]
    )

    output_df = (
        pd.pivot_table(
            df,
            values="curr_grossed_value",
            index=[question_no],
            columns="returned_or_imputed",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)[[question_no, "returned", "imputed"]]
    )

    # Totals for all questioncodes are given by Q290
    total_returned = output_df["returned"].sum()
    total_imputed = output_df["imputed"].sum()
    q290_row = pd.DataFrame(
        {question_no: [290], "returned": [total_returned], "imputed": [total_imputed]}
    )
    output_df = pd.concat([output_df, q290_row], axis=0, ignore_index=True)

    output_df["returned"] = output_df["returned"].fillna(0)
    output_df["imputed"] = output_df["imputed"].fillna(0)
    output_df.insert(2, "total", output_df["imputed"] + output_df["returned"])

    # Sort by specified question_no order
    question_order = [290, 201, 211, 221, 231, 242, 241, 202, 212, 222, 232, 243]
    output_df[question_no] = pd.Categorical(
        output_df[question_no], question_order, ordered=True
    )
    output_df.sort_values(question_no, inplace=True)
    output_df[question_no] = output_df[question_no].astype(int)

    # setting filename based on periods
    if config["imputation_contribution_periods"]:
        periods_strings = [
            str(period) for period in config["imputation_contribution_periods"]
        ]
        periods_prefix = "_".join(periods_strings)
        filename_prefix = f"imputation_contribution_output_{periods_prefix}"

        filename = get_versioned_filename(filename_prefix, config)

        return (output_df.reset_index(drop=True), filename)

    else:
        return output_df.reset_index(drop=True)
