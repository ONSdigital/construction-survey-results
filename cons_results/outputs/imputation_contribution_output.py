import itertools

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
            index=["classification", question_no],
            columns="returned_or_imputed",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)[
            ["classification", question_no, "returned", "imputed"]
        ]
    )

    # low level sics that don't match the higher level classification
    low_level_sics = [
        sic
        for sic in config["imputation_contribution_sics"]
        if sic not in config["imputation_contribution_classification"]
    ]

    # creating a low level sic for each question code
    low_level_sic_qc = set(
        itertools.product(low_level_sics, config["components_questions"])
    )

    low_level_sic_df = pd.DataFrame(
        low_level_sic_qc, columns=["classification", question_no]
    )

    output_df = pd.concat([output_df, low_level_sic_df], axis=0).fillna(0)

    output_df.insert(2, "total", output_df["imputed"] + output_df["returned"])
    output_df.sort_values([question_no, "classification"], inplace=True)

    # renaming classification to frosic to match the extract
    # classification contains the higher level frosic code
    output_df.rename(columns={"classification": "frosic2007"}, inplace=True)

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
