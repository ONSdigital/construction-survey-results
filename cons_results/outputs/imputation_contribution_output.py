import itertools

import numpy as np
import pandas as pd


def get_imputation_contribution_output(
    df: pd.DataFrame, imputation_contribution_sics: list, all_questions: list, **config
):
    """
    Creates imputation contribution output

    Parameters
    ----------
    df : pd.DataFrame
        Outliering output dataframe with relevant variables for output
    imputation_contribution_sics : list
        List containing all SICs for imputation contribution output (SICs in
        construction division)
    all_questions : list
        List containing all question numbers in survey. Used in combination with
        imputation_contribution_sics to create data with sic-questioncode groupings
        without data.

    Returns
    -------
    pd.DataFrame
        Output dataframe with returned and imputed combinations for each
        SIC-questioncode grouping.

    """

    df["returned_or_imputed"] = np.where(
        df["imputation_flags_adjustedresponse"] == "r",
        "returned",
        np.where(
            df["imputation_flags_adjustedresponse"].isin(
                ["fir", "bir", "mc", "fimc", "fic", "c"]
            ),
            "imputed",
            np.nan,
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
            index=["frosic2007", "questioncode"],
            columns="returned_or_imputed",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)[
            ["frosic2007", "questioncode", "returned", "imputed"]
        ]
    )

    # Adding in missing SIC-questioncode combinations
    output_sic_qc = set(
        output_df[["frosic2007", "questioncode"]].to_records(index=False).tolist()
    )
    all_sic_qc = set(itertools.product(imputation_contribution_sics, all_questions))
    missing_sic_qc = list(all_sic_qc - output_sic_qc)
    missing_sic_df = pd.DataFrame(
        missing_sic_qc, columns=["frosic2007", "questioncode"]
    )
    output_df = pd.concat([output_df, missing_sic_df], axis=0)

    # Totals for each SIC are given by Q290
    q290_totals = output_df.groupby("frosic2007").sum().reset_index()
    q290_totals["questioncode"] = 290

    output_df = pd.concat([output_df, q290_totals], axis=0).fillna(0)

    output_df.insert(2, "total", output_df["imputed"] + output_df["returned"])
    output_df.sort_values(["questioncode", "frosic2007"], inplace=True)

    return output_df.reset_index(drop=True)
