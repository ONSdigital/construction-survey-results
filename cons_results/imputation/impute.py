import pandas as pd
from mbs_results.imputation.ratio_of_means import ratio_of_means

from cons_results.imputation.post_imputation import (
    create_q290,
    derive_q290,
    rescale_290_case,
    validate_q290,
)


def impute(
    df: pd.DataFrame,
    config: dict,
    manual_constructions: pd.DataFrame = None,
    filter_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    wrapper function to apply imputation to the given df

    Parameters
    ----------
    df : pd.DataFrame
        df with both contributors and responses from snapshot
    manual_constructions : pd.DataFrame
        manual_constructions : pd.DataFrame
    config : dict
        config file containing column names and manual construction path
    filter_df : pd.DataFrame
        filter_df df from the staging module


    Returns
    -------
    pd.DataFrame
        post imputation df, values have been derived and constrained following
        imputation
    """

    df = df.groupby(config["question_no"]).apply(
        lambda df_q_code: ratio_of_means(
            df=df_q_code,
            manual_constructions=manual_constructions,
            reference=config["reference"],
            target=config["target"],
            period=config["period"],
            current_period=config["current_period"],
            revision_window=config["revision_window"],
            question_no=config["question_no"],
            strata="imputation_class",
            auxiliary=config["auxiliary_converted"],
            filters=filter_df,
        )
    )

    df = df.reset_index(drop=True)  # remove groupby leftovers

    df = df[~df["is_backdata"]]  # remove backdata
    df.drop(columns=["is_backdata"], inplace=True)

    df = rescale_290_case(
        df,
        config["period"],
        config["reference"],
        config["question_no"],
        config["target"],
    )

    df = create_q290(
        df,
        config,
        config["reference"],
        config["period"],
        config["question_no"],
        config["target"],
        config["imputation_marker_col"],
    )

    df = derive_q290(
        df,
        config["question_no"],
        config["imputation_marker_col"],
        config["period"],
        config["reference"],
        config["target"],
    )

    validate_q290(df, config)

    df[config["period"]] = df[config["period"]].dt.strftime("%Y%m").astype("int")

    return df
