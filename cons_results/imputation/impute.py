from warnings import warn

import pandas as pd
from mbs_results.imputation.ratio_of_means import ratio_of_means

from cons_results.imputation.post_imputation import post_imputation_processing


def impute(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    main wrapper for imputation method on construction survey

    Parameters
    ----------
    df : pd.DataFrame
        staged dataframe
    config : dict
        config dictionary

    Returns
    -------
    pd.DataFrame
        post imputation dataframe
    """
    warn.warn("This method is not fully developed and acting as a placeholder")

    pre_impute_dataframe = df.copy()
    manual_constructions = None

    post_impute = pre_impute_dataframe.groupby(config["question_no"]).apply(
        lambda df: ratio_of_means(
            df=df,
            manual_constructions=manual_constructions,
            reference=config["reference"],
            target=config["target"],
            period=config["period"],
            strata="imputation_class",
            auxiliary=config["auxiliary"],
        )
    )

    post_impute = post_imputation_processing(df, **config)
    return post_impute
