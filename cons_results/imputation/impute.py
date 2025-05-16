import pandas as pd

from cons_results.imputation.derive_questions import (
  create_q290,
  derive_q290,
)

from mbs_results.imputation.ratio_of_means import ratio_of_means

def impute(
    dataframe: pd.DataFrame, 
    manual_constructions: pd.DataFrame, 
    config: dict, 
    contributors: pd.DataFrame,
    filter_df=None,
) -> pd.DataFrame:
    """
    wrapper function to apply imputation to the given dataframe

    Parameters
    ----------
    dataframe : pd.DataFrame
        dataframe with both contributors and responses from snapshot
    dataframe : pd.DataFrame
        manual constructions dataframe
    config : dict
        config file containing column names and manual construction path
    filter_df : pd.DataFrame
        filter_df dataframe from the staging module
        

    Returns
    -------
    pd.DataFrame
        post imputation dataframe, values have been derived and constrained following
        imputation
    """
    dataframe = dataframe.groupby(config["question_no"]).apply(
        lambda df: ratio_of_means(
            df=df,
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

    dataframe = create_q290(dataframe, 
                            contributors, 
                            config["reference"], 
                            config["period"], 
                            config["question_no"])
    
    dataframe = derive_q290(dataframe, 
                        config["question_no"],
                        config["imputation_flag"], 
                        config["period"], 
                        config["reference"], 
                        config["adjustedvalue"])
    
    dataframe["period"] = dataframe["period"].dt.strftime("%Y%m").astype("int")
    dataframe = dataframe.reset_index(drop=True)  # remove groupby leftovers
    dataframe = dataframe[~dataframe["is_backdata"]]  # remove backdata
    dataframe.drop(columns=["is_backdata"], inplace=True)

    
    return post_constrain