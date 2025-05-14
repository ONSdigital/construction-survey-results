def create_q290(df: pd.DataFrame, 
                contributors: pd.DataFrame,
                reference: str,
                period: str,
                question_no: str) -> pd.DataFrame:
  """Parameters
    ----------
    df : pd.Dataframe
        The main construction dataframe following the staging module.
    contributors : pd.DataFrame
        Dataframe containing contributors.
    reference : str
        Column name containing reference variable, must exist in both
        responses and contributors.
    period : str
        Column name containing period variable, must exist in both
        responses and contributors.
    question_col : str
        Column name containing question_col variable, must exist in responses.

    Returns
    -------
    df
      A dataframe with rows added for questioncode 290 where these were missing."""

    missing_290 = (create_df.
                   groupby([period, reference]).
                   filter(lambda x: 290 not in x[question_no].values)[[period, reference]])
    
    missing_290.drop_duplicates(inplace=True)

    missing_290 = missing_290.assign(question_no=290, adjustedvalue=None, imputation_flag="d")
    
    missing_290.set_index([period, reference], inplace=True)
    contributors = contributors.set_index([period, reference])

    missing_290 = pd.concat([missing_290, contributors.loc[missing_290.index]], axis=1)
    missing_290.reset_index(inplace=True)
    
    df = pd.concat([df, missing_290]).reset_index(drop=True)

    return df
  
def derive_q290(df: pd.DataFrame,
                question_no: str,
                imputation_flag: str,
                period: str,
                reference: str,
                adjustedvalue: str) -> pd.DataFrame:
    imputed_mask = (df[question_no] != 290) & (df[imputation_flag] != "r")
    
    imputed_sums = (
        df[imputed_mask]
        .groupby([period, reference])[adjustedvalue]
        .sum()
        .reset_index()
        .rename(columns={adjustedvalue: "imputed_sum"})
    )
    
    df = df.merge(imputed_sums, on=[period, reference], how="left")
    
    q290_mask = df[question_no] == 290
    df["imputed_sum"].fillna(df[adjustedvalue], inplace=True) 
    df.loc[q290_mask, adjustedvalue] = df.loc[q290_mask, "imputed_sum"]

    df.loc[(df[question_no] == 290) & (df[imputation_flag] != "r"), imputation_flag] = "d"

    df = df.drop(columns=["imputed_sum"])
    
    return df