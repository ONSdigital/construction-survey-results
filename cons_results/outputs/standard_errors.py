def create_standard_errors(additional_outputs_df, **config):
    """
    Function to create standard errors, sample variances and coefficients of
    variation across period-SIC-cell number-question number groupings.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Dataframe with relevant variables for calculating standard errors
    **config : dict
        Configuration containing variable names for additional_outputs_df

    Returns
    -------
    pd.DataFrame
        DataFrame containing standard errors, sample variances and coefficients
        of variation

    """

    df = additional_outputs_df[
        [
            config["period"],
            config["question_no"],
            "classification",
            config["cell_number"],
            config["target"],
        ]
    ].groupby(
        [
            config["period"],
            config["question_no"],
            "classification",
            config["cell_number"],
        ]
    )

    sample_var = (
        df.var(ddof=1).reset_index().rename(columns={config["target"]: "sample_var"})
    )
    standard_error = (
        df.sem(ddof=1).reset_index().rename(columns={config["target"]: "std_error"})
    )
    variation = (
        df[config["target"]]
        .agg(lambda x: x.std(ddof=1) / x.mean() * 100)
        .reset_index()
        .rename(columns={config["target"]: "cov"})
    )

    df = sample_var.merge(
        standard_error,
        on=[
            config["period"],
            "classification",
            config["cell_number"],
            config["question_no"],
        ],
    ).merge(
        variation,
        on=[
            config["period"],
            "classification",
            config["cell_number"],
            config["question_no"],
        ],
    )
    df = df.round({"sample_var": 3, "std_error": 3, "cov": 3})

    return df
