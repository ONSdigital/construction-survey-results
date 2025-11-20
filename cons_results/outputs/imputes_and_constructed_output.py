import pandas as pd


def get_imputes_and_constructed_output(
    additional_outputs_df: pd.DataFrame, **config
) -> pd.DataFrame:
    """
    Creates imputes and constructed output for current period on frozen runs only.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        The input DataFrame containing additional outputs.
    config : dict
        A dictionary containing configuration parameters. Must include:
        - "state" : str
            The state of the process. If not "frozen", the function returns None.
        - "period" : str
            The column name for the period.
        - "current_period" : Any
            The value of the current period.
        - "reference" : str
            The column name for the reference.
        - "question_no" : str
            The column name for the question number.
        - "target" : str
            The column name for the target data.
        - "imputation_marker_col" : str
            The column name for the imputation markers.

    Returns
    -------
    pd.DataFrame
        Imputes and construction output.
        Returns None if not frozen run.

    filename: str
        The name of the output file, formatted as "constructed228_{current_period}".
    """
    if config["state"] != "frozen":
        return

    additional_outputs_df = additional_outputs_df[
        additional_outputs_df[config["period"]] == config["current_period"]
    ]

    imputes_and_constructed_output = additional_outputs_df[
        [
            config["reference"],
            config["question_no"],
            config["target"],
            config["imputation_marker_col"],
        ]
    ]

    imputes_and_constructed_output = imputes_and_constructed_output[
        imputes_and_constructed_output[config["imputation_marker_col"]] != "r"
    ]

    imputes_and_constructed_output = imputes_and_constructed_output.rename(
        columns={
            config["target"]: "constructedresponse",
            config["imputation_marker_col"]: "imputationmarker",
        }
    )

    imputes_and_constructed_output.reset_index(drop=True, inplace=True)

    filename = f"constructed228_{config['current_period']}.csv"

    return imputes_and_constructed_output, filename
