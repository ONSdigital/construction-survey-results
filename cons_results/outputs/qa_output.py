import pandas as pd


def get_qa_output(additional_outputs_df: pd.DataFrame, **config) -> pd.DataFrame:
    """
    Creates QA output for current period on frozen runs only.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        The input DataFrame containing additional outputs.
    config : dict
        A dictionary containing configuration parameters. Must include:
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

    Returns
    -------
    pd.DataFrame
        QA output.
    """

    qa_output = additional_outputs_df[
        [config["reference"], config["question_no"], config["target"]]
    ]

    # qa_output = qa_output.rename(
    #     columns={
    #         config["target"]: "response",
    #         config["question_no"]: "question_no",
    #         config["reference"]: "reference",
    #     }
    # )

    return qa_output.reset_index(drop=True)


if __name__ == "__main__":
    # Example usage
    data = {
        "reference": ["ref1", "ref1", "ref1"],
        "question_no": [1, 2, 3],
        "target": [10, 20, 30],
        "adj_target": [11, 22, 33],
        "marker": ["a", "b", "c"],
        "imputation_marker_col": ["r", "i", "c"],
        "period": ["2023-01", "2023-01", "2023-01"],
    }
    df = pd.DataFrame(data)
    config = {
        "period": "period",
        "current_period": "2023-01",
        "reference": "reference",
        "question_no": "question_no",
        "target": "target",
    }
    # Example: Create a pivot table with df
    pivot_df = df.pivot_table(
        index=["reference", "period"],
        columns="question_no",
        values=["target", "adj_target", "marker", "imputation_marker_col"],
        aggfunc="first",
    )
    # Rearrange columns to have question_no as the top level, then target/adj_target
    pivot_df = pivot_df.swaplevel(axis=1).sort_index(axis=1, level=0)
    print(pivot_df)

    qa_output_df = get_qa_output(df, **config)
    print(qa_output_df)
    print(qa_output_df.dtypes)
