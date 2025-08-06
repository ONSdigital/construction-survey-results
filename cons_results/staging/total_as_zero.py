import pandas as pd


def flag_total_only_and_zero(
    df: pd.DataFrame,
    reference: str,
    period: str,
    values: str,
    qcodes: str,
    total_question_code: str = 290,
) -> pd.DataFrame:
    """
    Adds a new boolean column `is_total_only_and_zero` indicating if a
    reference in a specific period has a total (default 290 question code)
    returned as zero and no returns for other question codes.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    reference : str
        Column containing reference values.
    period : str
        Column containing period values.
    values : str
        Column containing values.
    qcodes : str
        Column containing question code values.
    total_question_code : str, optional
        Question code value which represents total. The default is 290.

    Returns
    -------
    df :  pd.DataFrame
        Original dataframe with `is_total_only_and_zero` column indicating
        which reference per period this condition exists.
        .

    Examples
    --------
    >>> data = {
        'period': [202201, 202201, 202201, 202201, 202201,202201],
        'reference': [1, 2, 2, 2, 2,3],
        'qcodes': [290,290,2,3,4,4],
        'values': [0, 150, 75, 75, 0,0]
    }
    >>> df = pd.DataFrame(data)
    >>> print(df)
        period  reference  qcodes  values
     0  202201          1     290       0
     1  202201          2     290     150
     2  202201          2       2      75
     3  202201          2       3      75
     4  202201          2       4       0
     5  202201          3       4       0


    >>> df2 = flag_total_only_and_zero(
        df,"reference","period","values","qcodes")
    >>> print(df2)
        period  reference  qcodes  values  is_total_only_and_zero
     0  202201          1     290       0                    True
     1  202201          2     290     150                   False
     2  202201          2       2      75                   False
     3  202201          2       3      75                   False
     4  202201          2       4       0                   False
     5  202201          3       4       0                   False
    """

    df_with_conditions = df.groupby([reference, period]).agg(
        {
            # check if sum is zero
            values: lambda x: (sum(x) == 0),
            # check only q290 exists no other questions
            qcodes: lambda x: (all(x == total_question_code)),
        }
    )

    total_only_and_0 = df_with_conditions.loc[
        (df_with_conditions[values]) & (df_with_conditions[qcodes])
    ]

    # initiliase flag column (default false)
    df["is_total_only_and_zero"] = False

    df = df.set_index([reference, period])

    # set to true for indices(period,reference) in total_only_and_0
    df.loc[total_only_and_0.index, "is_total_only_and_zero"] = True

    return df.reset_index()
