from typing import List

import pandas as pd


def derive_imputation_class(
    df: pd.DataFrame,
    sizebands: List[List[int]],
    column: str,
    save_bins_col_name: str,
    closed: str = "both",
) -> pd.DataFrame:
    """
     Adds new column to a dataframe based on pre-defined values.

     Parameters
     ----------
     df : pd.DataFrame
         Input dataframe to apply the sizebands.
     sizebands : List[List[int]]
         This is passed to pandas.IntervalIndex, list with lower and
         upper values for each sizeband.
     column : str
         Column name to apply the sizebands.
     save_bins_col_name : str
         Column name in which the bins should be saved.
     closed : str, optional
         Arg passed to pandas.IntervalIndex, please refer to pandas doc for this
         Accepted values are left, right, both, neither. For this
         function the default is "both".

     Returns
     -------
     df : pd.DataFrame
         Dataframe with an extra variable with bins as defined in the sizebands.

    Examples
     --------
     >>> data = {'ref': [1,1,2], 'cell_n': [11,13,35]}
     >>> df = pd.DataFrame(data=data)
            ref  cell_n
         0    1      11
         1    1      13
         2    2      35
     >>> bands = [[11,17],[31,37]]
     >>> result = derive_imputation_class(df,bands,"cell_n","imputation_class")
         ref  cell_n imputation_class
          0    1      11         [11, 17]
          1    1      13         [11, 17]
          2    2      35         [31, 37]

    """

    if any([len(x) != 2 for x in sizebands]):
        raise ValueError(
            f"""{sizebands} is not properly defined, function
 expects a list containing a list with 2 integers which will be used to define
 lower and upper bounds"""
        )

    # pd.IntervalIndex expects a list of tuples with upper lower bands
    bands_list_tuples = [tuple(x) for x in sizebands]

    bins = pd.IntervalIndex.from_tuples(bands_list_tuples, closed)

    df[save_bins_col_name] = pd.cut(df[column], bins)

    # TODO: discuss logging, raising, debugging strategy
    if df[save_bins_col_name].isnull().any():

        # nas_df = df[df[save_bins_col_name].isna()]
        raise ValueError(
            f"""There are values in {column}, which are not
 defined in {sizebands}"""
        )

    return df
