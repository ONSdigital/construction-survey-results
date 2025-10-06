import itertools

import pandas as pd
from mbs_results.outputs.growth_rates_output import get_growth_rates_output


def get_cord_output(
    additional_outputs_df: pd.DataFrame, **config: dict
) -> pd.DataFrame:
    """Generate CORD output DataFrame. Uses MBS' get_growth_rates_output
    for most of the reformatting since this is doing the same thing.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        DataFrame containing variables needed for additional outputs.
    config : dict
        Configuration dictionary containing necessary parameters.

    Returns
    -------
    pd.DataFrame
        DataFrame formatted for CORD output.
    """
    # Filter to only components questions
    additional_outputs_df = additional_outputs_df[
        additional_outputs_df["questioncode"].isin(config["components_questions"])
    ]

    cord_output_df = get_growth_rates_output(additional_outputs_df, **config)

    # Map sizeband from numeric -> character
    cord_output_df["sizeband"] = cord_output_df["sizeband"].astype(str)

    cord_output_df["sizeband"] = cord_output_df["sizeband"].replace(
        config["sizeband_numeric_to_character"]
    )

    # add missing sizebands
    missing_sizebands = set(config["sizeband_numeric_to_character"].values()) - set(
        cord_output_df["sizeband"]
    )
    if missing_sizebands:

        missing_groups = set(
            itertools.product(
                list(map(int, config["imputation_contribution_classification"])),
                config["components_questions"],
                missing_sizebands,
            )
        )
        missing_df = pd.DataFrame(
            missing_groups,
            columns=["classification", config["question_no"], "sizeband"],
        )

        cord_output_df = pd.concat([cord_output_df, missing_df]).fillna(0)

    cord_output_df["classification"] = cord_output_df["classification"].astype(int)
    cord_output_df["sizeband"] = cord_output_df["sizeband"].astype(str)
    cord_output_df[config["question_no"]] = cord_output_df[
        config["question_no"]
    ].astype(int)

    # Change sort order
    cord_output_df = cord_output_df.sort_values(
        by=["classification", "sizeband", config["question_no"]]
    ).reset_index(drop=True)

    return cord_output_df
