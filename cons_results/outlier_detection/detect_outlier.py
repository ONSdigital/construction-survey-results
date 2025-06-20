import pandas as pd
from mbs_results.outlier_detection.detect_outlier import join_l_values
from mbs_results.outlier_detection.winsorisation import winsorise
from mbs_results.utilities.constrains import replace_with_manual_outlier_weights

from cons_results.outlier_detection.derive_outlier_weights import (
    derive_q290_outlier_weights,
)


def detect_outlier(
    df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:

    pre_win = join_l_values(
        df, config["l_values_path"], config["classification_values_path"], config
    )

    # filtering out q290 since they don't need to be winsorised
    non_290 = pre_win[pre_win[config["question_no"]] != 290]
    q290_rows = pre_win[pre_win[config["question_no"]] == 290]

    post_win = non_290.groupby(config["question_no"]).apply(
        lambda df: winsorise(
            df,
            config["strata"],
            config["period"],
            config["auxiliary_converted"],
            config["census"],
            "design_weight",
            "calibration_factor",
            config["target"],
            "l_value",
        )
    )

    # Remove groupby leftovers
    post_win.reset_index(drop=True, inplace=True)

    # Concat with question_290 rows
    post_win = pd.concat([post_win, q290_rows])

    # Calculate outlier weightsfor q290 rows
    post_win = derive_q290_outlier_weights(
        post_win,
        config["all_questions"],
        config["target"],
        config["question_no"],
        config["reference"],
        config["period"],
    )

    # Replace outlier weights
    post_win = replace_with_manual_outlier_weights(
        post_win,
        config["reference"],
        config["period"],
        config["question_no"],
        "outlier_weight",
        config,
    )

    # This is needed for the additional outputs functions
    post_win["winsorised_value"] = (
        post_win["outlier_weight"] * post_win["adjustedresponse"]
    )
    
    return post_win
