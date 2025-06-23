import pandas as pd

from cons_results.outputs.get_additional_outputs import get_additional_outputs
from cons_results.outputs.imputes_and_constructed_output import (
    get_imputes_and_constructed_output,
)
from cons_results.utilities.utils import get_versioned_filename


def produce_additional_outputs(config: dict, additional_outputs_df: pd.DataFrame):
    additional_outputs = get_additional_outputs(
        config,
        {
            "imputes_and_constructed_output": get_imputes_and_constructed_output,
        },
        additional_outputs_df,
    )

    if additional_outputs is None:
        return

    for output, (df, name) in additional_outputs.items():
        if name:
            filename = name
        else:
            filename = get_versioned_filename(output, config)
        df.to_csv(config["output_path"] + filename, index=False)
        print(config["output_path"] + filename + " saved")
