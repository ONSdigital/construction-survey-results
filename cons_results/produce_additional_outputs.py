from mbs_results.utilities.inputs import load_config, read_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename, read_run_id

from cons_results.outputs.produce_additional_outputs import produce_additional_outputs


def produce_additional_outputs_wrapper(config_user_dict=None):
    """Produces any additional outputs based on MBS methods output"""

    config = load_config("config_outputs.json", config_user_dict)
    config["run_id"] = config.get("run_id") or read_run_id()

    output_file_name = get_versioned_filename(
        config["cons_output_prefix"],
        config["run_id"],
    )

    output_path = f"{config['main_cons_output_folder_path']}{output_file_name}"

    df = read_csv_wrapper(
        filepath=output_path,
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    target = config.get("target")
    df[f"{target}_actual"] = df[target].copy()
    df.loc[~df[config["question_no"]].isin([11, 12, 146, 901, 902, 902]), target] = df[
        "adjustedresponse_pounds_thousands"
    ].copy()

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=False, optional_outputs=True, config=config
    )


if __name__ == "__main__":
    produce_additional_outputs_wrapper()
