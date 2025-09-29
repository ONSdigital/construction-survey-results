from mbs_results.utilities.inputs import load_config, read_csv_wrapper

from cons_results.outputs.produce_additional_outputs import produce_additional_outputs


def produce_additional_outputs_wrapper(config_user_dict=None):
    """Produces any additional outputs based on MBS methods output"""

    config = load_config("config_outputs.json", config_user_dict)

    df = read_csv_wrapper(
        filepath=config["cons_output_path"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=False, optional_outputs=True, config=config
    )


if __name__ == "__main__":
    produce_additional_outputs_wrapper()
