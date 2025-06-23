import pandas as pd


def get_additional_outputs(
    config: dict,
    function_mapper: dict,
    additional_outputs_df: pd.DataFrame,
) -> dict:
    additional_outputs = dict()

    if not isinstance(config["additional_outputs"], list):
        raise ValueError(
            """
            In config file additional_outputs must be a list, please use:\n
            ["all"] to get all outputs\n
            [] to get no outputs\n
            or a list with the outputs, e.g. ["output_1","output_2"]
            """
        )

    if config["additional_outputs"] == ["all"]:
        # Dont have to worry about mandatory outputs here, all functions from mapper
        # will run. mandatory and optional outputs should be defined in the mapper
        functions_to_run = function_mapper.keys()
    else:
        # If "all" is not specified, use the provided list combining with
        # mandatory outputs
        functions_to_run = list(
            set(config["additional_outputs"]) | set(config["mandatory_outputs"])
        )

    for function in functions_to_run:
        if function in function_mapper:
            result = function_mapper[function](
                additional_outputs_df=additional_outputs_df, **config
            )
            # Function can return either a tuple of df, name or just a dataframe
            if isinstance(result, tuple):
                df, name = result
            else:
                df, name = result, None
            additional_outputs[function] = (df, name)
        else:
            raise ValueError(
                f"""
                The function {function} is not registered, check spelling.\n
                Currently the registered functions are: \n {function_mapper}
                """
            )

    return additional_outputs
