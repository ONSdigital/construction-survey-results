import pandas as pd


def get_additional_outputs(
    config: dict,
    function_mapper: dict,
    additional_outputs_df: pd.DataFrame,
) -> dict:
    """
    produces outputs by combining "additional_outputs" and "mandatory_outputs"
    from the config file and running the functions from the function_mapper.
    mandatory_outputs are always run and defined in the dev config file.
    optional_outputs are defined in the user config file.

    Parameters
    ----------
    config : dict
        main config file for pipeline
    function_mapper : dict
        dictionary with function names as keys and functions as values
    additional_outputs_df : pd.DataFrame
        dataframe used to produce outputs

    Returns
    -------
    dict
        dictionary with function names as keys and tuples of
        (dataframe, name) as values, where dataframe is the output of the function

    Raises
    ------
    TypeError
        raises type error if "additional_outputs" or "mandatory_outputs" in config
        are not lists or if they are not specified correctly
    ValueError
        raises value error if a function specified in "additional_outputs" or
        "mandatory_outputs" is not registered in the function_mapper
    """
    additional_outputs = dict()
    for config_list_name in ["additional_outputs", "mandatory_outputs"]:
        if not isinstance(config[config_list_name], list):
            raise TypeError(
                f"""
                In config file {config_list_name} must be a list, please use: \n
                ["all"] to get all outputs\n
                [] to get only mandatory outputs (defined in dev config) (\n
                or a list with the outputs, e.g. ["output_1", "output_2"]
                """  # noqa: E272
            )

    if config["additional_outputs"] == ["all"]:
        # Dont have to worry about mandatory outputs here, all functions from mapper
        # will run. mandatory and optional outputs should be defined in the mapper
        functions_to_run = function_mapper.keys()
    else:
        # If "all" is not specified, use the provided list combining with
        # mandatory outputs
        functions_to_run = sorted(
            list(set(config["additional_outputs"]) | set(config["mandatory_outputs"]))
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
