import json
import uuid

import pandas as pd
from mbs_results.utilities.csw_to_spp_converter import (
    convert_cp_to_contributors,
    convert_qv_to_responses,
)
from mbs_results.utilities.file_selector import find_files
from mbs_results.utilities.inputs import read_colon_separated_file, read_csv_wrapper


def join_sample(
    contributors_df: pd.DataFrame,
    input_directory: str,
    current_period: int,
    revision_window: int,
    config: dict,
) -> pd.DataFrame:
    """
    Reads finalsel files and joins thems in contributors_df df on reference
    and period

    Parameters
    ----------
    contributors_df : pd.DataFrame
        Contributors dataframe.
    input_directory : str
        Directory where the finalsel files are stored.
    current_period : int
        Latest period of files.
    revision_window : int
        Length of periods to read.
    config : dict
        This is passed to find files, should contain:
            platform arguments.

    Returns
    -------
    pd.DataFrame
        Contributors dataframe joined with finalsel

    """

    finalsel_column_remapper = {
        "cell_no": "cellnumber",
        "froempees": "frozenemployees",
        "frosic2007": "frozensic",
        "frotover": "frozenturnover",
    }
    sample_column_names = [
        "reference",
        "checkletter",
        "frosic2003",
        "rusic2003",
        "frosic2007",
        "rusic2007",
        "froempees",
        "employees",
        "froempment",
        "employment",
        "froFTEempt",
        "FTEempt",
        "frotover",
        "turnover",
        "entref",
        "wowentref",
        "vatref",
        "payeref",
        "crn",
        "live_lu",
        "live_vat",
        "live_paye",
        "legalstatus",
        "entrepmkr",
        "region",
        "birthdate",
        "entname1",
        "entname2",
        "entname3",
        "runame1",
        "runame2",
        "runame3",
        "ruaddr1",
        "ruaddr2",
        "ruaddr3",
        "ruaddr4",
        "ruaddr5",
        "rupostcode",
        "tradstyle1",
        "tradstyle2",
        "tradstyle3",
        "contact",
        "telephone",
        "fax",
        "seltype",
        "inclexcl",
        "cell_no",
        "formtype",
        "cso_tel",
        "currency",
    ]

    sample_files = find_files(
        file_path=input_directory,
        file_prefix="finalsel228",
        current_period=current_period,
        revision_window=revision_window,
        config=config,
    )

    finalsel_data = pd.concat(
        [
            read_colon_separated_file(filepath=f, column_names=sample_column_names)
            for f in sample_files
        ],
        ignore_index=True,
    )
    finalsel_data = finalsel_data[
        [
            "reference",
            "period",
            "cell_no",
            "formtype",
            "froempees",
            "frosic2007",
            "frotover",
        ]
    ]

    finalsel_data["formtype"] = "0" + finalsel_data["formtype"].astype(str)
    finalsel_data.rename(columns=finalsel_column_remapper, inplace=True)
    return contributors_df.merge(finalsel_data, on=["reference", "period"], how="left")


def remove_skipped_questions(
    responses_df: pd.DataFrame,
    reference_col: str,
    period_col: str,
    questioncode_col: str,
    target_col: str,
    route_skipped_questions: dict,
    no_value: int = 2,
) -> pd.DataFrame:
    """
    Removes questions as follows, if a question the route_skipped_questions
    key is euqal to no_value(2) then drop the questions which exist in key
    values.
    e.g.
    route_skipped_questions= {
        902:[201,202,211,212]}

    shall remove questions 201,202,211,212 when 902 is equal to 2.

    Parameters
    ----------
    responses_df : pd.DataFrame
        responses dataframe.
    reference_col : str
        column name with reference values.
    period_col : str
        column name with period values.
    questioncode_col : str
        column name with question codes.
    target_col : str
        column name with question code values.
    route_skipped_questions : dict
        map to questions to be removed.
    no_value : int, optional
        How "no" is defined in routed questions. The default is 2.


    Returns
    -------
    anti_join : pd.DataFrame
        Dataframe without skipped questions.
    """

    # placeholder to save reference,period,questioncode
    skipped_questions_dfs = []

    for route_question, skipped_questions in route_skipped_questions.items():

        routed_value_for_no = responses_df[
            (responses_df[questioncode_col] == route_question)
            & (responses_df[target_col] == no_value)
        ]
        routed_value_for_no_reference_period = routed_value_for_no[
            [reference_col, period_col]
        ]

        skiped_questions = pd.DataFrame({questioncode_col: skipped_questions})

        all_skip_questions_by_reference_period = (
            routed_value_for_no_reference_period.merge(skiped_questions, how="cross")
        )

        skipped_questions_dfs.append(all_skip_questions_by_reference_period)

    # has all reference,period,questioncode
    # reference,period,questioncode uniquely identifies a row
    skipped_questions_df = pd.concat(skipped_questions_dfs, ignore_index=True)

    outer_join = responses_df.merge(skipped_questions_df, how="outer", indicator=True)

    anti_join = outer_join[(outer_join._merge == "left_only")].drop("_merge", axis=1)

    return anti_join


def remove_nil_contributors(
    responses_df: pd.DataFrame,
    contributors_df: pd.DataFrame,
    reference_col: str,
    period_col: str,
    response_type_col: str,
    nil_contributors_response_type: list,
) -> pd.DataFrame:
    """
    Removes questions in responses which came from nil contributors in contributors_df.

    Parameters
    ----------
    responses_df : pd.DataFrame
        responses dataframe.
    contributors_df : pd.DataFrame
        contributors dataframe.
    reference_col : str
        column name with reference values.
    period_col : str
        column name with period values.
    response_type_col : str
        column name with response types
    nil_contributors_response_type : list
        List of values which identify nil contributors in response_type_col.

    Returns
    -------
    anti_join : pd.DataFrame
        Dataframe with questions which came from nil contributors dropped.
    """

    nil_only_df = contributors_df[
        contributors_df[response_type_col].isin(nil_contributors_response_type)
    ]

    nil_only_df = nil_only_df[[reference_col, period_col]]

    outer_join = responses_df.merge(nil_only_df, how="outer", indicator=True)

    anti_join = outer_join[(outer_join._merge == "left_only")].drop("_merge", axis=1)

    return anti_join


def create_construction_228_snapshot(
    input_directory: str,
    output_directory: str,
    current_period: int,
    revision_window: int,
):
    """
    Creates a json file based on CSW files (qv cp and finalsel), the aim is
    to simulate the SPP snapshot from the CSW files.

    Parameters
    ----------
    input_directory : str
        Directory where CSW files exist.
    output_directory : str
        Directory to save snapshot.
    current_period : int
        Latest period for the snapshot.
    revision_window : int
        Lengh of period to convert.

    Examples
    --------
    Ensure that all files for the requested periods are in input_directory
    In the below example `D:/con_test/qv_cp/` should have qv cp and sample
    files from 202201 until 202203

    >>> create_construction_228_snapshot("D:/con_test/qv_cp/","D:/", 202303, 15)
    """

    config = {"platform": "network", "bucket": None}

    qv_files = find_files(
        input_directory, "qv_228", current_period, revision_window, config
    )

    cp_files = find_files(
        input_directory, "cp_228", current_period, revision_window, config
    )

    qv = pd.concat(
        [read_csv_wrapper(f, config["platform"], config["bucket"]) for f in qv_files],
        ignore_index=True,
    )

    all_cp = [
        read_csv_wrapper(f, config["platform"], config["bucket"]) for f in cp_files
    ]

    for df in all_cp:
        df.columns = df.columns.str.strip()

    cp = pd.concat(all_cp, ignore_index=True)

    qv = remove_skipped_questions(
        responses_df=qv,
        reference_col="reference",
        period_col="period",
        questioncode_col="question_no",
        target_col="adjusted_value",
        route_skipped_questions={
            902: [201, 202, 211, 212],
            903: [221, 222],
            904: [231, 232, 241, 242, 243],
        },
    )

    qv = remove_nil_contributors(
        responses_df=qv,
        contributors_df=cp,
        reference_col="reference",
        period_col="period",
        response_type_col="response_type",
        nil_contributors_response_type=[4, 5, 6, 7, 8, 9, 10, 12, 13],
    )

    qv = convert_qv_to_responses(qv)
    cp = convert_cp_to_contributors(cp)

    cp = join_sample(cp, input_directory, current_period, revision_window, config)

    output = {
        "snapshot_id": input_directory + str(uuid.uuid4().hex),
        "contributors": cp.to_dict("list"),
        "responses": qv.to_dict("list"),
    }

    with open(
        f"{output_directory}snapshot_qv_cp_{current_period}_{revision_window}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    return
