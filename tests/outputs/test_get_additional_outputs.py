import pandas as pd
import pytest

from cons_results.outputs.get_additional_outputs import get_additional_outputs


def test1(**kwargs):
    print(1)  # dummy function


def test2(**kwargs):
    print(2)  # dummy function


def test_3_mandatory(**kwargs):
    print(3)  # dummy function that should always run


@pytest.fixture(scope="class")
def function_mapper():
    return {"test1": test1, "test2": test2, "test_3_mandatory": test_3_mandatory}


@pytest.fixture(scope="class")
def mocked_config():
    return {"mandatory_outputs": ["test_3_mandatory"]}


@pytest.mark.parametrize(
    "inp, expected",
    [
        ({"additional_outputs": ["all"]}, "1\n2\n3\n"),
        ({"additional_outputs": ["test1"]}, "1\n3\n"),
        ({"additional_outputs": ["test2"]}, "2\n3\n"),
        ({"additional_outputs": []}, "3\n"),
    ],
)
def test_output(mocked_config, capsys, function_mapper, inp, expected):
    """Test that the right functions were run"""
    get_additional_outputs({**inp, **mocked_config}, function_mapper, pd.DataFrame())
    out, err = capsys.readouterr()
    assert out == expected


def test_raise_errors(function_mapper):
    """Test if error is raised when user doesn't pass a list or passes a
    function which does not link to a function"""

    with pytest.raises(TypeError):
        get_additional_outputs(
            {"additional_outputs": "not_a_list"}, function_mapper, pd.DataFrame()
        )


def test_raise_errors_not_included(function_mapper):
    """Test if error is raised when user doesn't pass a list or passes a
    function which does not link to a function"""

    with pytest.raises(ValueError):
        get_additional_outputs(
            {"additional_outputs": ["test1"], "mandatory_outputs": ["test3"]},
            function_mapper,
            pd.DataFrame(),
        )
