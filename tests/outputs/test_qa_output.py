import pandas as pd
import pytest

from cons_results.outputs.qa_output import get_qa_output


@pytest.fixture
def sample_df_and_config():
    data = {
        "reference": [101, 101, 101, 102],
        "question_no": [1, 2, 3, 2],
        "target": [10, 20, 30, 40],
        "adj_target": [11, 22, 33, 44],
        "marker": ["a", "b", "c", "d"],
        "imputation_marker_col": ["r", "i", "c", "i"],
        "period": ["2023-01", "2023-01", "2023-01", "2023-01"],
        "cell_number": [1, 1, 1, 2],
        "auxiliary": [100, 100, 100, 200],
        "design_weight": [1.0, 1.0, 1.0, 2.0],
        "outlier_weight": [1.0, 1.0, 1.0, 2.0],
        "calibration_factor": [1.0, 1.0, 1.0, 2.0],
        "froempment": [3, 3, 3, 4],
        "sic": [100, 100, 100, 200],
    }
    df = pd.DataFrame(data)
    config = {
        "period": "period",
        "current_period": "2023-01",
        "reference": "reference",
        "question_no": "question_no",
        "target": "target",
        "cell_number": "cell_number",
        "auxiliary": "auxiliary",
        "froempment": "froempment",
        "sic": "sic",
        "imputation_marker_col": "imputation_marker_col",
    }
    return df, config


@pytest.fixture
def expected_qa_output():
    expected = pd.read_csv(
        "./tests/data/outputs/qa_output/expected_output.csv",
        header=[0, 1],
        index_col=[0, 1, 2, 3, 4, 5],
    )
    return expected


def test_get_qa_output_shape_and_columns(sample_df_and_config):
    df, config = sample_df_and_config
    result = get_qa_output(df, config)
    # Should have a MultiIndex on columns: (question_no, value_column)
    assert isinstance(result.columns, pd.MultiIndex)
    # Should have 3 question_no columns (1,2,3) and 4 value columns
    assert set(result.columns.get_level_values(0)) == {1, 2, 3}
    assert set(result.columns.get_level_values(1)) == {
        "target",
        "imputation_marker_col",
        "outlier_weight",
        "curr_grossed_value",
    }
    # Should have 2 rows (since all index columns are the same)
    assert result.shape[0] == 2


def test_get_qa_output_values(sample_df_and_config):
    df, config = sample_df_and_config
    result = get_qa_output(df, config)
    # Check that curr_grossed_value is correct (target * 1 * 1 * 1)
    for q in [1, 2, 3]:
        assert (
            result[(q, "curr_grossed_value")].iloc[0]
            == df.loc[df["question_no"] == q, "target"].iloc[0]
        )
        assert (
            result[(q, "target")].iloc[0]
            == df.loc[df["question_no"] == q, "target"].iloc[0]
        )
        assert (
            result[(q, "imputation_marker_col")].iloc[0]
            == df.loc[df["question_no"] == q, "imputation_marker_col"].iloc[0]
        )
        assert result[(q, "outlier_weight")].iloc[0] == 1.0


def test_get_qa_output_index(sample_df_and_config, expected_qa_output):
    df, config = sample_df_and_config
    result = get_qa_output(df, config)
    # Index should be a MultiIndex with the specified index columns
    expected = expected_qa_output
    # Index should match expected
    pd.testing.assert_index_equal(result.index, expected.index)
