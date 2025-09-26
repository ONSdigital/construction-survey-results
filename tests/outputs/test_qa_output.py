import pandas as pd
import pytest

from cons_results.outputs.qa_output import produce_qa_output


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
        "runame1": ["A", "A", "A", "B"],
        "nil_status_col": ["N", "N", "N", "Y"],
        "classification": [100, 100, 100, 100],
        "region": ["North", "North", "North", "South"],
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
        "nil_status_col": "nil_status_col",
    }
    return df, config


@pytest.fixture
def expected_qa_output():
    expected = pd.read_csv(
        "./tests/data/outputs/qa_output/expected_output.csv", header=[0, 1]
    ).rename(columns={"placeholder": ""})
    return expected


def test_produce_qa_output_shape_and_columns(sample_df_and_config):
    df, config = sample_df_and_config
    result = produce_qa_output(df, **config)
    # Should have a MultiIndex on columns: (question_no, value_column)
    assert isinstance(result.columns, pd.MultiIndex)
    # Should have 3 question_no columns (1,2,3) and 4 value columns
    assert set(result.columns.get_level_values(0)) == {
        "1",
        "2",
        "3",
        "sic",
        "period",
        "reference",
        "design_weight",
        "calibration_factor",
        "nil_status_col",
        "auxiliary",
        "froempment",
        "runame1",
        "cell_number",
        "classification",
        "region",
    }
    assert set(result.columns.get_level_values(1)) == {
        "target",
        "imputation_marker_col",
        "outlier_weight",
        "weighted adjusted value",
        "",
    }
    # Should have 2 rows (since all index columns are the same)
    assert result.shape[0] == 2


def test_produce_qa_output_values(sample_df_and_config):
    df, config = sample_df_and_config
    result = produce_qa_output(df, **config)
    # Check that weighted adjusted value is correct (adjustedresponse * 1 * 1 * 1)
    for q in ["1", "2", "3"]:
        assert (
            result[(q, "weighted adjusted value")].iloc[0]
            == df.loc[df["question_no"] == int(q), "target"].iloc[0]
        )
        assert (
            result[(q, "target")].iloc[0]
            == df.loc[df["question_no"] == int(q), "target"].iloc[0]
        )
        assert (
            result[(q, "imputation_marker_col")].iloc[0]
            == df.loc[df["question_no"] == int(q), "imputation_marker_col"].iloc[0]
        )
        assert result[(q, "outlier_weight")].iloc[0] == 1.0


def test_produce_qa_output_index(sample_df_and_config, expected_qa_output):
    df, config = sample_df_and_config
    result = produce_qa_output(df, **config)
    # Index should be a MultiIndex with the specified index columns
    expected = expected_qa_output
    # Index should match expected
    # Check that columns are the same (ignoring order)
    assert set(result.columns) == set(expected.columns)
    # Check that the dataframes are roughly equal (allowing for column order
    # differences)

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1),
        expected.sort_index(axis=1),
        check_like=True,
    )
