import pandas as pd
import pytest

from cons_results.outputs.qa_output import produce_qa_output


@pytest.fixture(scope="class")
def sample_df_and_config():
    data = {
        "reference": [101, 101, 101, 102, 103, 104],
        "question_no": [1, 2, 3, 2, 1, 1],
        "target": [10, 20, 30, 40, 50, 60],
        "adj_target": [11, 22, 33, 44, 55, 66],
        "marker": ["a", "b", "c", "d", "e", "f"],
        "imputation_marker_col": ["r", "i", "c", "i", "r", "c"],
        "period": ["2023-01", "2023-01", "2023-01", "2023-01", "2023-02", "2023-02"],
        "cell_number": [1, 1, 1, 2, 3, 4],
        "auxiliary": [100, 100, 100, 200, 200, 300],
        "design_weight": [1.0, 1.0, 1.0, 2.0, 1.0, 1.0],
        "outlier_weight": [1.0, 1.0, 1.0, 2.0, 1.0, 1.0],
        "calibration_factor": [1.0, 1.0, 1.0, 2.0, 1.0, 1.0],
        "froempment": [3, 3, 3, 4, 5, 6],
        "sic": [100, 100, 100, 200, 200, 200],
        "runame1": ["A", "A", "A", "B", "C", "D"],
        "nil_status_col": ["N", "N", "N", "Y", "N", "Y"],
        "classification": [100, 100, 100, 100, 100, 100],
        "region": ["North", "North", "North", "South", "East", "West"],
    }
    df = pd.DataFrame(data)
    config = {
        "period": "period",
        "current_period": "2023-01",
        "reference": "reference",
        "question_no": "question_no",
        "target": "target",
        "pound_thousand_col": "target",
        "cell_number": "cell_number",
        "auxiliary": "auxiliary",
        "froempment": "froempment",
        "sic": "sic",
        "imputation_marker_col": "imputation_marker_col",
        "nil_status_col": "nil_status_col",
    }
    return df, config


@pytest.fixture(scope="class")
def expected_qa_output(outputs_data_dir):

    path = outputs_data_dir / "qa_output"

    period_dict = {
        "2023-01": pd.read_csv(path / "expected_output_p1.csv", header=[0, 1]).rename(
            columns={"placeholder": ""}
        ),
        "2023-02": pd.read_csv(path / "expected_output_p2.csv", header=[0, 1]).rename(
            columns={"placeholder": ""}
        ),
    }

    return period_dict


class TestProduceQAOutput:
    @pytest.mark.skip(
        reason="Shape and columns are already tested in test_produce_qa_output_index"
    )
    def test_produce_qa_output_shape_and_columns(self, sample_df_and_config):
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

    def test_produce_qa_output_values(self, sample_df_and_config):
        df, config = sample_df_and_config
        result = produce_qa_output(df, **config)
        # Check that weighted adjusted value is correct (adjustedresponse * 1 * 1 * 1)

        for q in ["1", "2", "3"]:
            assert (
                result["2023-01"][(q, "weighted adjusted value")].iloc[0]
                == df.loc[df["question_no"] == int(q), "target"].iloc[0]
            )
            assert (
                result["2023-01"][(q, "target")].iloc[0]
                == df.loc[df["question_no"] == int(q), "target"].iloc[0]
            )
            assert (
                result["2023-01"][(q, "imputation_marker_col")].iloc[0]
                == df.loc[df["question_no"] == int(q), "imputation_marker_col"].iloc[0]
            )
            assert result["2023-01"][(q, "outlier_weight")].iloc[0] == 1.0

    def test_produce_qa_output_index(self, sample_df_and_config, expected_qa_output):
        df, config = sample_df_and_config
        result = produce_qa_output(df, **config)
        # Index should be a MultiIndex with the specified index columns
        expected = expected_qa_output
        # Index should match expected

        for key in result.keys():

            pd.testing.assert_frame_equal(
                result[key].reset_index(drop=True),
                expected[key].reset_index(drop=True),
                check_like=True,
                check_dtype=False,
            )
