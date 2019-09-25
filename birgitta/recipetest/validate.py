"""Function for validating that two data sets are the same.
"""
from birgitta.dataframe import dataframe
from birgitta.dataframe import dfdiff


def datasets(sqlContext, expected_ds, result_ds, project_key=None):
    """Validate that two data sets are the same.

    Args:
        sqlContext (SqlContext): spark sql context used to load data frames.
        expected_ds (str): The expected data set to load.
        result_ds (str): The result data set to load.
        project_key (str): Used if data set in a separate dataiku project.
    """
    expected_df = dataframe.get(sqlContext, expected_ds, project_key)
    result_df = dataframe.get(sqlContext, result_ds, project_key)
    diff_ret = dfdiff.diff(expected_df, result_df)
    assert not diff_ret, "Dataframe diff: " + diff_ret
    print("Test successful")
