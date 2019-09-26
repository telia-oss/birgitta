"""Functions for diffing pyspark dataframes."""

__all__ = ['diff']


def array_diff(first, second):
    if len(first) != len(second):
        return True
    first = set(first)
    second = set(second)
    return [item for item in first if item not in second]


def diff(expected, actual):
    """Diff two dataframes. However, this function has errors
    as it only subtracts expected from actual, when it should
    test the opposite also."""
    # Same number of cols
    if array_diff(expected.columns, actual.columns):
        expected_cols = ",".join(expected.columns)
        actual_cols = ",".join(actual.columns)
        return """Error: Cols diff
        Expected: %s
        Actual:   %s""" % (expected_cols, actual_cols)
    expected_count = expected.count()
    actual_count = actual.count()
    # Same number of rows
    if expected_count != actual_count:
        return """Error: Row count diff
        Expected: %s
        Actual:   %s""" % (expected_count, actual_count)
    # Are the records the same
    diff_df = expected.subtract(actual)
    diff_df_count = diff_df.count()
    if diff_df_count > 0:
        differing_rows_str = diff_df.toPandas().to_string()
        return """Error: Rows are different
        %s""" % (differing_rows_str)
    return False
