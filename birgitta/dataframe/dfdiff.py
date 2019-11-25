"""Functions for diffing pyspark dataframes."""

import pandas as pd

__all__ = ['diff', 'assert_equals']


def array_diff(first, second):
    if len(first) != len(second):
        return True
    first = set(first)
    second = set(second)
    return [item for item in first if item not in second]


def cols_diff(expected, actual):
    """Should have same columns"""
    if array_diff(expected.columns, actual.columns):
        expected_cols = ",".join(expected.columns)
        actual_cols = ",".join(actual.columns)
        return """Error: Cols diff
Expected: %s
Actual:   %s""" % (expected_cols, actual_cols)
    return None


def dtypes_diff(expected, actual):
    """Should have same data types"""
    if array_diff(expected.dtypes, actual.dtypes):
        print("actual.dtypes:", repr(actual.dtypes))
        expected_dtypes = repr(expected.dtypes)
        actual_dtypes = repr(actual.dtypes)
        return """Error: Column type diff
Expected: %s
Actual:   %s""" % (expected_dtypes, actual_dtypes)
    return None


def diff(expected, actual):
    """Diff two dataframes. However, this function has errors
    as it only subtracts expected from actual, when it should
    test the opposite also."""
    col_diff = cols_diff(expected, actual)
    if col_diff:
        return col_diff
    dtype_diff = dtypes_diff(expected, actual)
    if dtype_diff:
        return dtype_diff
    expected_count = expected.count()
    actual_count = actual.count()
    # Same number of rows
    ret = False
    if expected_count != actual_count:
        ret = """Error: Row count diff
Expected: %s
Actual:   %s""" % (expected_count, actual_count)
    # Are the records the same
    expected_pdf = panda_sorted(expected)
    actual_pdf = panda_sorted(actual)
    if not expected_pdf.equals(actual_pdf):
        max_rows = 20
        diff_df = expected.subtract(actual)
        reverse_diff_df = actual.subtract(expected)
        differing_rows_str = panda_sorted(diff_df).head(max_rows).to_string()
        reverse_differing_rows_str = panda_sorted(
            reverse_diff_df).head(max_rows).to_string()
        expected_rows_str = expected_pdf.head(max_rows).to_string()
        actual_rows_str = actual_pdf.head(max_rows).to_string()
        if not ret:
            ret = "Error: "
        else:
            ret += "\n\n"
        ret += """Rows are different (max %d rows shown)
Only in expected:
%s
Only in actual result:
%s
Expected:
%s
Actual:
%s""" % (max_rows,
         differing_rows_str,
         reverse_differing_rows_str,
         expected_rows_str,
         actual_rows_str)
    return ret


def panda_assert_df_equals(expected_df, actual_df):
    expected_pdf = panda_sorted(expected_df)
    actual_pdf = panda_sorted(actual_df)
    pd.testing.assert_frame_equal(expected_pdf,
                                  actual_pdf,
                                  check_like=True,
                                  check_dtype=True)


def panda_sorted(df):
    return df.toPandas().sort_values(df.columns).reset_index(drop=True)


def assert_equals(expected, actual):
    # Create richer diff information
    diff_ret = diff(expected, actual)
    if (diff_ret):
        cols = actual.columns
        col_strs = []
        for idx, col in enumerate(cols):
            col_strs.append("%d:%s" % (idx, col))
        print("Columns:", ", ".join(col_strs))
        print(diff_ret)
    # Perform a deeper comparison to catch dtype and other diffs
    panda_assert_df_equals(expected, actual)
