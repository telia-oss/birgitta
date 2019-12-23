"""Data frame assertion helpers
"""
from pandas.util.testing import assert_frame_equal

__all__ = ['assert_outputs']


def assert_outputs(expected_dfs, dataframe_source, spark_session):
    """Assert that the expected fixture dataframes are equal to the
    produced outputs. Improves exception presentation."""
    for df_key in expected_dfs.keys():
        result_df = dataframe_source.load(spark_session, df_key, None)
        expected_df = expected_dfs[df_key]
        result_panda = to_pandas(result_df)
        expected_panda = to_pandas(expected_df)
        # Ugly hack to print schema
        if not result_panda.equals(expected_panda):
            print_schema_diff(df_key, result_df, expected_df)
        try:
            assert_frame_equal(result_panda, expected_panda)
        except AssertionError as e:
            if "Attributes are different" in str(e):
                print("============== Assertion Error:",
                      df_key,
                      "==============")
                print("Result for %s" % (df_key))
                result_df.show()
                print("Expected for %s" % (df_key))
                expected_df.show()
            raise e


def to_pandas(df):
    # Convert timestamp to string to work around arrow error:
    # "Can only use .dt accessor with datetimelike values"
    for field in df.schema.fields:
        ftype = field.dataType.__class__.__name__
        if ftype in ["DateType", "TimestampType"]:
            df = df.withColumn(field.name, df[field.name].cast("string"))
    return df.toPandas()


def print_schema_diff(df_key, result_df, expected_df):
    print_schema(df_key + "_result (left)", result_df)
    print_schema(df_key + "_expected (right)", expected_df)
    print_schema_type_diffs(df_key, result_df, expected_df)


def print_schema_type_diffs(df_key, result_df, expected_df):
    print("============== ", end="")
    print("Data Frame Schema Diff: %s" % (df_key), end="")
    print(" ==============")
    result_fields = df_schema_to_set(result_df)
    expected_fields = df_schema_to_set(expected_df)
    not_in_result = sorted(expected_fields.difference(result_fields))
    if not_in_result:
        print("Not in result:", repr(not_in_result))
    not_in_expected = sorted(result_fields.difference(expected_fields))
    if not_in_expected:
        print("Not in expected:", repr(not_in_expected))


def df_schema_to_set(df):
    field_strs = set()
    for i, f in enumerate(df.schema.fields):
        field_strs.add("%d:%s:%s" % (i, f.name,
                                     f.dataType.__class__.__name__))
    return field_strs


def df_schema_to_str(df):
    field_strs = []
    for i, f in enumerate(df.schema.fields):
        field_strs.append("%d:%s:%s" % (i, f.name,
                                        f.dataType.__class__.__name__[:8]))
    return ", ".join(field_strs)


def print_schema(name, df):
    print("============== ", end="")
    print("Data Frame Schema: %s" % (name), end="")
    print(" ==============")
    print(df_schema_to_str(df))
