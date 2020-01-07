"""Functions for loading and writing spark dataframes,
adapting to the globally defined storage platform
(file, memory, dataiku, s3 etc). This is useful to enable
the running and testing of recipes different underlying
storage platforms.

cast_binary_cols_to_string() is a utility function to deal with
binary to string conversion of a spark dataframe."""


import pandas as pd
from birgitta.dataframesource import contextsource
from birgitta.recipetest.localtest import assertion
from birgitta.schema.spark import short_to_class_type
from pyspark.sql import functions
from pyspark.sql.utils import AnalysisException


__all__ = ['get', 'write', 'cast_binary_cols_to_string', 'SchemaError']


def get(spark_session,
        dataset_name=None,
        *,
        prefix=None,
        dataset=None,
        schema=None,
        cast_binary_to_str=False,
        dataframe_source=None):
    """Obtain a dataframe. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file, memory or dataiku (HDFS).

    Args:
        sqlContext (SqlContext): spark sql context used to load data frames.
        dataset_name (str): The data set to load.

    Kwargs:
        prefix (str): Prefix path or dataiku project_key for loading
        the data set.
        cast_binary_to_str (bool): Convert binary to str.
        schema (Schema): Birgitta schema to verify after read.
        dataset (Dataset): Birgitta dataset to use for name and schema
        dataframe_source (DataframeSourceBase): Option to override
        the data frame source defined in the context.
    Returns:
       Spark DataFrame.
    """
    if not dataframe_source:
        dataframe_source = contextsource.get()
    if dataset:
        if schema is None:
            schema = dataset.schema
        if dataset_name is None:
            dataset_name = dataset.name
    ret = dataframe_source.load(spark_session,
                                dataset_name,
                                prefix,
                                schema=schema)
    if cast_binary_to_str:
        ret = cast_binary_cols_to_string(ret)
    return ret


def cast_schema(dataset_name, df, schema):
    """Ensure df schema corresponds to defined schema"""
    try:
        return schema.cast(df)
    except AnalysisException as err:
        sdiff = schema_diff(df, schema)
        raise SchemaError(
            f"Dataset {dataset_name} with unexpected schema {sdiff}" +
            "\n\nOriginal exception: " + repr(err))


def schema_diff(df, schema):
    ret = "\n"
    df_fields = assertion.df_schema_to_set(df)
    schema_fields = schema_to_set(schema)
    not_in_df = sorted(schema_fields.difference(df_fields))
    if not_in_df:
        ret += "\nNot in df:\n\n" + repr(not_in_df)
    not_in_schema = sorted(df_fields.difference(schema_fields))
    if not_in_df:
        ret += "\n\nNot in schema:\n\n" + repr(not_in_schema)
    return ret


def schema_to_set(schema):
    field_strs = set()
    s_dict = schema.dict()
    for i, name in enumerate(schema.fields()):
        field_strs.add("%d:%s:%s" % (
            i, name, short_to_class_type(s_dict[name]).__class__.__name__))
    return field_strs


def write(df,
          dataset_name,
          *,
          prefix=None,
          schema=None,
          dataframe_source=None,
          skip_cast=False,
          **kwargs):
    """Write a dataframe to storage. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file or dataiku (HDFS).

    Args:
        df (DataFrame): spark data frame to write.
        dataset_name (str): The data set to load.

    Kwargs:
        prefix (str): Prefix path or dataiku project_key for loading
        the data set.
        schema (Schema): Birgitta schema to apply on write.
        dataframe_source (DataframeSourceBase): Option to override
        the data frame source defined in the context.
        skip_cast (bool): If True, don't cast

    Returns:
       None.
    """
    if schema and not skip_cast:
        df = cast_schema(dataset_name, df, schema)
    if not dataframe_source:
        dataframe_source = contextsource.get()
    return dataframe_source.write(df,
                                  dataset_name,
                                  prefix,
                                  schema=schema,
                                  **kwargs)


def cast_binary_cols_to_string(df):
    """Write a dataframe to storage. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file or dataiku (HDFS).

    Args:
        df (DataFrame): spark data frame to transform.

    Returns:
       Dataframe where columns in 'binary' format are casted to 'string'.
    """
    col_types = df.dtypes
    col_types = pd.DataFrame(col_types, columns=['column', 'type'])
    binary_cols = col_types.loc[col_types['type'] == 'binary']
    binary_cols = list(binary_cols['column'])
    for col_name in binary_cols:
        df = df.withColumn(col_name, functions.col(col_name).cast('string'))
    return(df)


class SchemaError(Exception):
    def __init__(self, message):
        super().__init__(message)
