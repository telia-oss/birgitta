"""Functions for loading and writing spark dataframes,
adapting to the globally defined storage platform
(file, memory, dataiku, s3 etc). This is useful to enable
the running and testing of recipes different underlying
storage platforms.

cast_binary_cols_to_string() is a utility function to deal with
binary to string conversion of a spark dataframe."""

import pandas as pd
from birgitta.dataframesource import contextsource
from pyspark.sql import functions

__all__ = ['get', 'write', 'cast_binary_cols_to_string']


def get(sqlContext,
        dataset_name,
        prefix=None,
        *,
        cast_binary_to_str=False):
    """Obtain a dataframe. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file, memory or dataiku (HDFS).

    Args:
        sqlContext (SqlContext): spark sql context used to load data frames.
        dataset_name (str): The data set to load.
        prefix (str): Prefix path or dataiku project_key for loading
        the data set.

    Kwargs:
        cast_binary_to_str (bool): Convert binary to str.

    Returns:
       Spark DataFrame.
    """
    dataframe_source = contextsource.get()
    ret = dataframe_source.load(dataset_name, prefix, sqlContext)
    if cast_binary_to_str:
        ret = cast_binary_cols_to_string(ret)
    return ret


def write(df,
          dataset_name,
          prefix=None,
          *,
          schema=None):
    """Write a dataframe to storage. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file or dataiku (HDFS).

    Args:
        df (DataFrame): spark data frame to write.
        dataset_name (str): The data set to load.
        prefix (str): Prefix path or dataiku project_key for loading
        the data set.

    Kwargs:
        schema (Schema): Birgitta schema to apply on write.

    Returns:
       None.
    """
    if schema:
        df = schema.enforce(df)
    dataframe_source = contextsource.get()
    return dataframe_source.write(df, dataset_name, prefix)


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
