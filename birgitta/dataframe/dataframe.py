"""Functions for loading and writing spark dataframes,
adapting to the globally defined storage platform
(file, memory, dataiku, s3 etc). This is useful to enable
the running and testing of recipes different underlying
storage platforms.

cast_binary_cols_to_string() is a utility function to deal with
binary to string conversion of a spark dataframe."""

import pandas as pd
from birgitta.dataframe import dataiku as bdataiku
from birgitta.dataframe import filebased
from birgitta.dataframe import membased
from birgitta.dataframe import storage
from birgitta.dataset import override
from pyspark.sql import functions

__all__ = ['get', 'write', 'cast_binary_cols_to_string']


def get(sqlContext,
        dataset_name,
        project_key=None,
        *,
        cast_binary_to_str=False):
    """Obtain a dataframe. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file, memory or dataiku (HDFS).

    Args:
        sqlContext (SqlContext): spark sql context used to load data frames.
        dataset_name (str): The data set to load.
        project_key (str): Used if data set in a separate dataiku project.
        (default is False).

    Kwargs:
        cast_binary_to_str (bool): Convert binary to str.

    Returns:
       Spark DataFrame.
    """
    dataset_name = override.override_if_set(dataset_name)
    if storage.stored_in("MEM"):
        ret = membased.get(dataset_name, sqlContext.sparkSession)
    elif storage.stored_in("FILE"):
        ret = filebased.get(dataset_name)
    else:
        ret = bdataiku.get(dataset_name, project_key, sqlContext)
    if cast_binary_to_str:
        ret = cast_binary_cols_to_string(ret)
    return ret


def write(df,
          dataset_name,
          project_key=None,
          *,
          schema=None):
    """Write a dataframe to storage. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file or dataiku (HDFS).

    Args:
        df (DataFrame): spark data frame to write.
        dataset_name (str): The data set to load.
        project_key (str): Used if data set in a separate dataiku project.
        (default is False).

    Kwargs:
        schema (Schema): Birgitta schema to apply on write.

    Returns:
       None.
    """
    if schema:
        df = schema.enforce(df)
    dataset_name = override.override_if_set(dataset_name)
    if storage.stored_in("MEM") or storage.stored_in("FILE"):
        return filebased.write(df, dataset_name)
    else:
        return bdataiku.write(df, dataset_name, project_key)


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
