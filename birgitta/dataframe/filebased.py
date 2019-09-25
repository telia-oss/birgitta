"""Functions for loading and writing filebased spark dataframes."""

import sys

from birgitta import glob
from birgitta import spark
from pyspark.sql.utils import AnalysisException

__all__ = ['get', 'write']


def get(dataset_name, dataset_dir=None, spark_session=None):
    if not dataset_dir:
        dataset_dir = conf_dataset_dir()
    sqlContext = spark.sql_ctx(spark_session)
    try:
        return sqlContext.read.parquet(
            "%s/%s/" % (dataset_dir, dataset_name))
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            print("e.desc:", repr(e.desc))
            err_msg = (
                "Missing dataset: %s, " +
                "Is the dataset name correct and equal to the dataset table_name for the fixture? " +  # noqa 501
                "Error: %s") % (dataset_name, e.desc)
            print("err_msg:", repr(err_msg))
            raise type(e)(err_msg, sys.exc_info()[2])
        else:
            raise e


def write(df, dataset_name, dataset_dir=None):
    if not dataset_dir:
        dataset_dir = conf_dataset_dir()
    dest_path = "%s/%s/" % (dataset_dir, dataset_name)
    # Use coalesce to write a single file, and thus preserve row order
    df.coalesce(1).write.format("parquet").mode("append").save(dest_path)


def get_conf():
    return glob.get("BIRGITTA_FILEBASED_DATASETS")


def conf_dataset_dir():
    """Return the globally defined dataset directory."""
    return get_conf()['dataset_dir']
