"""The Dataiku dataframesource class.
"""
import sys

from birgitta.dataframesource import DataframeSourceBase
from pyspark.sql.utils import AnalysisException


__all__ = ['LocalSource']


class LocalSource(DataframeSourceBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if 'dataset_dir' in kwargs:
            self.dataset_dir = kwargs['dataset_dir']
        else:
            self.dataset_dir = None

    def load(self, spark_session, dataset_name, prefix, **kwargs):
        dataset_dir = prefix  # Interpret prefix as dataset_dir
        if not dataset_dir:
            dataset_dir = self.dataset_dir
        try:
            read = spark_session.read
            schema = kwargs.get('schema')
            if schema:
                read = read.schema(schema.to_spark())
            return read.parquet(
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

    def write(self, df, dataset_name, dataset_dir=None, **kwargs):
        if not dataset_dir:
            dataset_dir = self.dataset_dir
        dest_path = "%s/%s/" % (dataset_dir, dataset_name)
        # Use coalesce to write a single file, and thus preserve row order
        df.coalesce(1).write.format("parquet").mode("append").save(dest_path)
