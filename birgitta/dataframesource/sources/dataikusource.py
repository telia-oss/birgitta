"""The Dataiku dataframesource class.
"""
import dataiku
import dataiku.spark as dkuspark
import pyspark
from birgitta.dataframesource import DataframeSourceBase
from pyspark.sql import SQLContext


__all__ = ['DataikuSource']


class DataikuSource(DataframeSourceBase):
    def load(self, spark_session, dataset_name, prefix, **kwargs):
        """When prefix is None use same project as recipe is in."""
        schema = kwargs.get('schema')
        project_key = prefix  # Assume that prefix is a valid project_key
        dataset = dataiku.Dataset(dataset_name, project_key)
        sql_ctx = self.dataiku_sql_ctx()
        df = dkuspark.get_dataframe(sql_ctx, dataset)
        if schema:
            # FUTURE: implement native schema on read, if available on DSS
            df = schema.cast(df)
        return df

    def write(self, df, dataset_name, prefix, **kwargs):
        """When project_key is None use same project as recipe is in."""
        project_key = prefix  # Assume that prefix is a valid project_key
        dataset = dataiku.Dataset(dataset_name, project_key)
        dkuspark.write_with_schema(dataset, df)

    def dataiku_sql_ctx(self):
        sc = pyspark.SparkContext.getOrCreate()
        return SQLContext(sc)
