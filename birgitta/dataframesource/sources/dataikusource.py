"""The Dataiku dataframesource class.
"""
import dataiku
import dataiku.spark as dkuspark
from birgitta.dataframesource import DataframeSourceBase

__all__ = ['DataikuSource']


class DataikuSource(DataframeSourceBase):
    def load(self, dataset_name, prefix, sqlContext):
        """When prefix is None use same project as recipe is in."""
        project_key = prefix  # Assume that prefix is a valid project_key
        dataset = dataiku.Dataset(dataset_name, project_key)
        return dkuspark.get_dataframe(sqlContext, dataset)

    def write(self, df, dataset_name, prefix):
        """When project_key is None use same project as recipe is in."""
        project_key = prefix  # Assume that prefix is a valid project_key
        dataset = dataiku.Dataset(dataset_name, project_key)
        dkuspark.write_with_schema(dataset, df)
