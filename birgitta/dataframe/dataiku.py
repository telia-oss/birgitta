"""Functions for loading and writing dataiku spark dataframes."""

import dataiku
import dataiku.spark as dkuspark

__all__ = ['get', 'write']


def get(dataset_name, project_key, sqlContext):
    """When project_key is None use same project as recipe is in."""
    dataset = dataiku.Dataset(dataset_name, project_key)
    return dkuspark.get_dataframe(sqlContext, dataset)


def write(df, dataset_name, project_key):
    """When project_key is None use same project as recipe is in."""
    dataset = dataiku.Dataset(dataset_name, project_key)
    dkuspark.write_with_schema(dataset, df)
