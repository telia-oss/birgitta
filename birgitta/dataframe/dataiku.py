"""Functions for loading and writing dataiku spark dataframes."""
try:  # Enable modules importing this module to load even if it isn't used # noqa 501
    import dataiku
    import dataiku.spark as dkuspark
except ImportError:
    pass

__all__ = ['get', 'write']


def get(dataset_name, project_key, sqlContext):
    """When project_key is None use same project as recipe is in."""
    dataset = dataiku.Dataset(dataset_name, project_key)
    return dkuspark.get_dataframe(sqlContext, dataset)


def write(df, dataset_name, project_key):
    """When project_key is None use same project as recipe is in."""
    dataset = dataiku.Dataset(dataset_name, project_key)
    dkuspark.write_with_schema(dataset, df)
