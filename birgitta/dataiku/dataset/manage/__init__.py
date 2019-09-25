"""Utility functions for manipulating dataiku datasets.
Used by dataiku recipetest."""
import dataiku
from pandas import DataFrame


def delete_if_exists(project, name):
    for dataset_meta in project.list_datasets():
        if name == dataset_meta['name']:
            dataset_to_delete = project.get_dataset(name)
            dataset_to_delete.delete()
            break


def empty_and_fill(project_key, dataset_name, schema, rows):
    dst_ds = dataiku.Dataset(dataset_name, project_key)
    fill_df = DataFrame(rows)
    dst_ds.write_with_schema(fill_df, True)  # True => dropAndCreate
