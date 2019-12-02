"""The Dataset class. Defines data set name and schema.
"""

__all__ = ['Dataset']


class Dataset:
    def __init__(self, name, schema):
        """Constructs a dataset object which should be treated
        as immutable.

        The name is important to give flexibility with regards to
        the underlying storage naming.

        Args:
            name (str): The actual table name.
            schema (Schema): The schema for the dataset.
        """
        self.name = name
        self.schema = schema
