"""RowsVariant class for fixtures with each rows' values' specified.

Unlike the RowConfsVariant which takes default values unless otherwise
specified, this class takes the rows to the constructor as a complete
dataset.
"""

from .variant_base import VariantBase
from ... import fixtures

__all__ = ['RowsVariant']


class RowsVariant(VariantBase):
    def __init__(self, collection):
        self.rows = collection

    def df(self, spark, dataset):
        """Method for creating the fixture data frame.

        Args:
            spark (SparkContext): Spark context to generate data frame

        Returns:
            A data frame of the fixture."""
        return fixtures.df_w_rows(spark, dataset.schema, self.rows)
