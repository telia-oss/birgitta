"""VariantBase, the base class for fixture variants.
This base class enables us to create fixtures in different ways.
"""

from abc import ABCMeta, abstractmethod

__all__ = ['VariantBase']


class VariantBase(metaclass=ABCMeta):
    def __init__(self, collection):
        """Abstract class for fixture variants.

        Args:
            collection (list of lists): The rows or row_confs,
            depending on class.
        """
        pass

    @abstractmethod
    def df(self, spark):
        """Method for creating the fixture data frame.

        Args:
            spark (SparkContext): Spark context to generate data frame

        Returns:
            A data frame of the fixture."""
        return NotImplemented
