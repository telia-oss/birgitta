"""The dataframesource abstract base class.
"""

from abc import ABCMeta, abstractmethod

__all__ = ['DataframeSourceBase']


class DataframeSourceBase(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        """Abstract data fram source class.

        Args:
            **kwargs (keyword args): optional keyword args for subclasses.
            e.g. dataset_dir for LocalSource.
        """
        self.keyword_args = kwargs

    @abstractmethod
    def load(self, spark_session, dataset_name, prefix, **kwargs):
        """Method for getting a data frame.

        Args:
            dataset_name (str): Name of the data set.
            prefix (str): Prefix path or dataiku project_key for loading
            the data set.
            sqlContext (SQLContext): SQLContext needed to load or
            instantiate the data frame.

        Returns:
            A data frame of the data set."""
        return NotImplemented

    @abstractmethod
    def write(self, df, dataset_name, prefix, **kwargs):
        """Write a data frame to a data source.

        Args:
            df (DataFrame): The DataFrame to write.
            dataset_name (str): Name of the data set.
            prefix (str): Prefix path or dataiku project_key for loading
            the data set.
."""
        return NotImplemented
