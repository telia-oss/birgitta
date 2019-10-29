"""The dataframesource module. Provides classes for reading and writing
dataframes from different sources.
"""

from . import contextsource
from .dataframesourcebase import DataframeSourceBase


__all__ = ['DataframeSourceBase', 'sources', 'contextsource']  # noqa F822
