"""RowConfsVariant - class for fixtures with row_confs.
Row confs derive fixture values from catalogs and schemas.
"""

from .variant_base import VariantBase
from ..row_conf import RowConf
from ... import fixtures

__all__ = ['RowConfsVariant']


class RowConfsVariant(VariantBase):
    def __init__(self, collection):
        for entry in collection:
            if not (type(entry) == RowConf):
                raise ValueError(
                    "Entry (%s) must be RowConf" % repr(entry))
        self.row_confs = collection

    def df(self, spark, dataset):
        return fixtures.df(spark,
                           dataset.schema,
                           self.row_confs)
