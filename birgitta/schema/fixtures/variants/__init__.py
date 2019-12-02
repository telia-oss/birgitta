"""Fixture Variants module.
This module enables a simple abstraction to deal with
both row conf and explicit row defintion of fixtures.
"""
from .row_confs_variant import RowConfsVariant
from .rows_variant import RowsVariant
from .variant_base import VariantBase

__all__ = ['VariantBase',
           'RowConfsVariant',
           'RowsVariant']
