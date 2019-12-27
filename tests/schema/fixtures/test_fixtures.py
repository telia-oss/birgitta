import pytest  # noqa 401
from birgitta.fields import Catalog
from birgitta.schema import fixtures
from birgitta.schema.fixtures import RowConf
from birgitta.schema.schema import Schema


def test_get_val_0():
    """This is necessary to ensure that row_conf field val 0 is not
    treated as None when deriving the fixture val."""
    catalog = Catalog()
    catalog.add_field('dummyval', description='Dummy val', example=39)
    schema = Schema([['dummyval', 'bigint']], catalog)
    row_conf = RowConf()
    row_conf.set_field('dummyval', 0)
    dummy_val = fixtures.get_val(schema, row_conf, 'dummyval')
    # Must be 0, not 39
    assert dummy_val == 0


def test_set_none_row_conf():
    """This is necessary to ensure that row_conf field val 0 is not
    treated as None when deriving the fixture val."""
    catalog = Catalog()
    catalog.add_field('dummyval', description='Dummy val', example=39)
    schema = Schema([['dummyval', 'bigint']], catalog)
    row_conf = RowConf()
    row_conf.set_field('dummyval', None)
    dummy_val = fixtures.get_val(schema, row_conf, 'dummyval')
    # Must be None, not 39
    assert dummy_val is None
