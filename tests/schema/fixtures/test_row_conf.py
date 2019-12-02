import pytest  # noqa 401
from birgitta.schema.fixtures import RowConf


def dummyfn():
    pass


def test_function_val():
    row_conf = RowConf()
    row_conf.set_field('dummyval', 'dummy')
    with pytest.raises(ValueError):
        row_conf.set_field('dummyfn', dummyfn)
