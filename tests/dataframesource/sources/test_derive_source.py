import sys

import pytest  # noqa F401
from birgitta.dataframesource import contextsource


def reset_context():
    contextsource.set(None)
    if 'dataiku.spark' in sys.modules:
        del sys.modules['dataiku.spark']
    if 'dataiku' in sys.modules:
        del sys.modules['dataiku']


def test_no_dataiku():
    reset_context()
    source = contextsource.get()
    assert not source


class DataikuMock(object):
    dss_settings = 'dummy'


def test_has_dataiku_without():
    reset_context()
    mod_mock = DataikuMock()
    mod_mock.default_project_key = 'dummy'
    sys.modules['dataiku'] = mod_mock
    # To enable dataiku source loading, both these spark lines are needed
    sys.modules['dataiku'].spark = 'NonMock'
    sys.modules['dataiku.spark'] = 'NonMock'
    source = contextsource.get()
    assert type(source).__name__ == 'DataikuSource'
