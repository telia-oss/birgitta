import sys

import mock
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


def test_has_dataiku_without_spark():
    """Ensure that just having dataiku module is not enough.
    It should also have spark module"""
    reset_context()
    sys.modules['dataiku'] = mock.MagicMock()
    source = contextsource.get()
    assert not source


def test_has_dataiku_without():
    reset_context()
    sys.modules['dataiku'] = mock.MagicMock()
    sys.modules['dataiku'].spark = 'NonMock'
    sys.modules['dataiku.spark'] = 'NonMock'
    source = contextsource.get()
    assert type(source).__name__ == 'DataikuSource'
