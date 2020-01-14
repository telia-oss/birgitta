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
    if 'dataikuapi' in sys.modules:
        del sys.modules['dataikuapi']


def test_no_dataiku():
    reset_context()
    source = contextsource.get()
    assert not source


def is_current_platform():
    return True


@mock.patch("birgitta.dataiku.platform.is_current_platform",
            is_current_platform)
def test_has_dataiku():
    reset_context()
    sys.modules['dataikuapi'] = mock.Mock()
    sys.modules['dataiku'] = mock.Mock()
    # To enable dataiku source loading, both these spark lines are needed
    sys.modules['dataiku'].spark = 'NonMock'
    sys.modules['dataiku.spark'] = 'NonMock'
    source = contextsource.get()
    assert type(source).__name__ == 'DataikuSource'
    # Reset dataiku mocks to none, to clean up for other tests
    reset_context()
