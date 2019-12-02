from birgitta.dataiku.libtest import libtest

from . import dummytest


def test_libtest():
    libtest.test(dummytest)
