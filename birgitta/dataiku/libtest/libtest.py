"""library for testing custom python libraries on dataiku

## Examples

How to run all test_*.py files within pytest in a dataiku project or
system library called foo:

from birgitta.dataiku.libtest import libtest

import foo

libtest.test(foo)
"""
import inspect
import os

import pytest


def test(lib):
    test_path = os.path.dirname(inspect.getfile(lib))
    ret = pytest.main(["-x", test_path + "/tests", "-vv"])
    success = ret == 0
    if not success:
        print(f"Pytest failed on library {test_path}")
    return success
