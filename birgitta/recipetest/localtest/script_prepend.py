"""Prepend to all pyspark recipes run by localtest
"""
PREPEND = """from birgitta import timing

import sys

import mock
from birgitta import glob
from birgitta.recipe.debug import dataframe as dfdbg
from birgitta import notebook
from birgitta.recipetest.coverage.report import log_transform
import pyspark

glob.set("BIRGITTA_DATASET_STORAGE", globals()['BIRGITTA_DATASET_STORAGE'])
glob.set("BIRGITTA_FILEBASED_DATASETS", globals()['BIRGITTA_FILEBASED_DATASETS'])
glob.set("BIRGITTA_MEMBASED_DATASETS", globals()['BIRGITTA_MEMBASED_DATASETS'])
glob.set("BIRGITTA_TEST_COVERAGE", globals()['BIRGITTA_TEST_COVERAGE'])
glob.set("BIRGITTA_DBG_COUNTS", globals()['BIRGITTA_DBG_COUNTS'])
sys.modules['dataiku'] = mock.MagicMock()
sys.modules['dataiku.spark'] = mock.MagicMock()

timing.time("script_prepend after magicmock")
"""

__all__ = ['code']


def code():
    return PREPEND
