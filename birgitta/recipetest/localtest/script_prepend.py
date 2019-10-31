"""Prepend to all pyspark recipes run by localtest
"""
PREPEND = """from birgitta import timing

import sys

import mock
from birgitta import context
from birgitta.dataframesource import contextsource
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipe.debug import dataframe as dfdbg
from birgitta import notebook
from birgitta.recipetest.coverage.report import log_transform
import pyspark

contextsource.set(LocalSource(dataset_dir='__DATASET_DIR__'))
context.set("BIRGITTA_TEST_COVERAGE", globals()['BIRGITTA_TEST_COVERAGE'])
context.set("BIRGITTA_DBG_COUNTS", globals()['BIRGITTA_DBG_COUNTS'])
context.set("BIRGITTA_SPARK_SESSION_TYPE",
    globals()['BIRGITTA_SPARK_SESSION_TYPE'])
sys.modules['dataiku'] = mock.MagicMock()
sys.modules['dataiku.spark'] = mock.MagicMock()

timing.time("script_prepend after magicmock")
"""

__all__ = ['code']


def code(dataset_dir):
    return PREPEND.replace('__DATASET_DIR__', dataset_dir)
