"""Handle global variables used to:

* Set dataframe source
* Collect spark recipe test coverage data
* Specify Today value, to get consistent tests
* Set other global variables
"""
import datetime


class Context():
    TODAY = datetime.date.today()


def reset():
    Context.BIRGITTA_DATAFRAMESOURCE = None
    Context.BIRGITTA_TEST_COVERAGE = {}
    Context.BIRGITTA_SPARK_SESSION_TYPE = "LOCAL"


reset()  # Init context


def get(key, default=None):
    attr = getattr(Context, key)
    return attr if attr else default


def set(key, val):
    setattr(Context, key, val)
