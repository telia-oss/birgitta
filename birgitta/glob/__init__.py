"""Handle global variables used to:

* Set storage type
* Collect spark recipe test coverage data
* Set data set overrides
* Specify Today value, to get consistent tests
* Set other global variables
"""
import datetime


class Glob():
    TODAY = datetime.date.today()


def reset():
    # BIRGITTA_DATASET_STORAGE = "DATAIKU"  # Store fixtures in Dataiku
    # BIRGITTA_DATASET_STORAGE = "MEM"  # Store fixtures in dfs in memory
    Glob.BIRGITTA_DATASET_STORAGE = "FILE"  # Store fixtures in parquet files
    Glob.BIRGITTA_FILEBASED_DATASETS = None
    Glob.BIRGITTA_MEMBASED_DATASETS = None
    Glob.BIRGITTA_TEST_COVERAGE = {}
    Glob.BIRGITTA_DATASET_OVERRIDES = None


reset()  # Init glob


def get(key, default=None):
    attr = getattr(Glob, key)
    return attr if attr else default


def set(key, val):
    setattr(Glob, key, val)
