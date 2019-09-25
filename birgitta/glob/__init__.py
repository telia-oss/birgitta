"""Handle global variables used to:

* Set storage type
* Collect spark recipe test coverage data
* Set data set overrides
* Specify Today value, to get consistent tests
* Set other global variables
"""
import datetime


class Glob():
    # BIRGITTA_DATASET_STORAGE = "DATAIKU"  # Store fixtures in Dataiku
    # BIRGITTA_DATASET_STORAGE = "MEM"  # Store fixtures in dfs in memory
    BIRGITTA_DATASET_STORAGE = "FILE"  # Store fixtures in parquet files
    BIRGITTA_FILEBASED_DATASETS = None
    BIRGITTA_MEMBASED_DATASETS = None
    BIRGITTA_TEST_COVERAGE = {}
    BIRGITTA_DATASET_OVERRIDES = None
    TODAY = datetime.date.today()


def get(key, default=None):
    attr = getattr(Glob, key)
    return attr if attr else default


def set(key, val):
    setattr(Glob, key, val)
