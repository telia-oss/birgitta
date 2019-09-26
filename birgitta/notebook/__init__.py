"""Functions like `pause()`, `show()` and `die()` to enable debugging libs
from within a notebooks, thus also enabling lib sharing in
notebook work flows, helping to build production quality code.
"""
import pyspark


def die():
    """Manual breakpoint in notebook"""
    assert False


def pause():
    raw_input("Press the <ENTER> key to continue...")  # noqa: F821


def show(df, rows=3, truncate=False):
    if in_notebook():
        df.show(rows, truncate)


def mem_persist(df):
    if in_notebook():
        df.persist(pyspark.StorageLevel.MEMORY_ONLY)


def disk_persist(df):
    if in_notebook():
        df.persist(pyspark.StorageLevel.DISK_ONLY)


def in_notebook():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False
