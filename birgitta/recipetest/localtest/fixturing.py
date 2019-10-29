"""Module for test case fixturing functions.
"""
from birgitta import timing


__all__ = ['obtain_fixture_fns', 'write_fixtures']


def obtain_fixture_fns(datasets, fixture_name):
    """Obtain fixture functions

    Returns:
        Dict of fixture name -> fixture fn.
    """
    fixture_fns = {}
    for ds in datasets:
        ds_module = ds[0]
        fixture_module = ds[1]
        fixture_fn = getattr(fixture_module, "fx_" + fixture_name)
        fixture_fns[ds_module.dataset.name] = fixture_fn
    return fixture_fns


def write_fixtures(globals_dict, fixture_fns, dataframe_source, spark_session):
    """Write fixtures to storage.

    Args:
        globals_dict (dict): The set of variables which will be
        available to the pyspark recipe as globals. It is needed
        in order to store the memory based fixtures in globals.
        fixture_fns
    """
    timing.time("write_fixtures start")
    # FUTURE: Try writing directly to parquet using pyarrow instead. Could be faster. # noqa 501
    for dataframe_key in fixture_fns.keys():
        df = fixture_fns[dataframe_key](spark_session)
        dataframe_source.write(df, dataframe_key)
    timing.time("write_fixtures end")
