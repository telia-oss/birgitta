"""
"""
from birgitta import timing
from birgitta.dataframe import filebased, membased, storage

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


def write_fixtures(globals_dict, fixture_fns, tmpdir, spark_session):
    """Write fixtures to storage.

    Args:
        globals_dict (dict): The set of variables which will be
        available to the pyspark recipe as globals. It is needed
        in order to store the memory based fixtures in globals.
        fixture_fns
    """
    timing.time("write_fixtures start")
    if storage.stored_in("MEM"):
        write_membased_fixtures(globals_dict, fixture_fns)
    else:
        globals_dict["BIRGITTA_MEMBASED_DATASETS"] = False
        write_filebased_fixtures(fixture_fns, tmpdir, spark_session)
    timing.time("write_fixtures end")


def write_membased_fixtures(globals_dict, fixture_fns):
    globals_dict["BIRGITTA_MEMBASED_DATASETS"] = membased.conf_sets(
        fixture_fns)


def write_filebased_fixtures(fixture_fns, tmpdir, spark_session):
    # FUTURE: Try writing directly to parquet using pyarrow instead. Could be faster. # noqa 501
    for dataframe_key in fixture_fns.keys():
        df = fixture_fns[dataframe_key](spark_session)
        filebased.write(df, dataframe_key, tmpdir)
