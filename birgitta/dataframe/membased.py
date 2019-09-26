"""Functions for loading and writing membased spark dataframes.
Under some circumstances, membased might load faster than filebased."""

from birgitta import glob

__all__ = ['get']

DS_FIXTURES = "ds_fixture_fns"


def get(dataset_name, spark_session):
    ds_fixture_fns = conf_dataset()
    return ds_fixture_fns[dataset_name](spark_session)


def conf_sets(fixtures):
    """Defines the default structure of the membased
    globals directory.
    Used by localtest.fixturing.write_membased_fixtures().

    Args:
        fixtures (dict of DataFrame): Fixture DataFrame stored in mem

    Returns:
        Default dictionary structure for globals membased dict.
    """
    return {
        DS_FIXTURES: fixtures
    }


def get_conf():
    return glob.get("BIRGITTA_MEMBASED_DATASETS")


def conf_dataset():
    return get_conf()[DS_FIXTURES]
